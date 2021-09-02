/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_1

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._

import org.apache.spark.internal.{Logging, config}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ucx.reducer.compat.spark_3_1.UcxShuffleClient
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleReader, UcxShuffleHandle, UcxShuffleManager}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, SparkEnv, SparkException, TaskContext}


/**
 * Extension of Spark's shuffe reader with a logic of injection UcxShuffleClient,
 * and lazy progress only when result queue is empty.
 */
class UcxShuffleReader[K, C](handle: UcxShuffleHandle[K, _, C],
                             startMapIndex: Int,
                             endMapIndex: Int,
                             startPartition: Int,
                             endPartition: Int,
                             context: TaskContext,
                             serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                             blockManager: BlockManager = SparkEnv.get.blockManager,
                             readMetrics: ShuffleReadMetricsReporter,
                             shouldBatchFetch: Boolean = false) extends ShuffleReader[K, C] with Logging {

    private val dep = handle.baseHandle.dependency

    /** Read the combined key-values for this reduce task */
    override def read(): Iterator[Product2[K, C]] = {
      val (blocksByAddressIterator1, blocksByAddressIterator2) = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition).duplicate
      val mapIdToBlockIndex = blocksByAddressIterator2.flatMap{
        case (_, blocks) => blocks.map {
          case (blockId, _, mapIdx) => blockId match {
            case x: ShuffleBlockId =>  (x.mapId.asInstanceOf[java.lang.Long], mapIdx.asInstanceOf[java.lang.Integer])
            case x: ShuffleBlockBatchId => (x.mapId.asInstanceOf[java.lang.Long], mapIdx.asInstanceOf[java.lang.Integer])
            case _ => throw new SparkException("Unknown block")
          }
        }
      }.toMap

      val workerWrapper = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
        .ucxNode.getThreadLocalWorker
      val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()
      val shuffleClient = new UcxShuffleClient(handle.shuffleId, workerWrapper, mapIdToBlockIndex.asJava, shuffleMetrics)
      val shuffleIterator = new ShuffleBlockFetcherIterator(
        context,
        shuffleClient,
        blockManager,
        blocksByAddressIterator1,
        serializerManager.wrapStream,
        // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
        SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
        SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
        SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
        SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
        SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
        SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
        readMetrics,
        fetchContinuousBlocksInBatch)

      val wrappedStreams = shuffleIterator.toCompletionIterator

      // Ucx shuffle logic
      // Java reflection to get access to private results queue
      val queueField = shuffleIterator.getClass.getDeclaredField(
        "org$apache$spark$storage$ShuffleBlockFetcherIterator$$results")
      queueField.setAccessible(true)
      val resultQueue = queueField.get(shuffleIterator).asInstanceOf[LinkedBlockingQueue[_]]

      // Do progress if queue is empty before calling next on ShuffleIterator
      val ucxWrappedStream = new Iterator[(BlockId, InputStream)] {
        override def next(): (BlockId, InputStream) = {
          val startTime = System.currentTimeMillis()
          workerWrapper.fillQueueWithBlocks(resultQueue)
          readMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime)
          wrappedStreams.next()
        }

        override def hasNext: Boolean = {
          val result = wrappedStreams.hasNext
          if (!result) {
            shuffleClient.close()
          }
          result
        }
      }
      // End of ucx shuffle logic

      val serializerInstance = dep.serializer.newInstance()
      
      // Create a key/value iterator for each stream
      val recordIter = ucxWrappedStream.flatMap { case (blockId, wrappedStream) =>
        // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
        // NextIterator. The NextIterator makes sure that close() is called on the
        // underlying InputStream when all records have been read.
        serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
      }

      // Update the context task metrics for each record read.
      val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
        recordIter.map { record =>
          readMetrics.incRecordsRead(1)
          record
        },
        context.taskMetrics().mergeShuffleReadMetrics())

      // An interruptible iterator must be used here in order to support task cancellation
      val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

      val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // We are reading values that are already combined
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // We don't know the value type, but also don't care -- the dependency *should*
          // have made sure its compatible w/ this aggregator, which will convert the value
          // type to the combined type C
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
      }

      // Sort the output if there is a sort ordering defined.
      val resultIter = dep.keyOrdering match {
        case Some(keyOrd: Ordering[K]) =>
          // Create an ExternalSorter to sort the data.
          val sorter =
            new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
          sorter.insertAll(aggregatedIter)
          context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
          context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
          context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
          // Use completion callback to stop sorter if task was finished/cancelled.
          context.addTaskCompletionListener[Unit](_ => {
            sorter.stop()
          })
          CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
        case None =>
          aggregatedIter
      }

      resultIter match {
        case _: InterruptibleIterator[Product2[K, C]] => resultIter
        case _ =>
          // Use another interruptible iterator here to support task cancellation as aggregator
          // or(and) sorter may have consumed previous interruptible iterator.
          new InterruptibleIterator[Product2[K, C]](context, resultIter)
      }
    }

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol
    if (shouldBatchFetch && !doBatchFetch) {
      logWarning("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer ${dep.serializer.getClass.getName} " +
        s"relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol.")
    }
    doBatchFetch
  }

}

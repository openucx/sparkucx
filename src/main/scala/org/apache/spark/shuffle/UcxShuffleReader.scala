/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ucx.UcxShuffleClient
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

class UcxShuffleReader[K, C](handle: BaseShuffleHandle[K, _, C],
  startPartition: Int,
  endPartition: Int,
  context: TaskContext,
  serializerManager: SerializerManager = SparkEnv.get.serializerManager,
  blockManager: BlockManager = SparkEnv.get.blockManager,
  mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

    private val dep = handle.dependency

    /** Read the combined key-values for this reduce task */
    override def read(): Iterator[Product2[K, C]] = {
      val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()
      val workerWrapper = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
        .ucxNode.getThreadLocalWorker
      val shuffleClient = new UcxShuffleClient(handle, shuffleMetrics, workerWrapper)
      val wrappedStreams = new ShuffleBlockFetcherIterator(
        context,
        shuffleClient,
        blockManager,
        mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId,
          startPartition, endPartition),
        serializerManager.wrapStream,
        // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
        SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
        SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
        SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
        SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
        SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

      // Java reflection to get access to private results queue
      val queueField = wrappedStreams.getClass.getDeclaredField(
        "org$apache$spark$storage$ShuffleBlockFetcherIterator$$results")
      queueField.setAccessible(true)
      val itertorQueue = queueField.get(wrappedStreams).asInstanceOf[LinkedBlockingQueue[_]]

      val ucxWrappedStream = new Iterator[(BlockId, InputStream)] {
        override def next(): (BlockId, InputStream) = {
          val startTime = System.currentTimeMillis()
          workerWrapper.progressRequests(itertorQueue)
          shuffleMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime)
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

      val serializerInstance = dep.serializer.newInstance()
      val recordIter = ucxWrappedStream.flatMap { case (blockId, wrappedStream) =>
        // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
        // NextIterator. The NextIterator makes sure that close() is called on the
        // underlying InputStream when all records have been read.
        serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
      }

      // Update the context task metrics for each record read.
      val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
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
            new ExternalSorter[K, C, C](context,
              ordering = Some(keyOrd), serializer = dep.serializer)
          sorter.insertAll(aggregatedIter)
          context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
          context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
          context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
          // Use completion callback to stop sorter if task was finished/cancelled.
          context.addTaskCompletionListener(_ => {
            sorter.stop()
          })
          CompletionIterator[Product2[K, C],
            Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
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

}

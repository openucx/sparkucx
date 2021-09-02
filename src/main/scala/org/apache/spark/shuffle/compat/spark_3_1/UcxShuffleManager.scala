/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.compat.spark_3_1.{UcxShuffleBlockResolver, UcxShuffleReader}
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleWriter, UnsafeShuffleWriter}
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}

/**
 * Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
 * and injects needed logic in override methods.
 */
class UcxShuffleManager(override val conf: SparkConf, isDriver: Boolean) extends CommonUcxShuffleManager(conf, isDriver) {
  ShutdownHookManager.addShutdownHook(Int.MaxValue - 1)(stop)
  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver = new UcxShuffleBlockResolver(this)

  override def registerShuffle[K, V, C](shuffleId: ShuffleId, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    val numMaps = dependency.partitioner.numPartitions
    val baseHandle = super.registerShuffle(shuffleId, dependency).asInstanceOf[BaseShuffleHandle[K, V, C]]
    registerShuffleCommon(baseHandle, shuffleId, numMaps)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext,
                               metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    shuffleIdToHandle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[UcxShuffleHandle[K, V, _]])
    val env = SparkEnv.get
    handle.asInstanceOf[UcxShuffleHandle[K, V, _]].baseHandle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K@unchecked, V@unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case other: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        new SortShuffleWriter(
          shuffleBlockResolver, other, mapId, context, shuffleExecutorComponents)
    }
  }

  override def getReader[K, C](handle: ShuffleHandle, startMapIndex: Int, endMapIndex: Int,
                               startPartition: MapId, endPartition: MapId, context: TaskContext,
                               metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[UcxShuffleHandle[K, _, C]])
    new UcxShuffleReader(handle.asInstanceOf[UcxShuffleHandle[K,_,C]], startMapIndex, endMapIndex, startPartition, endPartition,
      context, readMetrics = metrics, shouldBatchFetch = true)
  }


  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
      .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

}

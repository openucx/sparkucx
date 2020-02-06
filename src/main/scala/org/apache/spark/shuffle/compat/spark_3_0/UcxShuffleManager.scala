/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle


import org.apache.spark.shuffle.compat.spark_3_0.UcxShuffleBlockResolver
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

/**
 * Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
 * and injects needed logic in override methods.
 */
class UcxShuffleManager(override val conf: SparkConf, isDriver: Boolean) extends CommonUcxShuffleManager(conf, isDriver) {
  ShutdownHookManager.addShutdownHook(Int.MaxValue - 1)(stop)
  override val shuffleBlockResolver = new UcxShuffleBlockResolver(this)

  override def registerShuffle[K, V, C](shuffleId: ShuffleId, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    val numMaps = dependency.partitioner.numPartitions
    val baseHandle = super.registerShuffle(shuffleId, dependency).asInstanceOf[BaseShuffleHandle[K, V, C]]
    registerShuffleCommon(baseHandle, shuffleId, numMaps)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext,
                               metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logInfo(s"MapId num ${TaskContext.getPartitionId()}")
    super.getWriter(handle.asInstanceOf[UcxShuffleHandle[K,V,_]].baseHandle, mapId, context, metrics)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: MapId, endPartition: MapId,
                               context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    super.getReader(handle.asInstanceOf[UcxShuffleHandle[K,_,C]].baseHandle, startPartition, endPartition,
      context, metrics)
  }

}

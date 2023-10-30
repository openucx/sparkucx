/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle

import org.apache.spark.shuffle.compat.spark_2_1.{
  UcxShuffleBlockResolver,
  UcxShuffleReader
}
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

/** Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
  * and injects needed logic in override methods.
  */
class UcxShuffleManager(override val conf: SparkConf, isDriver: Boolean)
    extends CommonUcxShuffleManager(conf, isDriver) {
  ShutdownHookManager.addShutdownHook(Int.MaxValue - 1)(stop)

  /** Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    * Called on driver and guaranteed by spark that shuffle on executor will start after it.
    */
  override def registerShuffle[K, V, C](
      shuffleId: ShuffleId,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]
  ): ShuffleHandle = {
    assume(isDriver)
    val baseHandle = super
      .registerShuffle(shuffleId, numMaps, dependency)
      .asInstanceOf[BaseShuffleHandle[K, V, C]]
    registerShuffleCommon(baseHandle, shuffleId, numMaps)
  }

  /** Mapper callback on executor. Just start UcxNode and use Spark mapper logic.
    */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext
  ): ShuffleWriter[K, V] = {
    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(
      handle.shuffleId,
      handle.asInstanceOf[UcxShuffleHandle[K, V, _]]
    )
    super.getWriter(
      handle.asInstanceOf[UcxShuffleHandle[K, V, _]].baseHandle,
      mapId,
      context
    )
  }

  override val shuffleBlockResolver: UcxShuffleBlockResolver =
    new UcxShuffleBlockResolver(this)

  /** Reducer callback on executor.
    */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext
  ): ShuffleReader[K, C] = {
    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(
      handle.shuffleId,
      handle.asInstanceOf[UcxShuffleHandle[K, _, C]]
    )
    new UcxShuffleReader(
      handle.asInstanceOf[UcxShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      context
    )
  }
}

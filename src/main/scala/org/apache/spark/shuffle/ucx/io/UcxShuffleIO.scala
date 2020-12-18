/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.io

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD
import org.apache.spark.shuffle.api.{ShuffleDriverComponents, ShuffleExecutorComponents}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.shuffle.ucx.UcxShuffleManager

class UcxShuffleIO(sparkConf: SparkConf) extends LocalDiskShuffleDataIO(sparkConf) with Logging {

  sparkConf.set(SHUFFLE_ACCURATE_BLOCK_THRESHOLD.key, "100000")

  override def driver(): ShuffleDriverComponents = {
    SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager].initTransport()
    super.driver()
  }

  override def executor(): ShuffleExecutorComponents = {
    new UcxShuffleExecutorComponents(sparkConf)
  }
}

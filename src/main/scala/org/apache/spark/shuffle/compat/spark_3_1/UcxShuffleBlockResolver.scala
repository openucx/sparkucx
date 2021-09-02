/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_1

import java.io.{File, RandomAccessFile}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.{CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}
import org.apache.spark.storage.ShuffleIndexBlockId

/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  override def getIndexFile(
                            shuffleId: Int,
                            mapId: Long,
                            dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val blockManager = SparkEnv.get.blockManager
    dirs
      .map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    // In Spark-3.0 MapId is long and unique among all jobs in spark. We need to use partitionId as offset
    // in metadata buffer
    val partitionId = TaskContext.getPartitionId()
    val dataFile = getDataFile(shuffleId, mapId)
    val dataBackFile = new RandomAccessFile(dataFile, "rw")

    if (dataBackFile.length() == 0) {
      dataBackFile.close()
      return
    }

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexBackFile = new RandomAccessFile(indexFile, "rw")

    writeIndexFileAndCommitCommon(shuffleId, partitionId, lengths, dataTmp, indexBackFile, dataBackFile)
  }
}

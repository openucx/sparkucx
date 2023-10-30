/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.compat.spark_2_1

import java.io.{File, RandomAccessFile}

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.{
  CommonUcxShuffleBlockResolver,
  CommonUcxShuffleManager,
  IndexShuffleBlockResolver
}
import org.apache.spark.storage.ShuffleIndexBlockId

/** Mapper entry point for UcxShuffle plugin. Performs memory registration
  * of data and index files and publish addresses to driver metadata buffer.
  */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
    extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    SparkEnv.get.blockManager.diskBlockManager.getFile(
      ShuffleIndexBlockId(
        shuffleId,
        mapId,
        IndexShuffleBlockResolver.NOOP_REDUCE_ID
      )
    )
  }

  /** Mapper commit protocol extension. Register index and data files and publish all needed
    * metadata to driver.
    */
  override def writeIndexFileAndCommit(
      shuffleId: ShuffleId,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File
  ): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = getDataFile(shuffleId, mapId)
    val dataBackFile = new RandomAccessFile(dataFile, "rw")

    if (dataBackFile.length() == 0) {
      dataBackFile.close()
      return
    }

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexBackFile = new RandomAccessFile(indexFile, "rw")
    writeIndexFileAndCommitCommon(
      shuffleId,
      mapId,
      lengths,
      dataTmp,
      indexBackFile,
      dataBackFile
    )
  }
}

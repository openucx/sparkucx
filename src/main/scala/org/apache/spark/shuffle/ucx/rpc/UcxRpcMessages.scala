/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.ucx.BlockId
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

object UcxRpcMessages {

  val PREFETCH_TAG = 1
  val FETCH_SINGLE_BLOCK_TAG = 2
  val FETCH_MULTIPLE_BLOCKS_TAG = 3

  /**
   * Called from executor to driver, to introduce ucx worker address.
   */
  case class ExecutorAdded(executorId: String,
                           endpoint: RpcEndpointRef,
                           ucxWorkerAddress: SerializableDirectBuffer)

  /**
   * Reply from driver with all executors in the cluster with their worker addresses.
   */
  case class IntroduceAllExecutors(executorIds: Seq[String],
                                   ucxWorkerAddresses: Seq[SerializableDirectBuffer])

  case class FetchBlockByBlockIdRequest(msgId: Int, blockId: BlockId)

  case class FetchBlocksByBlockIdsRequest(startTag: Int, blockIds: Seq[BlockId])

  case class PrefetchBlockIds(blockIds: Seq[BlockId])
}

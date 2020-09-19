/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransport)
  extends RpcEndpoint  with Logging {


  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(executorId: String, _: RpcEndpointRef,
    ucxWorkerAddress: SerializableDirectBuffer) => {
      logInfo(s"Received ExecutorAdded($executorId)")
      transport.addExecutor(executorId, ucxWorkerAddress.value)
    }
    case IntroduceAllExecutors(executorIds: Seq[String],
      ucxWorkerAddresses: Seq[SerializableDirectBuffer]) => {
      logInfo(s"Received IntroduceAllExecutors(${executorIds.mkString(",")})")
      executorIds.zip(ucxWorkerAddresses).foreach {
        case (executorId, workerAddress) => transport.addExecutor(executorId, workerAddress.value)
      }
    }
  }
}

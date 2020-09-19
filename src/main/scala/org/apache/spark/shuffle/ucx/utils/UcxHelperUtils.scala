package org.apache.spark.shuffle.ucx.utils

import java.net.{BindException, InetSocketAddress}

import scala.util.Random

import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp.{UcpListener, UcpListenerParams, UcpWorker}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object UcxHelperUtils extends Logging{
  def startListenerOnRandomPort(worker: UcpWorker, sparkConf: SparkConf): UcpListener = {
    val ucpListenerParams = new UcpListenerParams()
    val (listener, _) = Utils.startServiceOnPort(1024 + Random.nextInt(65535 - 1024), (port: Int) => {
      ucpListenerParams.setSockAddr(new InetSocketAddress(port))
      val listener = try {
        worker.newListener(ucpListenerParams)
      } catch {
        case ex:UcxException => throw new BindException(ex.getMessage)
      }
      (listener, listener.getAddress.getPort)
    }, sparkConf)
    logInfo(s"Started UcxListener on ${listener.getAddress}")
    listener
  }
}

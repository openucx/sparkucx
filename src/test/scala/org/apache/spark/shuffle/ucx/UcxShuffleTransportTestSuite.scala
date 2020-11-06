/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.openucx.jucx.UcxUtils
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.SparkConf

case class TestBlockId(id: Int) extends BlockId

class UcxShuffleTransportTestSuite extends AnyFunSuite {
  val conf = new UcxShuffleConf(new SparkConf())
  val blockSize = 4000000

  class ServerExecutor extends Thread {
    val ID: String = "1"
    private val transport = new UcxShuffleTransport(conf, ID)
    val workerAddress: ByteBuffer = transport.init()

    override def run(): Unit = {
      val block1 = new Block {
        override def getMemoryBlock: MemoryBlock = {
          val buf = ByteBuffer.allocateDirect(blockSize)
          buf.asCharBuffer().put("Hello world")
          buf.rewind()
          MemoryBlock(UcxUtils.getAddress(buf), blockSize)
        }
      }
      val block2 = new Block {
        override def getMemoryBlock: MemoryBlock = {
          val buf = ByteBuffer.allocateDirect(blockSize)
          buf.asCharBuffer().put("Test block2")
          buf.rewind()
          MemoryBlock(UcxUtils.getAddress(buf), blockSize)
        }
      }

      transport.register(TestBlockId(1), block1)
      transport.register(TestBlockId(2), block2)
      while (!isInterrupted) {
        transport.progress()
      }
      transport.close()
    }

    def mutateBlock(cb: OperationCallback): Unit = {
      transport.mutate(TestBlockId(2), new Block {
        override def getMemoryBlock: MemoryBlock = {
          val buf = ByteBuffer.allocateDirect(blockSize)
          buf.asCharBuffer().put("Test block3")
          buf.rewind()
          MemoryBlock(UcxUtils.getAddress(buf), blockSize)
        }
      }, cb)
    }
  }

  test("Shuffle transport flow") {
    val server = new ServerExecutor
    server.start()

    val transport = new UcxShuffleTransport(conf, "2")
    transport.init()

    transport.addExecutor(server.ID, server.workerAddress)

    val resultBuffer1 = ByteBuffer.allocateDirect(blockSize)
    val resultMemory1 = MemoryBlock(UcxUtils.getAddress(resultBuffer1), blockSize)

    val resultBuffer2 = ByteBuffer.allocateDirect(blockSize)
    val resultMemory2 = MemoryBlock(UcxUtils.getAddress(resultBuffer2), blockSize)

    val completed = new AtomicInteger(0)

    transport.fetchBlockByBlockId(server.ID, TestBlockId(1), resultMemory1,
      (result: OperationResult) => {
        completed.incrementAndGet()
        assert(result.getStats.get.recvSize == blockSize)
        assert(result.getStatus == OperationStatus.SUCCESS)
    })

    transport.fetchBlockByBlockId(server.ID, TestBlockId(2), resultMemory2,
      (result: OperationResult) => {
        completed.incrementAndGet()
        assert(result.getStats.get.recvSize == blockSize)
        assert(result.getStatus == OperationStatus.SUCCESS)
      })

    while (completed.get() != 2) {
      transport.progress()
    }

    assert(resultBuffer1.asCharBuffer().toString.trim == "Hello world")
    assert(resultBuffer2.asCharBuffer().toString.trim == "Test block2")

    // Test mutate:
    val mutated = new AtomicBoolean(false)
    server.mutateBlock((result: OperationResult) => {
      assert(result.getStatus == OperationStatus.SUCCESS)
      mutated.set(true)
    })

    while (!mutated.get()) {}

    val request = transport.fetchBlockByBlockId(server.ID, TestBlockId(2), resultMemory2, null)
    while (!request.isCompleted) {
      transport.progress()
    }

    assert(resultBuffer2.asCharBuffer().toString.trim == "Test block3")

    server.interrupt()
    transport.close()
  }

}

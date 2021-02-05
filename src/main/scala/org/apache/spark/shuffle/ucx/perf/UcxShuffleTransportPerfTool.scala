/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.perf

import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import ai.rapids.cudf.DeviceMemoryBuffer
import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp.UcpMemMapParams
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ucx._
import org.apache.spark.shuffle.ucx.memory.MemoryPool
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

object UcxShuffleTransportPerfTool {
  private val HELP_OPTION = "h"
  private val ADDRESS_OPTION = "a"
  private val NUM_BLOCKS_OPTION = "n"
  private val SIZE_OPTION = "s"
  private val PORT_OPTION = "p"
  private val ITER_OPTION = "i"
  private val MEMORY_TYPE_OPTION = "m"
  private val NUM_THREADS_OPTION = "t"
  private val REUSE_ADDRESS_OPTION = "r"

  private val ucxShuffleConf = new UcxShuffleConf(new SparkConf())
  private val transport = new UcxShuffleTransport(ucxShuffleConf, "e")
  private val workerAddress = transport.init()
  private var memoryPool: MemoryPool = transport.hostMemoryPool

  case class TestBlockId(id: Int) extends BlockId

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long,
                         serverPort: Int, numIterations: Int, numThreads: Int, memoryType: Int)

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption(HELP_OPTION, "help", false,
      "display help message")
    options.addOption(ADDRESS_OPTION, "address", true,
      "address of the remote host")
    options.addOption(NUM_BLOCKS_OPTION, "num-blocks", true,
      "number of blocks to transfer. Default: 1")
    options.addOption(SIZE_OPTION, "block-size", true,
      "size of block to transfer. Default: 4m")
    options.addOption(PORT_OPTION, "server-port", true,
      "server port. Default: 12345")
    options.addOption(ITER_OPTION, "num-iterations", true,
      "number of iterations. Default: 5")
    options.addOption(NUM_THREADS_OPTION, "num-threads", true,
      "number of threads. Default: 1")
    options.addOption(MEMORY_TYPE_OPTION, "memory-type", true,
      "memory type: host (default), cuda")
  }

  private def parseOptions(args: Array[String]): PerfOptions = {
    val parser = new GnuParser()
    val options = initOptions()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption(HELP_OPTION)) {
      new HelpFormatter().printHelp("UcxShufflePerfTool", options)
      System.exit(0)
    }

    val inetAddress = if (cmd.hasOption(ADDRESS_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(ADDRESS_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }

    val serverPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION, "12345"))

    val numIterations =  Integer.parseInt(cmd.getOptionValue(ITER_OPTION, "5"))

    val threadsNumber = Integer.parseInt(cmd.getOptionValue(NUM_THREADS_OPTION, "1"))

    var memoryType = UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
    if (cmd.hasOption(MEMORY_TYPE_OPTION) && cmd.getOptionValue(MEMORY_TYPE_OPTION) == "cuda") {
      memoryPool = transport.deviceMemoryPool
      memoryType = UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA
    }

    PerfOptions(inetAddress,
      Integer.parseInt(cmd.getOptionValue(NUM_BLOCKS_OPTION, "1")),
      Utils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "4m")),
      serverPort, numIterations, threadsNumber, memoryType)
  }

  private def startServer(perfOptions: PerfOptions): Unit = {
    val blocks: Seq[Block] = (0 until perfOptions.numBlocks * perfOptions.numThreads).map { _ =>
      if (ucxShuffleConf.pinMemory) {
        val block = memoryPool.get(perfOptions.blockSize)
        new Block {
          override def getMemoryBlock: MemoryBlock =
            block
        }
      } else if (perfOptions.memoryType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
        new Block {
          var oldBlock: DeviceMemoryBuffer = _
          override def getMemoryBlock: MemoryBlock = {
            if (oldBlock != null) {
              oldBlock.close()
            }
            val block = DeviceMemoryBuffer.allocate(perfOptions.blockSize)
            oldBlock = block
            MemoryBlock(block.getAddress, block.getLength, isHostMemory = false)
          }
        }
      } else {
        new Block {
          override def getMemoryBlock: MemoryBlock = {
            val buf = ByteBuffer.allocateDirect(perfOptions.blockSize.toInt)
            MemoryBlock(UcxUtils.getAddress(buf), perfOptions.blockSize)
          }
        }
      }

    }

    val blockIds = (0 until perfOptions.numBlocks * perfOptions.numThreads).map(i => TestBlockId(i))
    blockIds.zip(blocks).foreach {
      case (blockId, block) => transport.register(blockId, block)
    }

    val serverSocket = new ServerSocket(perfOptions.serverPort)

    println(s"Waiting for connections on " +
      s"${InetAddress.getLocalHost.getHostName}:${perfOptions.serverPort} ")

    val clientSocket = serverSocket.accept()
    val out = clientSocket.getOutputStream
    val in = clientSocket.getInputStream

    val buf = ByteBuffer.allocate(workerAddress.capacity())
    buf.put(workerAddress)
    buf.flip()

    out.write(buf.array())
    out.flush()

    println(s"Sending worker address to ${clientSocket.getInetAddress}")

    buf.flip()

    in.read(buf.array())
    clientSocket.close()
    serverSocket.close()

    blocks.foreach(block => memoryPool.put(block.getMemoryBlock))
    blockIds.foreach(transport.unregister)
    transport.close()
  }

  private def startClient(perfOptions: PerfOptions): Unit = {
    val socket = new Socket(perfOptions.remoteAddress.getHostName,
      perfOptions.remoteAddress.getPort)

    val buf = new Array[Byte](4096)
    val readSize = socket.getInputStream.read(buf)
    val executorId = "1"
    val workerAddress = ByteBuffer.allocateDirect(readSize)

    workerAddress.put(buf, 0, readSize)
    println("Received worker address")

    transport.addExecutor(executorId, workerAddress)

    val resultSize = perfOptions.numThreads * perfOptions.numBlocks * perfOptions.blockSize
    val resultMemory = memoryPool.get(resultSize)
    transport.register(TestBlockId(-1), new Block {
      override def getMemoryBlock: MemoryBlock = resultMemory
    })

    val threadPool =  Executors.newFixedThreadPool(perfOptions.numThreads)

    for (i <- 1 to perfOptions.numIterations) {
      val startTime = System.nanoTime()
      val countDownLatch = new CountDownLatch(perfOptions.numThreads)
      val deviceMemoryBuffers: Array[DeviceMemoryBuffer] = new Array(perfOptions.numThreads * perfOptions.numBlocks)
      for (tid <- 0 until perfOptions.numThreads) {
        threadPool.execute(() => {
          val blocksOffset = tid * perfOptions.numBlocks
          val blockIds = (blocksOffset until blocksOffset + perfOptions.numBlocks).map(i => TestBlockId(i))

          val mem = new Array[MemoryBlock](perfOptions.numBlocks)

          for (j <- 0 until perfOptions.numBlocks) {
            mem(j) = MemoryBlock(resultMemory.address +
              (tid * perfOptions.numBlocks * perfOptions.blockSize) + j * perfOptions.blockSize, perfOptions.blockSize)

            if (!ucxShuffleConf.pinMemory && perfOptions.memoryType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {
              val buffer = Platform.allocateDirectBuffer(perfOptions.blockSize.toInt)
              mem(j) = MemoryBlock(UcxUtils.getAddress(buffer), perfOptions.blockSize)
            } else if (!ucxShuffleConf.pinMemory &&
              perfOptions.memoryType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
              val buffer = DeviceMemoryBuffer.allocate(perfOptions.blockSize)
              deviceMemoryBuffers(tid * perfOptions.numBlocks + j) = buffer
              mem(j) = MemoryBlock(buffer.getAddress, buffer.getLength, isHostMemory = false)
            } else {
              mem(j) = MemoryBlock(resultMemory.address +
                (tid * perfOptions.numBlocks * perfOptions.blockSize) + j * perfOptions.blockSize, perfOptions.blockSize)
            }
          }

          val requests = transport.fetchBlocksByBlockIds(executorId, blockIds, mem, Seq.empty[OperationCallback])

          while (!requests.forall(_.isCompleted)) {
            transport.progress()
          }

          countDownLatch.countDown()
        })
      }

      countDownLatch.await()
      val elapsedTime = System.nanoTime() - startTime

      deviceMemoryBuffers.foreach(d => if (d != null) d.close())
      val totalTime  = if (elapsedTime < TimeUnit.MILLISECONDS.toNanos(1)) {
        s"$elapsedTime ns"
      } else {
        s"${TimeUnit.NANOSECONDS.toMillis(elapsedTime)} ms"
      }
      val throughput: Double = (resultSize / 1024.0D / 1024.0D / 1024.0D) /
        (elapsedTime / 1e9D)

      if ((i % 100 == 0) || i == perfOptions.numIterations) {
        println(f"${s"[$i/${perfOptions.numIterations}]"}%12s" +
          s" numBlocks: ${perfOptions.numBlocks}" +
          s" numThreads: ${perfOptions.numThreads}" +
          s" size: ${Utils.bytesToString(perfOptions.blockSize)}," +
          s" total size: ${Utils.bytesToString(resultSize * perfOptions.numThreads)}," +
          f" time: $totalTime%3s" +
          f" throughput: $throughput%.5f GB/s")
      }
    }

    val out = socket.getOutputStream
    out.write(buf)
    out.flush()
    out.close()
    socket.close()

    transport.unregister(TestBlockId(-1))
    memoryPool.put(resultMemory)
    transport.close()
    threadPool.shutdown()
  }

  def main(args: Array[String]): Unit = {
    val perfOptions = parseOptions(args)

    if (perfOptions.remoteAddress == null) {
      startServer(perfOptions)
    } else {
      startClient(perfOptions)
    }
  }

}

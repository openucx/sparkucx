/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.perf

import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ucx._
import org.apache.spark.shuffle.ucx.memory.MemoryPool
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

  private val ucxShuffleConf = new UcxShuffleConf(new SparkConf())
  private val transport = new UcxShuffleTransport(ucxShuffleConf, "e")
  private val workerAddress = transport.init()
  private var memoryPool: MemoryPool = transport.memoryPool

  case class TestBlockId(id: Int) extends BlockId

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long,
                         serverPort: Int, numIterations: Int, numThreads: Int)

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

    if (cmd.hasOption(MEMORY_TYPE_OPTION) && cmd.getOptionValue(MEMORY_TYPE_OPTION) == "cuda") {
      val className = "org.apache.spark.shuffle.ucx.GpuMemoryPool"
      val cls = Utils.classForName(className)
      memoryPool = cls.getConstructor().newInstance().asInstanceOf[MemoryPool]
    }

    PerfOptions(inetAddress,
      Integer.parseInt(cmd.getOptionValue(NUM_BLOCKS_OPTION, "1")),
      Utils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "4m")),
      serverPort, numIterations, threadsNumber)
  }

  private def startServer(perfOptions: PerfOptions): Unit = {
    val blocks: Seq[Block] = (0 until perfOptions.numBlocks).map { _ =>
      val block = memoryPool.get(perfOptions.blockSize)
      new Block {
        override def getMemoryBlock: MemoryBlock =
          block
      }
    }

    val blockIds = (0 until perfOptions.numBlocks).map(i => TestBlockId(i))
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

    val resultSize = perfOptions.numBlocks * perfOptions.blockSize
    val resultMemory = memoryPool.get(resultSize)

    val blockIds = (0 until perfOptions.numBlocks).map(i => TestBlockId(i))

    val threadPool =  Executors.newFixedThreadPool(perfOptions.numThreads)

    for (i <- 1 to perfOptions.numIterations) {
      val elapsedTime = new AtomicLong(0)
      val countDownLatch = new CountDownLatch(perfOptions.numThreads)

      for (_ <- 0 until perfOptions.numThreads) {
        threadPool.execute(() => {
          val completed = new AtomicInteger(0)

          val mem = new Array[MemoryBlock](perfOptions.numBlocks)
          val callbacks = new Array[OperationCallback](perfOptions.numBlocks)

          for (j <- 0 until perfOptions.numBlocks) {
            mem(j) = MemoryBlock(resultMemory.address + j * perfOptions.blockSize, perfOptions.blockSize)
            callbacks(j) = (result: OperationResult) => {
              elapsedTime.addAndGet(result.getStats.get.getElapsedTimeNs)
              completed.incrementAndGet()
            }
          }

          transport.fetchBlocksByBlockIds(executorId, blockIds, mem, callbacks)

          while (completed.get() != perfOptions.numBlocks) {
            transport.progress()
          }
          countDownLatch.countDown()
        })
      }

      countDownLatch.await()

      val totalTime  = if (elapsedTime.get() < TimeUnit.MILLISECONDS.toNanos(1)) {
        s"$elapsedTime ns"
      } else {
        s"${TimeUnit.NANOSECONDS.toMillis(elapsedTime.get())} ms"
      }
      val throughput: Double = (resultSize * perfOptions.numThreads / 1024.0D / 1024.0D / 1024.0D) /
        (elapsedTime.get() / 1e9D)

      println(f"${s"[$i/${perfOptions.numIterations}]"}%12s" +
        s" numBlocks: ${perfOptions.numBlocks}" +
        s" numThreads: ${perfOptions.numThreads}" +
        s" size: ${Utils.bytesToString(perfOptions.blockSize)}," +
        s" total size: ${Utils.bytesToString(resultSize * perfOptions.numThreads)}," +
        f" time: $totalTime%3s" +
        f" throughput: $throughput%.5f GB/s")
    }

    val out = socket.getOutputStream
    out.write(buf)
    out.flush()
    out.close()
    socket.close()

    memoryPool.put(resultMemory)
    transport.close()
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

/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single instance class per spark process, that keeps UcpContext, memory and worker pools.
 */
public class UcxNode implements Closeable {
  private final UcpContext context;
  private final MemoryPool memoryPool;
  private final UcpWorkerParams workerParams = new UcpWorkerParams().requestThreadSafety();
  private UcpListener listener;
  private final UcpWorker globalWorker;
  private UcpEndpoint globalDriverEndpoint;
  private final RegisteredMemory metadataMemory;

  private boolean closed = false;

  private final Set<UcxWorkerWrapper> workerPool = ConcurrentHashMap.newKeySet();

  private final ThreadLocal<UcxWorkerWrapper> worker;
  private final Thread listenerProgressThread;

  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);
  private static final AtomicInteger numWorkers = new AtomicInteger(0);

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    UcpParams params = new UcpParams().requestTagFeature()
      .requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true)
      .setEstimatedNumEps(conf.coresPerProcess() * (long)conf.getNumProcesses());
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);
    globalWorker = context.newWorker(workerParams);
    InetSocketAddress driverAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());
    metadataMemory = memoryPool.get(conf.metadataRPCBufferSize());
    ByteBuffer metadataBuffer = metadataMemory.getBuffer();
    metadataBuffer.order(ByteOrder.nativeOrder());

    if (isDriver) {
      // Start listener on a driver and accept RPC messages from executors with their
      // worker addresses
      UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(driverAddress);
      listener = globalWorker.newListener(listenerParams);
      logger.info("Started UcxNode on {}", driverAddress);
    } else {
      globalDriverEndpoint = globalWorker.newEndpoint(
        new UcpEndpointParams().setSocketAddress(driverAddress).setPeerErrorHadnlingMode()
      );
      BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
      ByteBuffer workerAddresss = globalWorker.getAddress();
      metadataBuffer.putInt(workerAddresss.capacity());
      metadataBuffer.put(workerAddresss);
      try {
        logger.info("MB position: {}", metadataBuffer.position());
        blockManagerId.writeExternal(
          new ObjectOutputStream(new ByteBufferOutputStream(metadataBuffer)));
        logger.info("MB position: {}", metadataBuffer.position());
      } catch (IOException e) {
        e.printStackTrace();
      }

      metadataBuffer.clear();
      globalDriverEndpoint.sendTaggedNonBlocking(metadataBuffer, new UcxCallback() {
        @Override
        public void onSuccess(UcxRequest request) {
          workerAddresss.clear();
          logger.info("Sent data to driver: {} : #{}, #:{}", workerAddresss.capacity(),
            workerAddresss.hashCode(), metadataBuffer.hashCode());
          metadataBuffer.clear();
        }
      });
    }


    // Global listener thread, that keeps lazy progress for connection establishment
    listenerProgressThread = new Thread() {
      @Override
      public void run() {
        while (!isInterrupted()) {
          globalWorker.recvTaggedNonBlocking(metadataBuffer, new UcxCallback() {
            @Override
            public void onSuccess(UcxRequest request) {
              metadataBuffer.clear();
              logger.info("Metadata buffer #:{}", metadataBuffer.hashCode());
              int workerAddressSize = metadataBuffer.getInt();
              logger.info("WorkerAddress size {}", workerAddressSize);
              ByteBuffer workerAddress = Platform.allocateDirectBuffer(workerAddressSize);
              for (int i = 0; i < workerAddressSize; i++) {
                workerAddress.put(metadataBuffer.get());
              }
              BlockManagerId blockManagerId = null;
              try {
                blockManagerId = BlockManagerId.apply(
                  new ObjectInputStream(new ByteBufferInputStream(metadataBuffer)));
              } catch (IOException e) {
                e.printStackTrace();
              }
              logger.info("Received message. BlockManagerId :{}, workerAddress: {}",
                blockManagerId, workerAddress.hashCode());
            }
          });
          if (globalWorker.progress() == 0) {
            globalWorker.waitForEvents();
          }
        }
      }
    };

    listenerProgressThread.setName("Listener progress thread.");
    listenerProgressThread.setDaemon(true);
    listenerProgressThread.start();

    if (!isDriver) {
      memoryPool.preAlocate();
    }
    worker = ThreadLocal.withInitial(() -> {
      UcpWorker localWorker = context.newWorker(workerParams);
      UcxWorkerWrapper result = new UcxWorkerWrapper(localWorker,
        conf, numWorkers.incrementAndGet());
      if (result.id() > conf.coresPerProcess()) {
        logger.warn("Thread: {} - creates new worker {} > numCores",
          Thread.currentThread().getId(), result.id());
      }
      return result;
    });
  }

  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public UcpContext getContext() {
    return context;
  }

  public UcxWorkerWrapper getWorker() {
    return worker.get();
  }

  public void putWorker(UcxWorkerWrapper workerWrapper) {
    workerPool.add(workerWrapper);
  }

  @Override
  public void close() {
    worker.remove();
    synchronized (this) {
      if (!closed) {
        logger.info("Stopping UcxNode");
        memoryPool.put(metadataMemory);
        listenerProgressThread.interrupt();
        if (globalDriverEndpoint != null) {
          globalDriverEndpoint.close();
        }
        globalWorker.signal();
        memoryPool.close();
        if (listener != null) {
          listener.close();
        }

        globalWorker.close();
        workerPool.forEach(UcxWorkerWrapper::close);
        workerPool.clear();
        context.close();
        closed = true;
      }
    }
  }
}

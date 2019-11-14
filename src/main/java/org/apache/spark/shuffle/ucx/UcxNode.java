/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.shuffle.ucx.rpc.SerializableBlockManagerID;
import org.apache.spark.shuffle.ucx.rpc.UcxListenerThread;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single instance class per spark process, that keeps UcpContext, memory and worker pools.
 */
public class UcxNode implements Closeable {
  // Global
  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);
  private final UcpContext context;
  private final MemoryPool memoryPool;
  private final UcpWorkerParams workerParams = new UcpWorkerParams().requestThreadSafety();
  private final UcpWorker globalWorker;
  private final UcxShuffleConf conf;
  // Mapping from spark's entity of BlockManagerId to UcxEntity workerAddress.
  private static final ConcurrentHashMap<BlockManagerId, ByteBuffer> workerAdresses =
    new ConcurrentHashMap<>();
  private final Thread listenerProgressThread;
  private boolean closed = false;

  // Driver
  private UcpListener listener;
  // Mapping from UcpEndpoint to ByteBuffer of RPC message, to introduce executor to cluster
  private static final ConcurrentHashMap<UcpEndpoint, ByteBuffer> rpcConnections =
    new ConcurrentHashMap<>();

  // Executor
  private UcpEndpoint globalDriverEndpoint;
  // Keep track of allocated workers to correctly close them.
  private static final Set<UcxWorkerWrapper> allocatedWorkers = ConcurrentHashMap.newKeySet();
  private final ThreadLocal<UcxWorkerWrapper> threadLocalWorker;

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    this.conf = conf;
    UcpParams params = new UcpParams().requestTagFeature()
      .requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true);
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);
    globalWorker = context.newWorker(workerParams);
    InetSocketAddress driverAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());

    if (isDriver) {
      startDriver(driverAddress);
    } else {
      startExecutor(driverAddress);
    }

    // Global listener thread, that keeps lazy progress for connection establishment
    listenerProgressThread = new UcxListenerThread(this, isDriver);
    listenerProgressThread.start();

    if (!isDriver) {
      memoryPool.preAlocate();
    }
    threadLocalWorker = ThreadLocal.withInitial(() -> {
      UcpWorker localWorker = context.newWorker(workerParams);
      UcxWorkerWrapper result = new UcxWorkerWrapper(localWorker,
        conf, allocatedWorkers.size());
      if (result.id() > conf.coresPerProcess()) {
        logger.warn("Thread: {} - creates new worker {} > numCores",
          Thread.currentThread().getId(), result.id());
      }
      allocatedWorkers.add(result);
      return result;
    });
  }

  private void startDriver(InetSocketAddress driverAddress) {
    // 1. Start listener on a driver and accept RPC messages from executors with their
    // worker addresses
    UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(driverAddress);
    listener = globalWorker.newListener(listenerParams);
    logger.info("Started UcxNode on {}", driverAddress);
  }

  private void startExecutor(InetSocketAddress driverAddress) {
    // 1. Executor: connect to driver using sockaddr
    // and send it's worker address followed by BlockManagerID.
    globalDriverEndpoint = globalWorker.newEndpoint(
      new UcpEndpointParams().setSocketAddress(driverAddress).setPeerErrorHadnlingMode()
    );
    BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
    ByteBuffer workerAddresss = globalWorker.getAddress();

    RegisteredMemory metadataMemory = memoryPool.get(conf.metadataRPCBufferSize());
    ByteBuffer metadataBuffer =  metadataMemory.getBuffer();
    metadataBuffer.order(ByteOrder.nativeOrder());
    metadataBuffer.putInt(workerAddresss.capacity());
    metadataBuffer.put(workerAddresss);
    try {
      SerializableBlockManagerID.serializeBlockManagerID(blockManagerId, metadataBuffer);
    } catch (IOException e) {
      String errorMsg = String.format("Failed to serialize %s: %s", blockManagerId,
        e.getMessage());
      throw new UcxException(errorMsg);
    }
    metadataBuffer.clear();
    // TODO: send using stream API when it would be available in jucx.
    globalDriverEndpoint.sendTaggedNonBlocking(metadataBuffer, new UcxCallback() {
      @Override
      public void onSuccess(UcxRequest request) {
        memoryPool.put(metadataMemory);
      }
    });
  }

  public UcxShuffleConf getConf() {
    return conf;
  }

  public UcpWorker getGlobalWorker() {
    return globalWorker;
  }

  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public UcpContext getContext() {
    return context;
  }

  /**
   * Get or initialize worker for current thread
   */
  public UcxWorkerWrapper getThreadLocalWorker() {
    return threadLocalWorker.get();
  }

  public static ConcurrentMap<BlockManagerId, ByteBuffer> getWorkerAddresses() {
    return workerAdresses;
  }

  public static ConcurrentMap<UcpEndpoint, ByteBuffer> getRpcConnections() {
    return rpcConnections;
  }

  @Override
  public void close() {
    threadLocalWorker.remove();
    if (!closed) {
      synchronized (this) {
        if (!closed) {
          logger.info("Stopping UcxNode");
          listenerProgressThread.interrupt();
          if (globalDriverEndpoint != null) {
            globalDriverEndpoint.close();
          }
          globalWorker.signal();
          if (listener != null) {
            listener.close();
            listener = null;
          }
          for (UcpEndpoint connection : rpcConnections.keySet()) {
            connection.close();
          }
          rpcConnections.clear();
          globalWorker.signal();
          memoryPool.close();
          globalWorker.close();
          allocatedWorkers.forEach(UcxWorkerWrapper::close);
          allocatedWorkers.clear();
          context.close();
          closed = true;
        }
      }
    }
  }
}

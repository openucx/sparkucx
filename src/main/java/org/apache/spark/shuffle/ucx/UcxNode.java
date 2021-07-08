/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.shuffle.ucx.rpc.SerializableBlockManagerID;
import org.apache.spark.shuffle.ucx.rpc.UcxListenerThread;
import org.apache.spark.storage.BlockManagerId;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
  private final boolean isDriver;
  private final UcpContext context;
  private final MemoryPool memoryPool;
  private final UcpWorkerParams workerParams = new UcpWorkerParams();
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
  private List<UcpEndpoint> backwardEndpoints = new ArrayList<>();

  // Executor
  private UcpEndpoint globalDriverEndpoint;
  // Keep track of allocated workers to correctly close them.
  private static final Set<UcxWorkerWrapper> allocatedWorkers = ConcurrentHashMap.newKeySet();
  private final ThreadLocal<UcxWorkerWrapper> threadLocalWorker;

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    this.conf = conf;
    this.isDriver = isDriver;
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
    UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(driverAddress)
      .setConnectionHandler(ucpConnectionRequest ->
        backwardEndpoints.add(globalWorker.newEndpoint(new UcpEndpointParams()
        .setConnectionRequest(ucpConnectionRequest))));
    listener = globalWorker.newListener(listenerParams);
    logger.info("Started UcxNode on {}", driverAddress);
  }

  /**
   * Allocates ByteBuffer from memoryPool and serializes there workerAddress,
   * followed by BlockManagerID
   * @return RegisteredMemory that holds metadata buffer.
   */
  private RegisteredMemory buildMetadataBuffer() {
    BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
    ByteBuffer workerAddresses = globalWorker.getAddress();

    RegisteredMemory metadataMemory = memoryPool.get(conf.metadataRPCBufferSize());
    ByteBuffer metadataBuffer = metadataMemory.getBuffer();
    metadataBuffer.putInt(workerAddresses.capacity());
    metadataBuffer.put(workerAddresses);
    try {
      SerializableBlockManagerID.serializeBlockManagerID(blockManagerId, metadataBuffer);
    } catch (IOException e) {
      String errorMsg = String.format("Failed to serialize %s: %s", blockManagerId,
        e.getMessage());
      throw new UcxException(errorMsg);
    }
    metadataBuffer.clear();
    return metadataMemory;
  }

  private void startExecutor(InetSocketAddress driverAddress) {
    // 1. Executor: connect to driver using sockaddr
    // and send it's worker address followed by BlockManagerID.
    globalDriverEndpoint = globalWorker.newEndpoint(
      new UcpEndpointParams().setSocketAddress(driverAddress).setPeerErrorHandlingMode()
    );

    RegisteredMemory metadataMemory = buildMetadataBuffer();
    // TODO: send using stream API when it would be available in jucx.
    globalDriverEndpoint.sendTaggedNonBlocking(metadataMemory.getBuffer(), new UcxCallback() {
      @Override
      public void onSuccess(UcpRequest request) {
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

  private void stopDriver() {
    listener.close();
    listener = null;
    rpcConnections.keySet().forEach(UcpEndpoint::close);
    rpcConnections.clear();
    backwardEndpoints.forEach(UcpEndpoint::close);
    backwardEndpoints.clear();
  }

  private void stopExecutor() {
    if (globalDriverEndpoint != null) {
      globalDriverEndpoint.close();
      globalDriverEndpoint = null;
    }
    allocatedWorkers.forEach(UcxWorkerWrapper::close);
    allocatedWorkers.clear();
  }

  @Override
  public void close() {
    threadLocalWorker.remove();
    synchronized (this) {
      if (!closed) {
        logger.info("Stopping UcxNode");
        listenerProgressThread.interrupt();
        globalWorker.signal();
        try {
          listenerProgressThread.join();
        } catch (InterruptedException e) {
          logger.error(e.getMessage());
          Thread.currentThread().interrupt();
        }

        if (isDriver) {
          stopDriver();
        } else {
          stopExecutor();
        }

        memoryPool.close();
        globalWorker.close();
        context.close();
        closed = true;
      }
    }
  }
}

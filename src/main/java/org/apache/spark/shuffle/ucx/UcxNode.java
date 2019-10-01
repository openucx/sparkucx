/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.storage.BlockManagerId;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.net.InetSocketAddress;
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
  private UcpWorker globalWorker;
  private UcpListener listener;
  private boolean closed = false;

  private final Set<UcxWorkerWrapper> workerPool = ConcurrentHashMap.newKeySet();

  private final ThreadLocal<UcxWorkerWrapper> worker;
  private final Thread listenerProgressThread;

  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);
  private static final AtomicInteger numWorkers = new AtomicInteger(0);

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    UcpParams params = new UcpParams().requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true)
      .setEstimatedNumEps(conf.coresPerProcess() * (long)conf.getNumProcesses());
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);
    globalWorker = context.newWorker(workerParams);
    InetSocketAddress socketAddress;
    if (isDriver) {
      socketAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());
    } else {
      BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
      socketAddress = new InetSocketAddress(blockManagerId.host(), blockManagerId.port() + 7);
    }
    UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(socketAddress);
    listener = globalWorker.newListener(listenerParams);
    logger.info("Started UcxNode on {}", socketAddress);

    // Global listener thread, that keeps lazy progress for connection establishment
    listenerProgressThread = new Thread() {
      @Override
      public void run() {
        while (!isInterrupted()) {
          try {
            if (globalWorker.progress() == 0) {
              globalWorker.waitForEvents();
            }
          } catch (Exception ex) {
            logger.error("Fail during progress. Stoping...");
            close();
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
        listenerProgressThread.interrupt();
        globalWorker.signal();
        memoryPool.close();
        listener.close();
        globalWorker.close();
        workerPool.forEach(UcxWorkerWrapper::close);
        workerPool.clear();
        context.close();
        closed = true;
      }
    }
  }
}

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
import java.util.Arrays;
import java.util.Map;
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

  private boolean closed = false;

  private static final Set<UcxWorkerWrapper> workerPool = ConcurrentHashMap.newKeySet();
  private static final Map<BlockManagerId, ByteBuffer> workerAdresses = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<UcpEndpoint, ByteBuffer> rpcConnections =
    new ConcurrentHashMap<>();

  private final ThreadLocal<UcxWorkerWrapper> worker;
  private final Thread listenerProgressThread;

  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);
  private static final AtomicInteger numWorkers = new AtomicInteger(0);

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    UcpParams params = new UcpParams().requestTagFeature()
      .requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true);
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);
    globalWorker = context.newWorker(workerParams);
    InetSocketAddress driverAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());


    if (isDriver) {
      // Start listener on a driver and accept RPC messages from executors with their
      // worker addresses
      UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(driverAddress);
      listener = globalWorker.newListener(listenerParams);
      logger.info("Started UcxNode on {}", driverAddress);
    } else {
      globalDriverEndpoint = globalWorker.newEndpoint(
        new UcpEndpointParams().setSocketAddress(driverAddress)//.setPeerErrorHadnlingMode()
      );
      BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
      ByteBuffer workerAddresss = globalWorker.getAddress();

      ByteBuffer metadataBuffer = Platform.allocateDirectBuffer(conf.metadataRPCBufferSize());
      metadataBuffer.order(ByteOrder.nativeOrder());
      metadataBuffer.putInt(workerAddresss.capacity());
      metadataBuffer.put(workerAddresss);
      try {
        ObjectOutputStream oos = new ObjectOutputStream(
          new ByteBufferOutputStream(metadataBuffer));
        blockManagerId.writeExternal(oos);
        oos.close();
      } catch (IOException e) {
        logger.error("Exception on serializing BlockManager: {}", e.getMessage());
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

      private UcxRequest recvRequest() {
        ByteBuffer metadataBuffer = Platform.allocateDirectBuffer(conf.metadataRPCBufferSize());
        metadataBuffer.order(ByteOrder.nativeOrder());
        return globalWorker.recvTaggedNonBlocking(metadataBuffer, new UcxCallback() {
          @Override
          public void onSuccess(UcxRequest request) {
            int workerAddressSize = metadataBuffer.getInt();
            ByteBuffer workerAddress = Platform.allocateDirectBuffer(workerAddressSize);
            for (int i = 0; i < workerAddressSize; i++) {
              workerAddress.put(metadataBuffer.get());
            }
            BlockManagerId blockManagerId = null;
            try {
              ObjectInputStream ois =
                new ObjectInputStream(new ByteBufferInputStream(metadataBuffer));
              blockManagerId = BlockManagerId.apply(ois);
              ois.close();
            } catch (IOException e) {
              logger.error("Failed to deserialize BlockManagerId: {}",
                e.getMessage());
            }
            logger.info("Received message from {}", blockManagerId);
            workerAddress.clear();
            if (isDriver) {
              metadataBuffer.clear();
              UcpEndpoint newConnection = globalWorker.newEndpoint(
                new UcpEndpointParams().setPeerErrorHadnlingMode()
                  .setUcpAddress(workerAddress));
              // For each existing connection
              rpcConnections.keySet().forEach(connection -> {
                // send address of joined worker to already connected workers
                connection.sendTaggedNonBlocking(metadataBuffer, null);
                // introduce other workers to joined worker
                newConnection.sendTaggedNonBlocking(rpcConnections.get(connection), null);
              });

              rpcConnections.put(newConnection, metadataBuffer);
            }
            workerAdresses.put(blockManagerId, workerAddress);
            synchronized (workerAdresses) {
              workerAdresses.notifyAll();
            }
          }
        });
      }

      @Override
      public void run() {
        UcxRequest recv = recvRequest();
        while (!isInterrupted()) {
          if (recv.isCompleted()) {
            recv = recvRequest();
          }
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

  public static Map<BlockManagerId, ByteBuffer> getWorkerAddresses() {
    return workerAdresses;
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
        if (globalDriverEndpoint != null) {
          globalDriverEndpoint.close();
        }
        globalWorker.signal();
        memoryPool.close();
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
        workerPool.forEach(UcxWorkerWrapper::close);
        workerPool.clear();
        context.close();
        closed = true;
      }
    }
  }
}

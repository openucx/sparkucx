/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.rpc;

import org.apache.spark.shuffle.ucx.UcxNode;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

/**
 * RPC processing logic. Both driver and excutor accepts the same RPC messgae:
 * executor worker address followed by it's serialized BlockManagerID.
 * Executor on accepting this message just adds workerAddress to the connection map.
 * Driver doing the logic of introducing connected executor to cluster nodes and
 * introduce cluster to connected executor.
 */
public class RpcConnectionCallback extends UcxCallback {
  private static final Logger logger = LoggerFactory.getLogger(RpcConnectionCallback.class);
  private final ByteBuffer metadataBuffer;
  private final boolean isDriver;
  private final UcxNode ucxNode;
  private static final ConcurrentMap<UcpEndpoint, ByteBuffer> rpcConnections =
    UcxNode.getRpcConnections();
  private static final ConcurrentMap<BlockManagerId, ByteBuffer> workerAdresses =
    UcxNode.getWorkerAddresses();

  RpcConnectionCallback(ByteBuffer metadataBuffer, boolean isDriver, UcxNode ucxNode) {
    this.metadataBuffer = metadataBuffer;
    this.isDriver = isDriver;
    this.ucxNode = ucxNode;
  }

  @Override
  public void onSuccess(UcxRequest request) {
    int workerAddressSize = metadataBuffer.getInt();
    ByteBuffer workerAddress = Platform.allocateDirectBuffer(workerAddressSize);
    /*Platform.copyMemory(null,
      UcxUtils.getAddress(metadataBuffer) + metadataBuffer.position(),
      null,
      UcxUtils.getAddress(workerAddress),
      workerAddressSize);
    metadataBuffer.position(metadataBuffer.position() + workerAddressSize);*/
    for (int i = 0; i < workerAddressSize; i++) {
      workerAddress.put(metadataBuffer.get());
    }
    BlockManagerId blockManagerId;
    try {
      blockManagerId = SerializableBlockManagerID
        .deserializeBlockManagerID(metadataBuffer);
    } catch (IOException e) {
      String errorMsg = String.format("Failed to deserialize BlockManagerId: %s", e.getMessage());
      throw new UcxException(errorMsg);
    }
    logger.debug("Received RPC message from {}", blockManagerId);
    UcpWorker globalWorker = ucxNode.getGlobalWorker();

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
}

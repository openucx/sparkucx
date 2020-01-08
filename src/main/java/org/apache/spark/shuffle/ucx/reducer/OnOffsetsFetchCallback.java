/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer;

import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.ShuffleBlockId;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;

import java.nio.ByteBuffer;

/**
 * Callback, called when got all offsets for blocks
 */
public class OnOffsetsFetchCallback extends ReducerCallback {
  private final RegisteredMemory offsetMemory;
  private final long[] dataAddresses;

  public OnOffsetsFetchCallback(ShuffleBlockId[] blockIds, UcpEndpoint endpoint,
                                UcxShuffleClient client, BlockFetchingListener listener,
                                RegisteredMemory offsetMemory, long[] dataAddresses) {
    super(blockIds, endpoint, client, listener);
    this.offsetMemory = offsetMemory;
    this.dataAddresses = dataAddresses;
  }

  @Override
  public void onSuccess(UcxRequest request) {
    ByteBuffer resultOffset = offsetMemory.getBuffer();
    long totalSize = 0;
    long[] sizes = new long[blockIds.length];
    for (int i = 0; i < blockIds.length; i++) {
      long blockOffset = resultOffset.getLong(i * 16);
      long blockLength = resultOffset.getLong(i * 16 + 8) - blockOffset;
      sizes[i] = blockLength;
      totalSize += blockLength;
      dataAddresses[i] += blockOffset;
    }

    if (totalSize <= 0 || totalSize >= Integer.MAX_VALUE) {
      throw new UcxException("Total size: " + totalSize);
    }
    mempool.put(offsetMemory);
    RegisteredMemory blocksMemory = mempool.get((int) totalSize);

    long offset = 0;
    // Submits N fetch blocks requests
    for (int i = 0; i < blockIds.length; i++) {
      endpoint.getNonBlockingImplicit(dataAddresses[i], client.dataRkeysCache.get(blockIds[i].mapId()),
        UcxUtils.getAddress(blocksMemory.getBuffer()) + offset, sizes[i]);
      offset += sizes[i];
    }

    // Process blocks when all fetched.
    // Flush guarantees that callback would invoke when all fetch requests will completed.
    endpoint.flushNonBlocking(new OnBlocksFetchCallback(this, blocksMemory, sizes));
  }
}

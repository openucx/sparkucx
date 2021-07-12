/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer.compat.spark_2_1;

import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.shuffle.ucx.UnsafeUtils;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.shuffle.ucx.reducer.OnBlocksFetchCallback;
import org.apache.spark.shuffle.ucx.reducer.ReducerCallback;
import org.apache.spark.storage.ShuffleBlockId;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Callback, called when got all offsets for blocks
 */
public class OnOffsetsFetchCallback extends ReducerCallback {
  private final RegisteredMemory offsetMemory;
  private final long[] dataAddresses;
  private Map<Integer, UcpRemoteKey> dataRkeysCache;

  public OnOffsetsFetchCallback(ShuffleBlockId[] blockIds, UcpEndpoint endpoint, BlockFetchingListener listener,
                                RegisteredMemory offsetMemory, long[] dataAddresses,
                                Map<Integer, UcpRemoteKey> dataRkeysCache) {
    super(blockIds, endpoint, listener);
    this.offsetMemory = offsetMemory;
    this.dataAddresses = dataAddresses;
    this.dataRkeysCache = dataRkeysCache;
  }

  @Override
  public void onSuccess(UcpRequest request) {
    ByteBuffer resultOffset = offsetMemory.getBuffer();
    long totalSize = 0;
    int[] sizes = new int[blockIds.length];
    int offsetSize = UnsafeUtils.LONG_SIZE;
    for (int i = 0; i < blockIds.length; i++) {
      // Blocks in metadata buffer are in form | blockOffsetStart | blockOffsetEnd |
      long blockOffset = resultOffset.getLong(i * 2 * offsetSize);
      long blockLength = resultOffset.getLong(i * 2 * offsetSize + offsetSize) - blockOffset;
      assert (blockLength > 0) && (blockLength <= Integer.MAX_VALUE);
      sizes[i] = (int) blockLength;
      totalSize += blockLength;
      dataAddresses[i] += blockOffset;
    }

    assert  (totalSize > 0) && (totalSize < Integer.MAX_VALUE);
    mempool.put(offsetMemory);
    RegisteredMemory blocksMemory = mempool.get((int) totalSize);

    long offset = 0;
    // Submits N fetch blocks requests
    for (int i = 0; i < blockIds.length; i++) {
      endpoint.getNonBlockingImplicit(dataAddresses[i], dataRkeysCache.get(((ShuffleBlockId)blockIds[i]).mapId()),
        UcxUtils.getAddress(blocksMemory.getBuffer()) + offset, sizes[i]);
      offset += sizes[i];
    }

    // Process blocks when all fetched.
    // Flush guarantees that callback would invoke when all fetch requests will completed.
    endpoint.flushNonBlocking(new OnBlocksFetchCallback(this, blocksMemory, sizes));
  }
}

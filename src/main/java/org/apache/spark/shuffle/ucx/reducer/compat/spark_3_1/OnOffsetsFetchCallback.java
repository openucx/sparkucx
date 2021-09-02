/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer.compat.spark_3_1;

import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.UnsafeUtils;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.shuffle.ucx.reducer.ReducerCallback;
import org.apache.spark.shuffle.ucx.reducer.OnBlocksFetchCallback;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.ShuffleBlockBatchId;
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
  private final Map<Long, Integer> mapId2PartitionId;

  public OnOffsetsFetchCallback(BlockId[] blockIds, UcpEndpoint endpoint, BlockFetchingListener listener,
                                RegisteredMemory offsetMemory, long[] dataAddresses,
                                Map<Integer, UcpRemoteKey> dataRkeysCache,
                                Map<Long, Integer> mapId2PartitionId) {
    super(blockIds, endpoint, listener);
    this.offsetMemory = offsetMemory;
    this.dataAddresses = dataAddresses;
    this.dataRkeysCache = dataRkeysCache;
    this.mapId2PartitionId = mapId2PartitionId;
  }

  @Override
  public void onSuccess(UcpRequest request) {
    ByteBuffer resultOffset = offsetMemory.getBuffer();
    long totalSize = 0;
    int[] sizes = new int[blockIds.length];
    int offset = 0;
    long blockOffset;
    long blockLength;
    int offsetSize = UnsafeUtils.LONG_SIZE;
    for (int i = 0; i < blockIds.length; i++) {
      // Blocks in metadata buffer are in form | blockOffsetStart | blockOffsetEnd |
      if (blockIds[i] instanceof ShuffleBlockBatchId) {
        ShuffleBlockBatchId blockBatchId = (ShuffleBlockBatchId) blockIds[i];
        int blocksInBatch = blockBatchId.endReduceId() - blockBatchId.startReduceId();
        blockOffset = resultOffset.getLong(offset * 2 * offsetSize);
        blockLength = resultOffset.getLong(offset * 2 * offsetSize + offsetSize * blocksInBatch)
          - blockOffset;
        offset += blocksInBatch;
      } else {
        blockOffset = resultOffset.getLong(offset * 16);
        blockLength = resultOffset.getLong(offset * 16 + 8) - blockOffset;
        offset++;
      }

      assert (blockLength > 0) && (blockLength <= Integer.MAX_VALUE);
      sizes[i] = (int) blockLength;
      totalSize += blockLength;
      dataAddresses[i] += blockOffset;
    }

    assert  (totalSize > 0) &&  (totalSize < Integer.MAX_VALUE);
    mempool.put(offsetMemory);
    RegisteredMemory blocksMemory = mempool.get((int) totalSize);

    offset = 0;
    // Submits N fetch blocks requests
    for (int i = 0; i < blockIds.length; i++) {
      int mapPartitionId = (blockIds[i] instanceof ShuffleBlockId) ?
        mapId2PartitionId.get(((ShuffleBlockId)blockIds[i]).mapId()) :
        mapId2PartitionId.get(((ShuffleBlockBatchId)blockIds[i]).mapId());
      endpoint.getNonBlockingImplicit(dataAddresses[i], dataRkeysCache.get(mapPartitionId),
        UcxUtils.getAddress(blocksMemory.getBuffer()) + offset, sizes[i]);
      offset += sizes[i];
    }

    // Process blocks when all fetched.
    // Flush guarantees that callback would invoke when all fetch requests will completed.
    endpoint.flushNonBlocking(new OnBlocksFetchCallback(this, blocksMemory, sizes));
  }
}

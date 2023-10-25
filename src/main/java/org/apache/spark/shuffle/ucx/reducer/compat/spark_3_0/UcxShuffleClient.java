/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer.compat.spark_3_0;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.BlockStoreClient;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.shuffle.DriverMetadata;
import org.apache.spark.shuffle.UcxShuffleManager;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.UnsafeUtils;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.*;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


import java.util.HashMap;
import java.util.Map;

public class UcxShuffleClient extends BlockStoreClient {
  private static final Logger logger = LoggerFactory.getLogger(UcxShuffleClient.class);
  private final UcxWorkerWrapper workerWrapper;
  private final Map<Long, Integer> mapId2PartitionId;
  private final TempShuffleReadMetrics shuffleReadMetrics;
  private final int shuffleId;
  final HashMap<Integer, UcpRemoteKey> offsetRkeysCache = new HashMap<>();
  final HashMap<Integer, UcpRemoteKey> dataRkeysCache = new HashMap<>();


  public UcxShuffleClient(int shuffleId, UcxWorkerWrapper workerWrapper,
                          Map<Long, Integer> mapId2PartitionId,  TempShuffleReadMetrics shuffleReadMetrics) {
    this.workerWrapper = workerWrapper;
    this.shuffleId = shuffleId;
    this.mapId2PartitionId = mapId2PartitionId;
    this.shuffleReadMetrics = shuffleReadMetrics;
  }

  /**
   * Submits n non blocking fetch offsets to get needed offsets for n blocks.
   */
  private void submitFetchOffsets(UcpEndpoint endpoint, BlockId[] blockIds,
                                  RegisteredMemory offsetMemory,
                                  long[] dataAddresses) {
    DriverMetadata driverMetadata =  workerWrapper.fetchDriverMetadataBuffer(shuffleId);
    long offset = 0;
    int startReduceId;
    long size;

    for (int i = 0; i < blockIds.length; i++) {
      BlockId blockId = blockIds[i];
      int mapIdpartition;

      if (blockId instanceof ShuffleBlockId) {
        ShuffleBlockId shuffleBlockId = (ShuffleBlockId) blockId;
        mapIdpartition = mapId2PartitionId.get(shuffleBlockId.mapId());
        size = 2L * UnsafeUtils.LONG_SIZE;
        startReduceId = shuffleBlockId.reduceId();
      } else {
        ShuffleBlockBatchId shuffleBlockBatchId = (ShuffleBlockBatchId) blockId;
        mapIdpartition = mapId2PartitionId.get(shuffleBlockBatchId.mapId());
        size = (shuffleBlockBatchId.endReduceId() - shuffleBlockBatchId.startReduceId())
          * 2L * UnsafeUtils.LONG_SIZE;
        startReduceId = shuffleBlockBatchId.startReduceId();
      }

      long offsetAddress = driverMetadata.offsetAddress(mapIdpartition);
      dataAddresses[i] = driverMetadata.dataAddress(mapIdpartition);

      offsetRkeysCache.computeIfAbsent(mapIdpartition, mapId ->
        endpoint.unpackRemoteKey(driverMetadata.offsetRkey(mapIdpartition)));

      dataRkeysCache.computeIfAbsent(mapIdpartition, mapId ->
        endpoint.unpackRemoteKey(driverMetadata.dataRkey(mapIdpartition)));

      endpoint.getNonBlockingImplicit(
        offsetAddress + startReduceId * UnsafeUtils.LONG_SIZE,
        offsetRkeysCache.get(mapIdpartition),
        UcxUtils.getAddress(offsetMemory.getBuffer()) + offset,
        size);

      offset += size;
    }
  }

  @Override
  public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener,
                          DownloadFileManager downloadFileManager) {
    long startTime = System.currentTimeMillis();
    BlockManagerId blockManagerId = BlockManagerId.apply(execId, host, port, Option.empty());
    UcpEndpoint endpoint = workerWrapper.getConnection(blockManagerId);
    long[] dataAddresses = new long[blockIds.length];
    int totalBlocks = 0;

    BlockId[] blocks = new BlockId[blockIds.length];

    for (int i = 0; i < blockIds.length; i++) {
      blocks[i] = BlockId.apply(blockIds[i]);
      if (blocks[i] instanceof ShuffleBlockId) {
        totalBlocks += 1;
      } else {
        ShuffleBlockBatchId blockBatchId = (ShuffleBlockBatchId)blocks[i];
        totalBlocks += (blockBatchId.endReduceId() - blockBatchId.startReduceId());
      }
    }

    RegisteredMemory offsetMemory = ((UcxShuffleManager)SparkEnv.get().shuffleManager())
      .ucxNode().getMemoryPool().get(totalBlocks * 2 * UnsafeUtils.LONG_SIZE);
    // Submits N implicit get requests without callback
    submitFetchOffsets(endpoint, blocks, offsetMemory, dataAddresses);

    // flush guarantees that all that requests completes when callback is called.
    endpoint.flushNonBlocking(
      new OnOffsetsFetchCallback(blocks, endpoint, listener, offsetMemory,
        dataAddresses, dataRkeysCache, mapId2PartitionId));

    shuffleReadMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime);
  }

  @Override
  public void close() {
    offsetRkeysCache.values().forEach(UcpRemoteKey::close);
    dataRkeysCache.values().forEach(UcpRemoteKey::close);
    logger.info("Shuffle read metrics, fetch wait time: {}ms", shuffleReadMetrics.fetchWaitTime());
  }

}

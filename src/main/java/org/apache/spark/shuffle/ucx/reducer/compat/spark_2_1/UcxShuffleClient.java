/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer.compat.spark_2_1;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.ShuffleClient;
import org.apache.spark.shuffle.DriverMetadata;
import org.apache.spark.shuffle.UcxShuffleManager;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.UnsafeUtils;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.Arrays;
import java.util.HashMap;

public class UcxShuffleClient extends ShuffleClient {
  private final MemoryPool mempool;
  private static final Logger logger = LoggerFactory.getLogger(UcxShuffleClient.class);
  private final UcxShuffleManager ucxShuffleManager;
  private final TempShuffleReadMetrics shuffleReadMetrics;
  private final UcxWorkerWrapper workerWrapper;
  final HashMap<Integer, UcpRemoteKey> offsetRkeysCache = new HashMap<>();
  final HashMap<Integer, UcpRemoteKey> dataRkeysCache = new HashMap<>();

  public UcxShuffleClient(TempShuffleReadMetrics shuffleReadMetrics,
                          UcxWorkerWrapper workerWrapper) {
    this.ucxShuffleManager = (UcxShuffleManager) SparkEnv.get().shuffleManager();
    this.mempool = ucxShuffleManager.ucxNode().getMemoryPool();
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.workerWrapper = workerWrapper;
  }

  /**
   * Submits n non blocking fetch offsets to get needed offsets for n blocks.
   */
  private void submitFetchOffsets(UcpEndpoint endpoint, ShuffleBlockId[] blockIds,
                                  long[] dataAddresses, RegisteredMemory offsetMemory) {
    DriverMetadata driverMetadata =  workerWrapper.fetchDriverMetadataBuffer(blockIds[0].shuffleId());
    for (int i = 0; i < blockIds.length; i++) {
      ShuffleBlockId blockId = blockIds[i];

      long offsetAddress = driverMetadata.offsetAddress(blockId.mapId());
      dataAddresses[i] = driverMetadata.dataAddress(blockId.mapId());

      offsetRkeysCache.computeIfAbsent(blockId.mapId(), mapId ->
        endpoint.unpackRemoteKey(driverMetadata.offsetRkey(blockId.mapId())));

      dataRkeysCache.computeIfAbsent(blockId.mapId(), mapId ->
        endpoint.unpackRemoteKey(driverMetadata.dataRkey(blockId.mapId())));

      endpoint.getNonBlockingImplicit(
        offsetAddress + blockId.reduceId() * UnsafeUtils.LONG_SIZE,
          offsetRkeysCache.get(blockId.mapId()),
        UcxUtils.getAddress(offsetMemory.getBuffer()) + (i * 2L * UnsafeUtils.LONG_SIZE),
        2L * UnsafeUtils.LONG_SIZE);
    }
  }

  /**
   * Reducer entry point. Fetches remote blocks, using 2 ucp_get calls.
   * This method is inside ShuffleFetchIterator's for loop over hosts.
   * First fetches block offset from index file, and then fetches block itself.
   */
  @Override
  public void fetchBlocks(String host, int port, String execId,
                          String[] blockIds, BlockFetchingListener listener) {
    long startTime = System.currentTimeMillis();

    BlockManagerId blockManagerId = BlockManagerId.apply(execId, host, port, Option.empty());
    UcpEndpoint endpoint = workerWrapper.getConnection(blockManagerId);

    long[] dataAddresses = new long[blockIds.length];

    // Need to fetch 2 long offsets current block + next block to calculate exact block size.
    RegisteredMemory offsetMemory = mempool.get(2 * UnsafeUtils.LONG_SIZE * blockIds.length);

    ShuffleBlockId[] shuffleBlockIds = Arrays.stream(blockIds)
      .map(blockId -> (ShuffleBlockId) BlockId.apply(blockId)).toArray(ShuffleBlockId[]::new);

    // Submits N implicit get requests without callback
    submitFetchOffsets(endpoint, shuffleBlockIds, dataAddresses, offsetMemory);

    // flush guarantees that all that requests completes when callback is called.
    endpoint.flushNonBlocking(
      new OnOffsetsFetchCallback(shuffleBlockIds, endpoint, listener, offsetMemory,
        dataAddresses, dataRkeysCache));
    shuffleReadMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime);
  }

  @Override
  public void close() {
    offsetRkeysCache.values().forEach(UcpRemoteKey::close);
    dataRkeysCache.values().forEach(UcpRemoteKey::close);
    logger.info("Shuffle read metrics, fetch wait time: {}ms", shuffleReadMetrics.fetchWaitTime());
  }
}

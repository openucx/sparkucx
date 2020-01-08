/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.network.shuffle.ShuffleClient;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.UcxShuffleManager;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.shuffle.ucx.memory.RegisteredMemory;
import org.apache.spark.storage.BlockId$;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.openucx.jucx.UcxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import scala.Option;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public class UcxShuffleClient extends ShuffleClient {
  private final MemoryPool mempool;
  private static final Logger logger = LoggerFactory.getLogger(UcxShuffleClient.class);
  private final UcxShuffleManager ucxShuffleManager;
  private final ShuffleHandle handle;
  private final TempShuffleReadMetrics shuffleReadMetrics;
  private final UcxWorkerWrapper workerWrapper;
  final HashMap<Integer, UcpRemoteKey> offsetRkeysCache = new HashMap<>();
  final HashMap<Integer, UcpRemoteKey> dataRkeysCache = new HashMap<>();

  public UcxShuffleClient(ShuffleHandle handle,
                          TempShuffleReadMetrics shuffleReadMetrics,
                          UcxWorkerWrapper workerWrapper) {
    this.ucxShuffleManager = (UcxShuffleManager) SparkEnv.get().shuffleManager();
    this.mempool = ucxShuffleManager.ucxNode().getMemoryPool();
    this.handle = handle;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.workerWrapper = workerWrapper;
  }

  /**
   * Submits n non blocking fetch offsets to get needed offsets for n blocks.
   */
  private void submitFetchOffsets(UcpEndpoint endpoint, ShuffleBlockId[] blockIds,
                                  long[] dataAddresses, RegisteredMemory offsetMemory) {
    ByteBuffer driverMetadata =  workerWrapper.fetchDriverMetadataBuffer(handle.shuffleId()).data();
    for (int i = 0; i < blockIds.length; i++) {
      ShuffleBlockId blockId = blockIds[i];
      // Get block offset
      int mapIdBlock = blockId.mapId() *
        (int) ucxShuffleManager.ucxShuffleConf().metadataBlockSize();
      int offsetWithinBlock = 0;
      // Parse metadata fetched from driver.
      long offsetAdress = driverMetadata.getLong(mapIdBlock + offsetWithinBlock);
      offsetWithinBlock += 8;
      long dataAddress = driverMetadata.getLong(mapIdBlock + offsetWithinBlock);
      dataAddresses[i] = dataAddress;
      offsetWithinBlock += 8;

      // Unpack Rkeys for this workerWrapper
      if (offsetRkeysCache.get(blockId.mapId()) == null ||
        dataRkeysCache.get(blockId.mapId()) == null) {
        int offsetRKeySize = driverMetadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;
        int dataRkeySize = driverMetadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;

        if (offsetRKeySize <= 0 || dataRkeySize <= 0) {
          throw new UcxException("Wrong rkey size.");
        }
        final ByteBuffer rkeyCopy = driverMetadata.slice();
        rkeyCopy.position(mapIdBlock + offsetWithinBlock)
          .limit(mapIdBlock + offsetWithinBlock + offsetRKeySize);

        offsetWithinBlock += offsetRKeySize;

        offsetRkeysCache.computeIfAbsent(blockId.mapId(), mapId -> endpoint.unpackRemoteKey(rkeyCopy));

        rkeyCopy.position(mapIdBlock + offsetWithinBlock)
          .limit(mapIdBlock + offsetWithinBlock + dataRkeySize);

        dataRkeysCache.computeIfAbsent(blockId.mapId(), mapId -> endpoint.unpackRemoteKey(rkeyCopy));
      }

      endpoint.getNonBlockingImplicit(
        offsetAdress + blockId.reduceId() * 8L, offsetRkeysCache.get(blockId.mapId()),
        UcxUtils.getAddress(offsetMemory.getBuffer()) + (i * 16), 16);
    }
  }

  /**
   * Reducer entry point. Fetches remote blocks, using 2 ucp_get calls.
   * This method is inside ShuffleFetchIterator's for loop over hosts.
   * First fetches block offset from index file, and then fetches block itself.
   */
  @Override
  public void fetchBlocks(String host, int port, String execId,
                          String[] blockIds, BlockFetchingListener listener,
                          DownloadFileManager downloadFileManager) {
    long startTime = System.currentTimeMillis();

    BlockManagerId blockManagerId = BlockManagerId.apply(execId, host, port, Option.empty());
    UcpEndpoint endpoint = workerWrapper.getConnection(blockManagerId);

    long[] dataAddresses = new long[blockIds.length];

    RegisteredMemory offsetMemory = mempool.get(16 * blockIds.length);

    ShuffleBlockId[] shuffleBlockIds = Arrays.stream(blockIds)
      .map(blockId -> (ShuffleBlockId)BlockId$.MODULE$.apply(blockId)).toArray(ShuffleBlockId[]::new);

    // Submits N implicit get requests without callback
    submitFetchOffsets(endpoint, shuffleBlockIds, dataAddresses, offsetMemory);

    // flush guarantees that all that requests completes when callback is called.
    // TODO: fix https://github.com/openucx/ucx/issues/4267 and use endpoint flush.
    workerWrapper.worker().flushNonBlocking(
      new OnOffsetsFetchCallback(shuffleBlockIds, endpoint, this, listener, offsetMemory,
        dataAddresses));
    shuffleReadMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime);
  }

  @Override
  public void close() {
    offsetRkeysCache.values().forEach(UcpRemoteKey::close);
    dataRkeysCache.values().forEach(UcpRemoteKey::close);
    logger.info("Shuffle read metrics, fetch wait time: {}ms", shuffleReadMetrics.fetchWaitTime());
  }
}

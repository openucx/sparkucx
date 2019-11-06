/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
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
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import scala.Option;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class UcxShuffleClient extends ShuffleClient {
  private final MemoryPool mempool;
  private static final Logger logger = LoggerFactory.getLogger(UcxShuffleClient.class);
  private final UcxShuffleManager ucxShuffleManager;
  private final ShuffleHandle handle;
  private final TempShuffleReadMetrics shuffleReadMetrics;

  public UcxShuffleClient(ShuffleHandle handle,
                          TempShuffleReadMetrics shuffleReadMetrics) {
    this.ucxShuffleManager = (UcxShuffleManager) SparkEnv.get().shuffleManager();
    this.mempool = ucxShuffleManager.ucxNode().getMemoryPool();
    this.handle = handle;
    this.shuffleReadMetrics = shuffleReadMetrics;
  }

  @Override
  public void fetchBlocks(String host, int port, String execId,
                          String[] blockIds, BlockFetchingListener listener,
                          DownloadFileManager downloadFileManager) {
    long startTime = System.currentTimeMillis();
    UcxWorkerWrapper workerWrapper = ((UcxShuffleManager)SparkEnv.get().shuffleManager())
      .ucxNode().getThreadLocalWorker();
    workerWrapper.addDriverMetadata(handle);
    workerWrapper.fetchDriverMetadataBuffer(handle);
    BlockManagerId blockManagerId = BlockManagerId.apply(execId, host, port, Option.empty());
    UcpEndpoint endpoint = workerWrapper.getConnection(blockManagerId);
    ByteBuffer metadata = workerWrapper.driverMetadataBuffer().get(handle.shuffleId()).get().data();

    UcpRemoteKey[] offsetRkeyArray = workerWrapper.offsetRkeyCache().get(handle.shuffleId()).get();
    UcpRemoteKey[] dataRkeyArray = workerWrapper.dataRkeyCache().get(handle.shuffleId()).get();

    long[] dataAddresses = new long[blockIds.length];
    UcpRemoteKey[] dataRkeys = new UcpRemoteKey[blockIds.length];

    RegisteredMemory offsetMemory = mempool.get(16 * blockIds.length);
    ByteBuffer resultOffset = offsetMemory.getBuffer();

    for (int i = 0; i < blockIds.length; i++) {
      ShuffleBlockId blockId = (ShuffleBlockId) BlockId$.MODULE$.apply(blockIds[i]);

      // Get block offset
      int mapIdBlock = blockId.mapId() *
        (int) ucxShuffleManager.ucxShuffleConf().metadataBlockSize();
      int offsetWithinBlock = 0;
      long offsetAdress = metadata.getLong(mapIdBlock + offsetWithinBlock);
      offsetWithinBlock += 8;
      long dataAddress = metadata.getLong(mapIdBlock + offsetWithinBlock);
      dataAddresses[i] = dataAddress;
      offsetWithinBlock += 8;

      if (offsetRkeyArray[blockId.mapId()] == null ||
        dataRkeyArray[blockId.mapId()] == null) {
        int offsetRKeySize = metadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;
        int dataRkeySize = metadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;


        if (offsetRKeySize <= 0 || dataRkeySize <= 0) {
          throw new UcxException("Wrong rkey size.");
        }
        final ByteBuffer rkeyCopy = metadata.slice();
        rkeyCopy.position(mapIdBlock + offsetWithinBlock)
          .limit(mapIdBlock + offsetWithinBlock + offsetRKeySize);


        offsetWithinBlock += offsetRKeySize;

        UcpRemoteKey offsetRkey = endpoint.unpackRemoteKey(rkeyCopy);
        offsetRkeyArray[blockId.mapId()] = offsetRkey;

        rkeyCopy.position(mapIdBlock + offsetWithinBlock)
          .limit(mapIdBlock + offsetWithinBlock + dataRkeySize);
        UcpRemoteKey dataMemory = endpoint.unpackRemoteKey(rkeyCopy);

        dataRkeyArray[blockId.mapId()] = dataMemory;
      }

      endpoint.getNonBlockingImplicit(
        offsetAdress + blockId.reduceId() * 8L, offsetRkeyArray[blockId.mapId()],
        UcxUtils.getAddress(resultOffset) + (i * 16), 16);

      dataRkeys[i] = dataRkeyArray[blockId.mapId()];
    }

    workerWrapper.worker().flushNonBlocking(new UcxCallback() {
      @Override
      public void onSuccess(UcxRequest request) {
        long totalSize = 0;
        long[] sizes = new long[blockIds.length];
        for (int i = 0; i < blockIds.length; i++) {
          long blockOffset = resultOffset.getLong(i * 16);
          long blockLength = resultOffset.getLong(i * 16 + 8) - blockOffset;
          assert blockLength > 0 && blockLength < Integer.MAX_VALUE;
          sizes[i] = blockLength;
          totalSize += blockLength;
          dataAddresses[i] += blockOffset;
        }
        mempool.put(offsetMemory);

        if (totalSize <= 0 || totalSize >= Integer.MAX_VALUE) {
          throw new UcxException("Total size: " + totalSize);
        }
        RegisteredMemory blocksMemory = mempool.get((int) totalSize);

        long offset = 0;
        for (int i = 0; i < blockIds.length; i++) {
          endpoint.getNonBlockingImplicit(dataAddresses[i], dataRkeys[i],
            UcxUtils.getAddress(blocksMemory.getBuffer()) + offset, sizes[i]);
          offset += sizes[i];
        }

        endpoint.flushNonBlocking(new UcxCallback() {
          @Override
          public void onSuccess(UcxRequest request) {
            int position = 0;
            AtomicInteger refCount = new AtomicInteger(blockIds.length);

            for (int i = 0; i < blockIds.length; i++) {
              String block = blockIds[i];
              blocksMemory.getBuffer().position(position).limit(position + (int) sizes[i]);
              ByteBuffer blockBuffer = blocksMemory.getBuffer().slice();
              assert blockBuffer.remaining() > 0;
              position += (int) sizes[i];
              listener.onBlockFetchSuccess(block, new NioManagedBuffer(blockBuffer) {
                @Override
                public ManagedBuffer release() {
                  if (refCount.decrementAndGet() == 0) {
                    mempool.put(blocksMemory);
                  }
                  return this;
                }
              });
            }
          }
        });
      }
    });
    shuffleReadMetrics.incFetchWaitTime(System.currentTimeMillis() - startTime);
  }

  @Override
  public void close() {
    logger.info("Shuffle read metrics, fetch wait time: {}ms", shuffleReadMetrics.fetchWaitTime());
  }
}

/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer;

import org.apache.spark.SparkEnv;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.shuffle.UcxShuffleManager;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.storage.ShuffleBlockId;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common data needed for offset fetch callback and subsequent block fetch callback.
 */
public abstract class ReducerCallback extends UcxCallback {
  protected MemoryPool mempool;
  protected ShuffleBlockId[] blockIds;
  protected UcpEndpoint endpoint;
  protected UcxShuffleClient client;
  protected BlockFetchingListener listener;
  protected static final Logger logger = LoggerFactory.getLogger(ReducerCallback.class);
  long startTime = System.currentTimeMillis();

  public ReducerCallback(ShuffleBlockId[] blockIds, UcpEndpoint endpoint,
                         UcxShuffleClient client, BlockFetchingListener listener) {
    this.mempool = ((UcxShuffleManager)SparkEnv.get().shuffleManager()).ucxNode().getMemoryPool();
    this.blockIds = blockIds;
    this.endpoint = endpoint;
    this.client = client;
    this.listener = listener;
  }

  public ReducerCallback(ReducerCallback callback) {
    this.blockIds = callback.blockIds;
    this.client = callback.client;
    this.endpoint = callback.endpoint;
    this.listener = callback.listener;
    this.mempool = callback.mempool;
    this.startTime = callback.startTime;
  }
}

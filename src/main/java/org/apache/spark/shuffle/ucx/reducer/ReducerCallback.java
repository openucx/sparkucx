/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer;

import org.apache.spark.SparkEnv;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.shuffle.CommonUcxShuffleManager;
import org.apache.spark.shuffle.ucx.memory.MemoryPool;
import org.apache.spark.storage.BlockId;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common data needed for offset fetch callback and subsequent block fetch callback.
 */
public abstract class ReducerCallback extends UcxCallback {
    protected MemoryPool mempool;
    protected BlockId[] blockIds;
    protected UcpEndpoint endpoint;
    protected BlockFetchingListener listener;
    protected static final Logger logger = LoggerFactory.getLogger(ReducerCallback.class);
    protected long startTime = System.currentTimeMillis();

    public ReducerCallback(BlockId[] blockIds, UcpEndpoint endpoint, BlockFetchingListener listener) {
        this.mempool = ((CommonUcxShuffleManager) SparkEnv.get().shuffleManager()).ucxNode().getMemoryPool();
        this.blockIds = blockIds;
        this.endpoint = endpoint;
        this.listener = listener;
    }

    public ReducerCallback(ReducerCallback callback) {
        this.blockIds = callback.blockIds;
        this.endpoint = callback.endpoint;
        this.listener = callback.listener;
        this.mempool = callback.mempool;
        this.startTime = callback.startTime;
    }
}

/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.reducer.compat.spark_3_0;

import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.BlockStoreClient;
import org.apache.spark.network.shuffle.DownloadFileManager;


public class UcxShuffleClient extends BlockStoreClient {

  @Override
  public void close() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener,
                          DownloadFileManager downloadFileManager) {
    throw new UnsupportedOperationException("TODO");
  }
}

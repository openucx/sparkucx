/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Utility class to serialize / deserialize metadata buffer on a driver.
 * Needed to propagate metadata buffer information to executors using
 * spark's mechanism to broadcast tasks.
 */
public class UcxRemoteMemory implements Serializable {
  private long address;
  private ByteBuffer rkeyBuffer;

  public UcxRemoteMemory(long address, ByteBuffer rkeyBuffer) {
    this.address = address;
    this.rkeyBuffer = rkeyBuffer;
  }

  public UcxRemoteMemory() {}

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeLong(address);
    out.writeInt(rkeyBuffer.limit());
    byte[] copy = new byte[rkeyBuffer.limit()];
    rkeyBuffer.clear();
    rkeyBuffer.get(copy);
    out.write(copy);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    this.address = in.readLong();
    int bufferSize = in.readInt();
    byte[] buffer = new byte[bufferSize];
    in.read(buffer, 0, bufferSize);
    this.rkeyBuffer = ByteBuffer.allocateDirect(bufferSize).put(buffer);
    this.rkeyBuffer.clear();
  }
}

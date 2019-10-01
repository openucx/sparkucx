/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.rpc

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer

/**
 * Utility class to serialize / deserialize driver metadata memory (address + rkey)
 * in order to transmit metadata over Spark RPC layer.
 */
case class UcxRemoteMemory(var address: Long, var rKeyBuffer: ByteBuffer)
  extends java.io.Serializable {

  private def writeObject(out: ObjectOutputStream ) {
    out.writeLong(address)
    out.writeInt(rKeyBuffer.limit())
    val copy = new Array[Byte](rKeyBuffer.limit())
    rKeyBuffer.clear()
    rKeyBuffer.get(copy)
    out.write(copy)
  }

  private def readObject(in: ObjectInputStream) {
    address = in.readLong()
    val bufferSize = in.readInt()
    val buffer = new Array[Byte](bufferSize)
    in.read(buffer, 0, bufferSize)
    this.rKeyBuffer = ByteBuffer.allocateDirect(bufferSize).put(buffer)
    this.rKeyBuffer.clear()
  }

}

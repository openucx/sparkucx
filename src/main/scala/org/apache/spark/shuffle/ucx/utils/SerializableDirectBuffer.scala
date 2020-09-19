/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.utils

import java.io.{EOFException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A wrapper around a java.nio.ByteBuffer that is serializable through Java serialization, to make
 * it easier to pass ByteBuffers in case class messages.
 */
class SerializableDirectBuffer(@transient var buffer: ByteBuffer) extends Serializable
  with Logging {

  def value: ByteBuffer = buffer

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    buffer = ByteBuffer.allocateDirect(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(buffer.limit())
    buffer.rewind()
    while (buffer.position() < buffer.limit()) {
      out.write(buffer.get())
    }
    buffer.rewind() // Allow us to write it again later
  }
}

class DeserializableToExternalMemoryBuffer(@transient var buffer: ByteBuffer)() extends Serializable
  with Logging {

  def value: ByteBuffer = buffer

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }
}

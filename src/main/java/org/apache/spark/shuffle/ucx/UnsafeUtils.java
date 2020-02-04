/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;

import org.openucx.jucx.UcxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Java's native mmap functionality, that allows to mmap files > 2GB.
 */
public class UnsafeUtils {
  private static final Method mmap;
  private static final Method unmmap;
  private static final Logger logger = LoggerFactory.getLogger(UnsafeUtils.class);

  private static final Constructor<?> directBufferConstructor;

  static {
    try {
      mmap = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
      mmap.setAccessible(true);
      unmmap = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
      unmmap.setAccessible(true);
      Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
      directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
      directBufferConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private UnsafeUtils() {}

  public static long mmap(FileChannel fileChannel, long offset, long length) {
    long result;
    try {
      result = (long)mmap.invoke(fileChannel, 1, offset, length);
    } catch (Exception e) {
      logger.error("MMap({}, {}) failed: {}", offset, length, e.getMessage());
      throw new UcxException(e.getMessage());
    }
    return result;
  }

  public static void munmap(long address, long length) {
    try {
      unmmap.invoke(null, address, length);
    } catch (IllegalAccessException | InvocationTargetException e) {
      logger.error(e.getMessage());
    }
  }

  public static ByteBuffer getByteBuffer(long address, int length) throws IOException {
    try {
      return (ByteBuffer)directBufferConstructor.newInstance(address, length);
    } catch (InvocationTargetException ex) {
      throw new IOException("java.nio.DirectByteBuffer: " +
        "InvocationTargetException: " + ex.getTargetException());
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.getMessage());
    }
  }
}

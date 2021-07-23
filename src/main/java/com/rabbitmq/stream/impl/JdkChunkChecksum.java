// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.ChunkChecksumValidationException;
import com.rabbitmq.stream.StreamException;
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdkChunkChecksum implements ChunkChecksum {

  static final ChunkChecksum CRC32_SINGLETON;
  private static final Logger LOGGER = LoggerFactory.getLogger(JdkChunkChecksum.class);
  private static final Supplier<Checksum> CRC32_SUPPLIER = CRC32::new;

  static {
    if (isChecksumUpdateByteBufferAvailable()) {
      LOGGER.debug("Checksum#update(ByteBuffer) method available, using it for direct buffers");
      CRC32_SINGLETON = new ByteBufferDirectByteBufChecksum(CRC32_SUPPLIER);
    } else {
      LOGGER.debug(
          "Checksum#update(ByteBuffer) method not available, using byte-by-byte CRC calculation for direct buffers");
      CRC32_SINGLETON = new JdkChunkChecksum(CRC32_SUPPLIER);
    }
  }

  private final Supplier<Checksum> checksumSupplier;

  JdkChunkChecksum() {
    this(CRC32_SUPPLIER);
  }

  JdkChunkChecksum(Supplier<Checksum> checksumSupplier) {
    this.checksumSupplier = checksumSupplier;
  }

  private static boolean isChecksumUpdateByteBufferAvailable() {
    try {
      Checksum.class.getDeclaredMethod("update", ByteBuffer.class);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void checksum(ByteBuf byteBuf, long dataLength, long expected) {
    Checksum checksum = checksumSupplier.get();
    if (byteBuf.hasArray()) {
      checksum.update(
          byteBuf.array(), byteBuf.arrayOffset() + byteBuf.readerIndex(), byteBuf.readableBytes());
    } else {
      byteBuf.forEachByte(
          byteBuf.readerIndex(), byteBuf.readableBytes(), new UpdateProcessor(checksum));
    }
    if (expected != checksum.getValue()) {
      throw new ChunkChecksumValidationException(expected, checksum.getValue());
    }
  }

  private static class ByteBufferDirectByteBufChecksum implements ChunkChecksum {

    private final Supplier<Checksum> checksumSupplier;
    private final Method updateMethod;

    private ByteBufferDirectByteBufChecksum(Supplier<Checksum> checksumSupplier) {
      this.checksumSupplier = checksumSupplier;
      try {
        this.updateMethod = Checksum.class.getDeclaredMethod("update", ByteBuffer.class);
      } catch (NoSuchMethodException e) {
        throw new StreamException("Error while looking up Checksum#update(ByteBuffer) method", e);
      }
    }

    @Override
    public void checksum(ByteBuf byteBuf, long dataLength, long expected) {
      Checksum checksum = checksumSupplier.get();
      if (byteBuf.hasArray()) {
        checksum.update(
            byteBuf.array(),
            byteBuf.arrayOffset() + byteBuf.readerIndex(),
            byteBuf.readableBytes());
      } else {
        try {
          this.updateMethod.invoke(
              checksum, byteBuf.nioBuffer(byteBuf.readerIndex(), byteBuf.readableBytes()));
        } catch (IllegalAccessException e) {
          throw new StreamException("Error while calculating CRC", e);
        } catch (InvocationTargetException e) {
          throw new StreamException("Error while calculating CRC", e);
        }
      }
      if (expected != checksum.getValue()) {
        throw new ChunkChecksumValidationException(expected, checksum.getValue());
      }
    }
  }

  private static class UpdateProcessor implements ByteProcessor {

    private final Checksum checksum;

    private UpdateProcessor(Checksum checksum) {
      this.checksum = checksum;
    }

    @Override
    public boolean process(byte value) {
      checksum.update(value);
      return true;
    }
  }
}

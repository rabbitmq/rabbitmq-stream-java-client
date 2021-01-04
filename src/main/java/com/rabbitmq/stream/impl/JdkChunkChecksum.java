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
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class JdkChunkChecksum implements ChunkChecksum {

  private static final Supplier<Checksum> CRC32_SUPPLIER = CRC32::new;
  static final ChunkChecksum CRC32_SINGLETON = new JdkChunkChecksum(CRC32_SUPPLIER);
  private final Supplier<Checksum> checksumSupplier;

  public JdkChunkChecksum() {
    this(CRC32_SUPPLIER);
  }

  public JdkChunkChecksum(Supplier<Checksum> checksumSupplier) {
    this.checksumSupplier = checksumSupplier;
  }

  @Override
  public void checksum(ByteBuf byteBuf, long dataLength, long expected) {
    Checksum checksum = checksumSupplier.get();
    if (byteBuf.hasArray()) {
      checksum.update(byteBuf.array(), byteBuf.readerIndex(), byteBuf.readableBytes());
    } else {
      byteBuf.forEachByte(
          byteBuf.readerIndex(), byteBuf.readableBytes(), new UpdateProcessor(checksum));
    }
    if (expected != checksum.getValue()) {
      throw new ChunkChecksumValidationException(expected, checksum.getValue());
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

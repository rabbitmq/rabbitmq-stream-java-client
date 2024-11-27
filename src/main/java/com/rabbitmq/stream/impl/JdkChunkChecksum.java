// Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

class JdkChunkChecksum implements ChunkChecksum {

  private static final Supplier<Checksum> CRC32_SUPPLIER = CRC32::new;
  static final ChunkChecksum CRC32_SINGLETON = new JdkChunkChecksum(CRC32_SUPPLIER);

  private final Supplier<Checksum> checksumSupplier;

  JdkChunkChecksum(Supplier<Checksum> checksumSupplier) {
    this.checksumSupplier = checksumSupplier;
  }

  @Override
  public void checksum(ByteBuf byteBuf, long dataLength, long expected) {
    Checksum checksum = checksumSupplier.get();
    if (byteBuf.hasArray()) {
      checksum.update(
          byteBuf.array(), byteBuf.arrayOffset() + byteBuf.readerIndex(), byteBuf.readableBytes());
    } else {
      checksum.update(byteBuf.nioBuffer(byteBuf.readerIndex(), byteBuf.readableBytes()));
    }
    if (expected != checksum.getValue()) {
      throw new ChunkChecksumValidationException(expected, checksum.getValue());
    }
  }
}

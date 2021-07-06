// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.compression;

import io.netty.buffer.ByteBuf;
import java.io.InputStream;
import java.io.OutputStream;

/** Codec to compress and decompress sub-entries. */
public interface CompressionCodec {

  /**
   * Provides the maximum compressed size from the source length.
   *
   * @param sourceLength size of plain, uncompressed data
   * @return maximum compressed size
   */
  int maxCompressedLength(int sourceLength);

  /**
   * Creates an {@link OutputStream} to compress data.
   *
   * @param target {@link ByteBuf} to write compressed data to
   * @return output stream to write plain data to
   */
  OutputStream compress(ByteBuf target);

  /**
   * Creates an {@link InputStream} to read decompressed data from.
   *
   * @param source the {@link ByteBuf} to read compressed from
   * @return input stream to read decompressed from
   */
  InputStream decompress(ByteBuf source);

  /**
   * Return the code for this type of codec.
   *
   * @return compression code
   */
  byte code();
}

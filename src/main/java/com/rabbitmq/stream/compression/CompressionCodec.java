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

public interface CompressionCodec {

  int maxCompressedLength(int sourceLength);

  OutputStream compress(int uncompressedLength, ByteBuf byteBuf);

  InputStream decompress(ByteBuf byteBuf);

  int uncompressedLength(int compressedLength, ByteBuf byteBuf);

  byte code();
}

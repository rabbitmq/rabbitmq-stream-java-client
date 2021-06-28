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

import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressGzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressLz4CompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressSnappyCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressZstdCompressionCodec;

public class CommonsCompressCompressionCodecFactory implements CompressionCodecFactory {
  private final CompressionCodec[] codecs = new CompressionCodec[5];

  public CommonsCompressCompressionCodecFactory() {
    codecs[1] = new CommonsCompressGzipCompressionCodec();
    codecs[2] = new CommonsCompressSnappyCompressionCodec();
    codecs[3] = new CommonsCompressLz4CompressionCodec();
    codecs[4] = new CommonsCompressZstdCompressionCodec();
  }

  @Override
  public CompressionCodec get(Compression compression) {
    return codecs[compression.code];
  }
}

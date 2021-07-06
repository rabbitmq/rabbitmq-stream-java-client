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

import com.rabbitmq.stream.compression.CompressionUtils.GzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.Lz4JavaCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.XerialSnappyCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.ZstdJniCompressionCodec;

/**
 * {@link CompressionCodecFactory} implementation using various compression libraries.
 *
 * <p>The GZIP codec is based on the JDK implementation, the SNAPPY codec uses <a
 * href="https://github.com/xerial/snappy-java">Xerial Snappy</a> (framed), the LZ4 codec uses <a
 * href="https://github.com/lz4/lz4-java">LZ4 Java</a> (framed), the ZSTD codec uses <a
 * href="https://github.com/luben/zstd-jni/">zstd-jni</a>.
 */
public class DefaultCompressionCodecFactory implements CompressionCodecFactory {

  private final CompressionCodec[] codecs = new CompressionCodec[5];

  public DefaultCompressionCodecFactory() {
    codecs[1] = new GzipCompressionCodec();
    codecs[2] = new XerialSnappyCompressionCodec();
    codecs[3] = new Lz4JavaCompressionCodec();
    codecs[4] = new ZstdJniCompressionCodec();
  }

  @Override
  public CompressionCodec get(Compression compression) {
    return codecs[compression.code];
  }
}

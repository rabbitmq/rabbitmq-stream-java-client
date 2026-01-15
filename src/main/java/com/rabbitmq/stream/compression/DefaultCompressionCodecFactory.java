// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.compression;

import com.rabbitmq.stream.compression.CompressionUtils.GzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.Lz4JavaCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.XerialSnappyCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.ZstdJniCompressionCodec;
import java.util.function.Supplier;

/**
 * {@link CompressionCodecFactory} implementation using various compression libraries.
 *
 * <p>The GZIP codec is based on the JDK implementation, the SNAPPY codec uses <a
 * href="https://github.com/xerial/snappy-java">Xerial Snappy</a> (framed), the LZ4 codec uses <a
 * href="https://github.com/lz4/lz4-java">LZ4 Java</a> (framed), the ZSTD codec uses <a
 * href="https://github.com/luben/zstd-jni/">zstd-jni</a>.
 *
 * This factory gracefully handles missing native libraries. If a native compression library is
 * not available (e.g., on Android or when dependencies are excluded), the corresponding codec will
 * be unavailable and requesting it will throw a {@link CompressionException}.
 *
 * gzip is always available since it is a part of the JDK.
 *
 * @see PortableCompressionCodecFactory
 */
public class DefaultCompressionCodecFactory implements CompressionCodecFactory {

  private final CompressionCodec[] codecs = new CompressionCodec[5];
  private final String[] loadErrors = new String[5];

  public DefaultCompressionCodecFactory() {
    codecs[Compression.GZIP.code()] = new GzipCompressionCodec();
    codecs[Compression.SNAPPY.code()] = tryLoad("Snappy", XerialSnappyCompressionCodec::new);
    codecs[Compression.LZ4.code()] = tryLoad("LZ4", Lz4JavaCompressionCodec::new);
    codecs[Compression.ZSTD.code()] = tryLoad("Zstd", ZstdJniCompressionCodec::new);
  }

  private CompressionCodec tryLoad(String name, Supplier<CompressionCodec> supplier) {
    try {
      return supplier.get();
    } catch (NoClassDefFoundError | UnsatisfiedLinkError | ExceptionInInitializerError e) {
      loadErrors[Compression.valueOf(name.toUpperCase()).code()] =
          name + " codec unavailable: " + e.getClass().getSimpleName() + " - " + e.getMessage();
      return null;
    }
  }

  @Override
  public CompressionCodec get(Compression compression) {
    if (compression == Compression.NONE) {
      return null;
    }
    CompressionCodec codec = codecs[compression.code()];
    if (codec == null) {
      String error = loadErrors[compression.code()];
      if (error != null) {
        throw new CompressionException(
            error + ". Consider using PortableCompressionCodecFactory for gzip-only support.");
      }
      throw new CompressionException("No codec available for " + compression);
    }
    return codec;
  }
}

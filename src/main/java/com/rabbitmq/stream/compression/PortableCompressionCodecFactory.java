// Copyright (c) 2024-2026 Broadcom. All Rights Reserved.
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

/**
 * A {@link CompressionCodecFactory} implementation that only supports gzip and intentionally avoids
 * native dependencies for maximum compatibility (e.g. with Android). Party like it's 1992.
 *
 * Use this factory when native compression libraries (zstd-jni, snappy-java, lz4-java) are not
 * available or are to be avoided for any reason.
 *
 * @since 1.5.0
 * @see DefaultCompressionCodecFactory
 */
public class PortableCompressionCodecFactory implements CompressionCodecFactory {

  private final CompressionCodec gzipCodec = new CompressionUtils.GzipCompressionCodec();

  @Override
  public CompressionCodec get(Compression compression) {
    if (compression == Compression.GZIP) {
      return gzipCodec;
    }
    if (compression == Compression.NONE) {
      return null;
    }
    throw new CompressionException(
        "PortableCompressionCodecFactory only supports gzip compression. Use DefaultCompressionCodecFactory for other algorithms."
    );
  }
}

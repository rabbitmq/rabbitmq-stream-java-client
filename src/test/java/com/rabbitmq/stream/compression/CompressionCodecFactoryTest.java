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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class CompressionCodecFactoryTest {

  @Test
  void portableFactorySupportsGzip() {
    PortableCompressionCodecFactory factory = new PortableCompressionCodecFactory();
    CompressionCodec codec = factory.get(Compression.GZIP);
    assertThat(codec).isNotNull();
    assertThat(codec.code()).isEqualTo(Compression.GZIP.code());
  }

  @Test
  void portableFactoryReturnsNullForNone() {
    PortableCompressionCodecFactory factory = new PortableCompressionCodecFactory();
    assertThat(factory.get(Compression.NONE)).isNull();
  }

  @Test
  void portableFactoryThrowsForUnsupportedCodecs() {
    PortableCompressionCodecFactory factory = new PortableCompressionCodecFactory();

    assertThatThrownBy(() -> factory.get(Compression.SNAPPY))
        .isInstanceOf(CompressionException.class)
        .hasMessageContaining("only supports gzip");

    assertThatThrownBy(() -> factory.get(Compression.LZ4))
        .isInstanceOf(CompressionException.class)
        .hasMessageContaining("only supports gzip");

    assertThatThrownBy(() -> factory.get(Compression.ZSTD))
        .isInstanceOf(CompressionException.class)
        .hasMessageContaining("only supports gzip");
  }

  @Test
  void defaultFactorySupportsAllCodecs() {
    DefaultCompressionCodecFactory factory = new DefaultCompressionCodecFactory();

    // GZIP is always available (JDK-based)
    assertThat(factory.get(Compression.GZIP)).isNotNull();

    // Native codecs should be available in test environment
    assertThat(factory.get(Compression.SNAPPY)).isNotNull();
    assertThat(factory.get(Compression.LZ4)).isNotNull();
    assertThat(factory.get(Compression.ZSTD)).isNotNull();

    // NONE returns null
    assertThat(factory.get(Compression.NONE)).isNull();
  }
}

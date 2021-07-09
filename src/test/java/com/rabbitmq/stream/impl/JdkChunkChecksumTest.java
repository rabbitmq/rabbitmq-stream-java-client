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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.ChunkChecksumValidationException;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class JdkChunkChecksumTest {

  static Charset UTF8 = StandardCharsets.UTF_8;
  static Map<String, Supplier<Checksum>> CHECKSUMS =
      new HashMap<String, Supplier<Checksum>>() {
        {
          put("crc32", () -> new CRC32());
          put("adler32", () -> new Adler32());
        }
      };

  static long crc32(String algorithm, byte[] content) {
    Checksum checksum = CHECKSUMS.get(algorithm).get();
    checksum.update(content, 0, content.length);
    return checksum.getValue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"crc32", "adler32"})
  void checksumsOk(String algorithm) {
    ChunkChecksum chunkChecksum = new JdkChunkChecksum(CHECKSUMS.get(algorithm));
    UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    Stream.of(allocator.directBuffer(), allocator.heapBuffer())
        .forEach(
            bb -> {
              byte[] content = "hello".getBytes(UTF8);
              bb.writeBytes(content);
              chunkChecksum.checksum(bb, content.length, crc32(algorithm, content));
              bb.release();
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"crc32", "adler32"})
  void checksumsKo(String algorithm) {
    ChunkChecksum chunkChecksum = new JdkChunkChecksum(CHECKSUMS.get(algorithm));
    UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    Stream.of(allocator.directBuffer(), allocator.heapBuffer())
        .forEach(
            bb -> {
              byte[] content = "hello".getBytes(UTF8);
              bb.writeBytes(content);
              assertThatThrownBy(
                      () ->
                          chunkChecksum.checksum(bb, content.length, crc32(algorithm, content) + 1))
                  .isInstanceOf(ChunkChecksumValidationException.class);
              bb.release();
            });
  }
}

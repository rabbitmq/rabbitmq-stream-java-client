// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.perf;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

public class StreamPerfTestTest {

  @Test
  void writeReadLongInByteArray() {
    byte[] array = new byte[8];
    LongStream.of(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            1,
            128,
            256,
            33_000,
            66_000,
            1_000_000,
            new Random().nextLong())
        .forEach(
            value -> {
              StreamPerfTest.writeLong(array, value);
              assertThat(StreamPerfTest.readLong(array)).isEqualTo(value);
            });
  }
}

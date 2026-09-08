// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Client.MAX_STREAM_INITIAL_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamParametersBuilderTest {

  private Client.StreamParametersBuilder builder;

  @BeforeEach
  void setUp() {
    builder = new Client.StreamParametersBuilder();
  }

  @Test
  void initialOffsetShouldStoreZeroAsString() {
    Map<String, String> parameters = builder.initialOffset(0L).build();

    assertThat(parameters).containsEntry("stream-initial-offset", "0");
  }

  @ParameterizedTest
  @ValueSource(longs = {1L, 100L, 1_000_000L, MAX_STREAM_INITIAL_OFFSET})
  void initialOffsetShouldStoreValidPositiveOffsets(long offset) {
    Map<String, String> parameters = builder.initialOffset(offset).build();

    assertThat(parameters).containsEntry("stream-initial-offset", Long.toUnsignedString(offset));
  }

  @Test
  void initialOffsetShouldAcceptBoundaryValuesWithoutException() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              builder.initialOffset(0L);
              builder.initialOffset(MAX_STREAM_INITIAL_OFFSET);
            });
  }

  @Test
  void initialOffsetShouldThrowExceptionWhenExceedingMaxOffset() {
    long invalidOffset = MAX_STREAM_INITIAL_OFFSET + 1; // 1L << 62

    assertThatThrownBy(() -> builder.initialOffset(invalidOffset))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Initial offset must be between 0 and " + MAX_STREAM_INITIAL_OFFSET)
        .hasMessageContaining("given: 4611686018427387904");
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, -100L, Long.MIN_VALUE})
  void initialOffsetShouldThrowExceptionForNegativeValues(long negativeOffset) {
    assertThatThrownBy(() -> builder.initialOffset(negativeOffset))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Initial offset must be between 0 and " + MAX_STREAM_INITIAL_OFFSET)
        .hasMessageContaining("given: " + Long.toUnsignedString(negativeOffset));
  }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.ConsumerFlowStrategy.CreditUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ConsumerFlowStrategyTest {

  @ParameterizedTest
  @MethodSource("byteCapacityFactories")
  void byteCapacityFactoriesReturnByteUnitAndWindowAsInitialCredits(
      Function<ByteCapacity, ConsumerFlowStrategy> factory) {
    ConsumerFlowStrategy strategy = factory.apply(ByteCapacity.MB(2));
    assertThat(strategy.unit()).isEqualTo(CreditUnit.BYTE);
    assertThat(strategy.initialCredits()).isEqualTo(2_000_000);
  }

  @ParameterizedTest
  @MethodSource("byteCapacityFactories")
  void byteCapacityFactoriesAcceptWindowOfIntegerMaxValueBytes(
      Function<ByteCapacity, ConsumerFlowStrategy> factory) {
    ConsumerFlowStrategy strategy = factory.apply(ByteCapacity.B(Integer.MAX_VALUE));
    assertThat(strategy.initialCredits()).isEqualTo(Integer.MAX_VALUE);
  }

  @ParameterizedTest
  @MethodSource("byteCapacityFactories")
  void byteCapacityFactoriesRejectNonPositiveWindow(
      Function<ByteCapacity, ConsumerFlowStrategy> factory) {
    assertThatThrownBy(() -> factory.apply(ByteCapacity.B(0)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> factory.apply(ByteCapacity.B(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("byteCapacityFactories")
  void byteCapacityFactoriesRejectWindowLargerThanIntegerMaxValue(
      Function<ByteCapacity, ConsumerFlowStrategy> factory) {
    assertThatThrownBy(() -> factory.apply(ByteCapacity.B(Integer.MAX_VALUE + 1L)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("chunkBasedFactories")
  void chunkBasedFactoriesUseChunkUnit(ConsumerFlowStrategy strategy) {
    assertThat(strategy.unit()).isEqualTo(CreditUnit.CHUNK);
  }

  static Stream<Function<ByteCapacity, ConsumerFlowStrategy>> byteCapacityFactories() {
    return Stream.of(
        ConsumerFlowStrategy::creditOnChunkArrival,
        ConsumerFlowStrategy::creditWhenHalfMessagesProcessed,
        window -> ConsumerFlowStrategy.creditOnProcessedMessageCount(window, 0.5));
  }

  static Stream<ConsumerFlowStrategy> chunkBasedFactories() {
    return Stream.of(
        ConsumerFlowStrategy.creditOnChunkArrival(),
        ConsumerFlowStrategy.creditOnChunkArrival(5),
        ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(),
        ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(5),
        ConsumerFlowStrategy.creditOnProcessedMessageCount(5, 0.5),
        ConsumerFlowStrategy.creditEveryNthChunk(10, 5));
  }
}

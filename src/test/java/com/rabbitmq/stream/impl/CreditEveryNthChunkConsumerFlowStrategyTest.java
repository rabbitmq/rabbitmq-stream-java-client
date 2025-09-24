// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.ConsumerFlowStrategy.creditEveryNthChunk;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.ConsumerFlowStrategy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class CreditEveryNthChunkConsumerFlowStrategyTest {

  AtomicInteger requestedCredits = new AtomicInteger();

  @Test
  void invalidArguments() {
    assertThatThrownBy(() -> build(1, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> build(1, 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> build(10, 0)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @CsvSource({"10,5", "5,2", "2,1"})
  void test(int initialCredits, int limit) {
    ConsumerFlowStrategy strategy = build(initialCredits, limit);

    range(0, limit - 1)
        .forEach(
            ignored -> {
              strategy.start(context());
              assertThat(requestedCredits).hasValue(0);
            });
    strategy.start(context());
    assertThat(requestedCredits).hasValue(limit);
  }

  ConsumerFlowStrategy build(int initial, int limit) {
    return creditEveryNthChunk(initial, limit);
  }

  ConsumerFlowStrategy.Context context() {
    requestedCredits.set(0);
    return new ConsumerFlowStrategy.Context() {
      @Override
      public void credits(int credits) {
        requestedCredits.addAndGet(credits);
      }

      @Override
      public long messageCount() {
        throw new UnsupportedOperationException();
      }
    };
  }
}

// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.ConsumerFlowStrategy.creditOnProcessedMessageCount;
import static java.util.stream.LongStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.ConsumerFlowStrategy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

public class MessageCountConsumerFlowStrategyTest {

  AtomicInteger requestedCredits = new AtomicInteger();

  @Test
  void shouldCreditOnceLimitIsReached() {
    ConsumerFlowStrategy strategy = build(0.5);
    long messageCount = 1000;
    LongConsumer processedCallback = strategy.start(context(messageCount));
    range(0, messageCount / 2 - 1).forEach(ignored -> processedCallback.accept(42));
    assertThat(requestedCredits).hasValue(0);
    processedCallback.accept(42);
    assertThat(requestedCredits).hasValue(1);
    processedCallback.accept(42);
    assertThat(requestedCredits).hasValue(1);
    range(0, messageCount).forEach(ignored -> processedCallback.accept(42));
    assertThat(requestedCredits).hasValue(1);
  }

  @Test
  void smallChunksAndSmallRatiosShouldCredit() {
    ConsumerFlowStrategy strategy = build(0.5);
    LongConsumer processedCallback = strategy.start(context(1));
    processedCallback.accept(42);
    assertThat(requestedCredits).hasValue(1);

    strategy = build(0.05);
    processedCallback = strategy.start(context(15));
    processedCallback.accept(42);
    assertThat(requestedCredits).hasValue(1);
  }

  ConsumerFlowStrategy build(double ratio) {
    return creditOnProcessedMessageCount(1, ratio);
  }

  ConsumerFlowStrategy.Context context(long messageCount) {
    requestedCredits.set(0);
    return new ConsumerFlowStrategy.Context() {
      @Override
      public void credits(int credits) {
        requestedCredits.addAndGet(credits);
      }

      @Override
      public long messageCount() {
        return messageCount;
      }
    };
  }
}

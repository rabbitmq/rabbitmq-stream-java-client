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
package com.rabbitmq.stream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

public interface ConsumerFlowStrategy {

  int initialCredits();

  LongConsumer start(Context context);

  interface Context {

    void credits(int credits);

    long messageCount();
  }

  class DefaultConsumerFlowStrategy implements ConsumerFlowStrategy {

    private final int initialCredits;

    public DefaultConsumerFlowStrategy(int initialCredits) {
      this.initialCredits = initialCredits;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public LongConsumer start(Context context) {
      context.credits(1);
      return value -> {};
    }
  }

  class MessageCountConsumerFlowStrategy implements ConsumerFlowStrategy {

    private final int initialCredits;

    public MessageCountConsumerFlowStrategy(int initialCredits) {
      this.initialCredits = initialCredits;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public LongConsumer start(Context context) {
      AtomicLong processedMessages = new AtomicLong(0);
      long limit = context.messageCount() == 1 ? 1 : context.messageCount() / 2;
      return messageOffset -> {
        if (processedMessages.incrementAndGet() == limit) {
          context.credits(1);
        }
      };
    }
  }
}

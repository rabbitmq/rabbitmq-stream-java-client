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

public interface ConsumerFlowStrategy {

  int initialCredits();

  MessageProcessedCallback start(Context context);

  interface Context {

    void credits(int credits);

    long messageCount();
  }

  @FunctionalInterface
  interface MessageProcessedCallback {

    void processed(MessageHandler.Context messageContext);
  }

  static ConsumerFlowStrategy creditOnChunkArrival() {
    return creditOnChunkArrival(1);
  }

  static ConsumerFlowStrategy creditOnChunkArrival(int initialCredits) {
    return new CreditOnChunkArrivalConsumerFlowStrategy(initialCredits);
  }

  static ConsumerFlowStrategy creditWhenHalfMessagesProcessed() {
    return creditOnProcessedMessageCount(1, 0.5);
  }

  static ConsumerFlowStrategy creditWhenHalfMessagesProcessed(int initialCredits) {
    return creditOnProcessedMessageCount(initialCredits, 0.5);
  }

  static ConsumerFlowStrategy creditOnProcessedMessageCount(int initialCredits, double ratio) {
    return new MessageCountConsumerFlowStrategy(initialCredits, ratio);
  }

  class CreditOnChunkArrivalConsumerFlowStrategy implements ConsumerFlowStrategy {

    private final int initialCredits;

    private CreditOnChunkArrivalConsumerFlowStrategy(int initialCredits) {
      this.initialCredits = initialCredits;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public MessageProcessedCallback start(Context context) {
      context.credits(1);
      return value -> {};
    }
  }

  class MessageCountConsumerFlowStrategy implements ConsumerFlowStrategy {

    private final int initialCredits;
    private final double ratio;

    private MessageCountConsumerFlowStrategy(int initialCredits, double ratio) {
      this.initialCredits = initialCredits;
      this.ratio = ratio;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public MessageProcessedCallback start(Context context) {
      long l = (long) (context.messageCount() * ratio);
      long limit = Math.max(1, l);
      AtomicLong processedMessages = new AtomicLong(0);
      return messageOffset -> {
        if (processedMessages.incrementAndGet() == limit) {
          context.credits(1);
        }
      };
    }
  }
}

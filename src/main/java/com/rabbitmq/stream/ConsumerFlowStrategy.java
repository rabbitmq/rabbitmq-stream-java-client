// Copyright (c) 2023-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Contract to determine when a subscription provides credits to get more messages.
 *
 * <p>The broker delivers "chunks" of messages to consumers. A chunk can contain from 1 to several
 * thousands of messages. The broker send chunks as long as the subscription has <em>credits</em>. A
 * client connection can provide credits for a given subscription and the broker will send the
 * corresponding number of chunks (1 credit = 1 chunk).
 *
 * <p>This credit mechanism avoids overwhelming a consumer with messages. A consumer does not want
 * to provide a credit only when it is done with messages of a chunk, because it will be idle
 * between its credit request and the arrival of the next chunk. The idea is to keep consumers busy
 * as much as possible, without accumulating an in-memory backlog on the client side. There is no
 * ideal solution, it depends on the use cases and several parameters (processing time, network,
 * etc).
 *
 * <p>This is an experimental API, subject to change.
 *
 * @since 0.12.0
 * @see MessageHandler.Context#processed()
 * @see ConsumerBuilder#flow()
 */
public interface ConsumerFlowStrategy {

  /**
   * The initial number of credits for a subscription.
   *
   * <p>It must be greater than 0. Values are usually between 1 and 10.
   *
   * @return initial number of credits
   */
  int initialCredits();

  /**
   * Return the behavior for {@link MessageHandler.Context#processed()} calls.
   *
   * <p>This method is called for each chunk of messages. Implementations return a callback that
   * will be called when applications consider a message dealt with and call {@link
   * MessageHandler.Context#processed()}. The callback can count messages and provide credits
   * accordingly.
   *
   * @param context chunk context
   * @return the message processed callback
   */
  MessageProcessedCallback start(Context context);

  /** Chunk context. */
  interface Context {

    /**
     * Provide credits for the subscription.
     *
     * <p>{@link ConsumerFlowStrategy} implementation should always provide 1 credit a given chunk.
     *
     * @param credits the number of credits provided, usually 1
     */
    void credits(int credits);

    /**
     * The number of messages in the chunk.
     *
     * @return number of messages in the chunk
     */
    long messageCount();
  }

  /** Behavior for {@link MessageHandler.Context#processed()} calls. */
  @FunctionalInterface
  interface MessageProcessedCallback {

    /**
     * Method called when {@link MessageHandler.Context#processed()} is called.
     *
     * <p>There is one instance of this class for a given chunk and it is called for the <code>
     * processed()</code> calls of the message of this chunk.
     *
     * <p>Implementations can count messages and call {@link Context#credits(int)} when appropriate.
     *
     * <p>Note calls to {@link MessageHandler.Context#processed()} are not idempotent: an
     * application can call the method several times for the same message and implementations must
     * deal with these multiple calls if they impact their logic.
     *
     * @param messageContext context of the message
     */
    void processed(MessageHandler.Context messageContext);
  }

  /**
   * Strategy that provides 1 initial credit and a credit on each new chunk.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   *
   * @return flow strategy
   */
  static ConsumerFlowStrategy creditOnChunkArrival() {
    return creditOnChunkArrival(1);
  }

  /**
   * Strategy that provides the specified number of initial credits and a credit on each new chunk.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   *
   * @param initialCredits number of initial credits
   * @return flow strategy
   * @see com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(int)
   */
  static ConsumerFlowStrategy creditOnChunkArrival(int initialCredits) {
    return new CreditOnChunkArrivalConsumerFlowStrategy(initialCredits);
  }

  /**
   * Strategy that provides 10 initial credits and a credit when half of the chunk messages are
   * processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   *
   * @return flow strategy
   */
  static ConsumerFlowStrategy creditWhenHalfMessagesProcessed() {
    return creditOnProcessedMessageCount(10, 0.5);
  }

  /**
   * Strategy that provides the specified number of initial credits and a credit when half of the
   * chunk messages are processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   *
   * @param initialCredits number of initial credits
   * @return flow strategy
   * @see com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(int)
   */
  static ConsumerFlowStrategy creditWhenHalfMessagesProcessed(int initialCredits) {
    return creditOnProcessedMessageCount(initialCredits, 0.5);
  }

  /**
   * Strategy that provides the specified number of initial credits and a credit when the specified
   * ratio of the chunk messages are processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   *
   * @param initialCredits number of initial credits
   * @return flow strategy
   */
  static ConsumerFlowStrategy creditOnProcessedMessageCount(int initialCredits, double ratio) {
    return new MessageCountConsumerFlowStrategy(initialCredits, ratio);
  }

  /**
   * Strategy that provides the specified number of initial credits and <code>n</code> credits every
   * <code>n</code> chunks.
   *
   * <p>This strategy can improve throughput for streams with small chunks (less than 30 messages
   * per chunk).
   *
   * <p>The number of initial credits must be at least twice as big as <code>n</code>.
   *
   * <p>A rule of thumb is to set <code>n</code> to a third of the value of initial credits.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   *
   * @param initialCredits number of initial credits
   * @param n number of chunks and number of credits
   * @return flow strategy
   */
  static ConsumerFlowStrategy creditEveryNthChunk(int initialCredits, int n) {
    return new CreditEveryNthChunkConsumerFlowStrategy(initialCredits, n);
  }

  /**
   * Strategy that provides the specified number of initial credits and <code>n</code> credits every
   * <code>n</code> chunks.
   *
   * <p>This strategy can improve throughput for streams with small chunks (less than 30 messages
   * per chunk).
   *
   * <p>The number of initial credits must be at least twice as big as <code>n</code>.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   */
  final class CreditEveryNthChunkConsumerFlowStrategy implements ConsumerFlowStrategy {

    private static final MessageProcessedCallback CALLBACK = v -> {};

    private final int initialCredits;
    private final AtomicLong chunkCount = new AtomicLong(0);
    private final int n;

    private CreditEveryNthChunkConsumerFlowStrategy(int initialCredits, int n) {
      if (n <= 0) {
        throw new IllegalArgumentException("The n argument must be greater than 0");
      }
      if (n * 2 > initialCredits) {
        throw new IllegalArgumentException(
            "The number of initial credits must be at least twice as big as n");
      }
      this.initialCredits = initialCredits;
      this.n = n;
    }

    @Override
    public int initialCredits() {
      this.chunkCount.set(0);
      return this.initialCredits;
    }

    @Override
    public MessageProcessedCallback start(Context context) {
      if (chunkCount.incrementAndGet() % n == 0) {
        context.credits(n);
      }
      return CALLBACK;
    }
  }

  /**
   * Strategy that provides the specified number of initial credits and a credit on each new chunk.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   */
  final class CreditOnChunkArrivalConsumerFlowStrategy implements ConsumerFlowStrategy {

    private static final MessageProcessedCallback CALLBACK = v -> {};

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
      return CALLBACK;
    }
  }

  /**
   * Strategy that provides the specified number of initial credits and a credit when the specified
   * ratio of the chunk messages are processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   */
  final class MessageCountConsumerFlowStrategy implements ConsumerFlowStrategy {

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

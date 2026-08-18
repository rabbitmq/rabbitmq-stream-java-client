// Copyright (c) 2023-2026 Broadcom. All Rights Reserved.
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
 * <p>Custom implementations must never make a credit release depend on the arrival of a
 * <em>later</em> chunk. Releasing when a chunk arrives, or when its own messages are processed, is
 * safe: the trigger has already happened by the time the credit is due. Waiting for a chunk that
 * has not arrived yet is not safe, because the broker may be blocked precisely because that chunk
 * was never sent.
 *
 * <p>Credit is usually expressed in chunks, but a strategy can express it in bytes instead, see
 * {@link CreditUnit}. Byte-based credit still follows a chunk granularity: a chunk is delivered in
 * full even if it exceeds the outstanding byte credit, so a consumer never stalls just because its
 * window is smaller than the next chunk.
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

  /** The unit a subscription's credit is expressed in. */
  enum CreditUnit {
    /** 1 credit lets the broker send 1 more chunk, whatever its size. */
    CHUNK,
    /**
     * Credit is a number of bytes; the client must eventually grant back every byte the broker
     * charged for a chunk. Requires broker support, see {@link
     * com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(ByteCapacity)}.
     */
    BYTE
  }

  /**
   * The unit this strategy's credit is expressed in.
   *
   * <p>Defaults to {@link CreditUnit#CHUNK}.
   *
   * @return the credit unit
   */
  default CreditUnit unit() {
    return CreditUnit.CHUNK;
  }

  /** Chunk context. */
  interface Context {

    /**
     * Provide credits for the subscription.
     *
     * <p>{@link ConsumerFlowStrategy} implementation should always provide 1 credit for a given
     * chunk.
     *
     * <p><code>credits</code> counts chunks in both units. For a byte-based subscription, the
     * client grants the bytes of the corresponding chunks as credit, not the raw <code>credits
     * </code> value.
     *
     * <p>Implementations must never call this method for a chunk based on the arrival of a
     * <em>later</em> chunk, only on the arrival of the chunk itself or the processing of its own
     * messages, see {@link ConsumerFlowStrategy}.
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

    /**
     * The offset of the first message in the chunk, aka chunk ID.
     *
     * @return offset of the first message in the chunk (chunk ID)
     */
    long chunkId();

    /**
     * The cost the broker charged for the chunk, in bytes.
     *
     * <p>This is what a byte-based subscription must eventually grant back as credit.
     *
     * @return the chunk cost, in bytes
     */
    long chunkByteCount();
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
   * Strategy that provides a byte window as initial credits and a credit on each new chunk.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   *
   * <p>Requires a broker supporting {@code Subscribe} version 2.
   *
   * @param window initial credit window, in bytes
   * @return flow strategy
   * @see com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(ByteCapacity)
   */
  static ConsumerFlowStrategy creditOnChunkArrival(ByteCapacity window) {
    return new CreditOnChunkArrivalConsumerFlowStrategy(
        windowToInitialCredits(window), CreditUnit.BYTE);
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
   * Strategy that provides a byte window as initial credits and a credit when half of the chunk
   * messages are processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   *
   * <p>Requires a broker supporting {@code Subscribe} version 2.
   *
   * @param window initial credit window, in bytes
   * @return flow strategy
   * @see com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(ByteCapacity)
   */
  static ConsumerFlowStrategy creditWhenHalfMessagesProcessed(ByteCapacity window) {
    return creditOnProcessedMessageCount(window, 0.5);
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
   * Strategy that provides a byte window as initial credits and a credit when the specified ratio
   * of the chunk messages are processed.
   *
   * <p>Make sure to call {@link MessageHandler.Context#processed()} on every message when using
   * this strategy, otherwise the broker may stop sending messages to the consumer.
   *
   * <p>Requires a broker supporting {@code Subscribe} version 2.
   *
   * @param window initial credit window, in bytes
   * @param ratio ratio of messages to process before providing credits
   * @return flow strategy
   * @see com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration#initialCredits(ByteCapacity)
   */
  static ConsumerFlowStrategy creditOnProcessedMessageCount(ByteCapacity window, double ratio) {
    return new MessageCountConsumerFlowStrategy(
        windowToInitialCredits(window), ratio, CreditUnit.BYTE);
  }

  private static int windowToInitialCredits(ByteCapacity window) {
    if (window == null || window.compareTo(ByteCapacity.B(0)) <= 0) {
      throw new IllegalArgumentException("The window must be positive");
    }
    if (window.compareTo(ByteCapacity.B(Integer.MAX_VALUE)) > 0) {
      throw new IllegalArgumentException(
          "The window must be at most " + Integer.MAX_VALUE + " bytes");
    }
    return (int) window.toBytes();
  }

  /**
   * Strategy that provides the specified number of initial credits and <code>n</code> credits every
   * <code>n</code> chunks.
   *
   * <p>This strategy can improve throughput for some workloads, it is possible to experiment with
   * it if hitting problems with other flow strategies.
   *
   * <p>The number of initial credits must be at least twice as big as <code>n</code>.
   *
   * <p>A rule of thumb is to set <code>n</code> to a third of the value of initial credits.
   *
   * <p>Calls to {@link MessageHandler.Context#processed()} are ignored.
   *
   * <p>This strategy has no byte-based variant and always uses {@link CreditUnit#CHUNK}: it
   * releases credit once <code>n</code> chunks have arrived, which makes the release of the last
   * <code>n - 1</code> chunks depend on the arrival of a chunk that has not happened yet, breaking
   * the contract described in {@link ConsumerFlowStrategy}. In chunk mode the residual is bounded
   * by <code>n</code> chunks, which the constructor check below keeps under control; in byte mode
   * the residual would be an unbounded number of bytes, since chunk sizes are not known when the
   * consumer is built.
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
   * <p>This strategy can improve throughput for some workloads, it is possible to experiment with
   * it if hitting problems with other flow strategies.
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
    private final CreditUnit unit;

    private CreditOnChunkArrivalConsumerFlowStrategy(int initialCredits) {
      this(initialCredits, CreditUnit.CHUNK);
    }

    CreditOnChunkArrivalConsumerFlowStrategy(int initialCredits, CreditUnit unit) {
      this.initialCredits = initialCredits;
      this.unit = unit;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public CreditUnit unit() {
      return this.unit;
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
    private final CreditUnit unit;

    private MessageCountConsumerFlowStrategy(int initialCredits, double ratio) {
      this(initialCredits, ratio, CreditUnit.CHUNK);
    }

    MessageCountConsumerFlowStrategy(int initialCredits, double ratio, CreditUnit unit) {
      this.initialCredits = initialCredits;
      this.ratio = ratio;
      this.unit = unit;
    }

    @Override
    public int initialCredits() {
      return this.initialCredits;
    }

    @Override
    public CreditUnit unit() {
      return this.unit;
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

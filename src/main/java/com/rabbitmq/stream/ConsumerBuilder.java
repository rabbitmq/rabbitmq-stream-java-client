// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import java.time.Duration;
import java.util.function.Predicate;

/** API to configure and create a {@link Consumer}. */
public interface ConsumerBuilder {

  /**
   * The stream to consume from.
   *
   * @param stream
   * @return this builder instance
   */
  ConsumerBuilder stream(String stream);

  /**
   * Set the consumer to consume from a super stream (partitioned stream).
   *
   * <p>This is meant to be used with {@link #singleActiveConsumer()}.
   *
   * <p>This is an experimental API, subject to change.
   *
   * <p>RabbitMQ 3.11 or more is required.
   *
   * @param superStream
   * @return this builder instance
   * @see #singleActiveConsumer()
   */
  ConsumerBuilder superStream(String superStream);

  /**
   * The offset to start consuming from.
   *
   * <p>The default is {@link OffsetSpecification#next()} (the end of the stream).
   *
   * @param offsetSpecification
   * @return this builder instance
   */
  ConsumerBuilder offset(OffsetSpecification offsetSpecification);

  /**
   * The callback for inbound messages.
   *
   * @param messageHandler
   * @return this builder instance
   */
  ConsumerBuilder messageHandler(MessageHandler messageHandler);

  /**
   * The logical name of the {@link Consumer}.
   *
   * <p>Set a logical name to enable offset tracking.
   *
   * @param name
   * @return this builder instance
   */
  ConsumerBuilder name(String name);

  /**
   * Declare the consumer as a single active consumer.
   *
   * <p>A single active consumer must set up a name with {@link #name(String)}.
   *
   * <p>Instances of the same application can declare several single active consumer instances with
   * the same name and only one will be active at a time, meaning it will be the only one to get
   * messages from the broker.
   *
   * <p>If the active consumer instance stops or crashes, the broker will choose a new active
   * instance among the remaining ones.
   *
   * <p>This is an experimental API, subject to change.
   *
   * <p>RabbitMQ 3.11 or more is required.
   *
   * @return this builder instance
   * @since 0.6.0
   * @see #name(String)
   */
  ConsumerBuilder singleActiveConsumer();

  /**
   * Set the listener for single active consumer updates.
   *
   * <p>This listener is usually set when manual offset tracking is used, either server-side or with
   * an external datastore.
   *
   * <p>This is an experimental API, subject to change.
   *
   * <p>RabbitMQ 3.11 or more is required.
   *
   * @param consumerUpdateListener
   * @return this builder instance
   * @since 0.6.0
   * @see #singleActiveConsumer()
   * @see ConsumerUpdateListener
   * @see #manualTrackingStrategy()
   */
  ConsumerBuilder consumerUpdateListener(ConsumerUpdateListener consumerUpdateListener);

  /**
   * Callback on subscription.
   *
   * <p>Can be used to set the offset specification before subscribing to the stream.
   *
   * <p>This is an experimental API, subject to change.
   *
   * @see SubscriptionListener
   * @param subscriptionListener the listener
   * @return this builder instance
   * @since 0.5.0
   */
  ConsumerBuilder subscriptionListener(SubscriptionListener subscriptionListener);

  /**
   * Enable {@link ManualTrackingStrategy}.
   *
   * @return the manual tracking strategy
   */
  ManualTrackingStrategy manualTrackingStrategy();

  /**
   * Enable {@link AutoTrackingStrategy}.
   *
   * <p>This is the default tracking strategy.
   *
   * @return the auto-tracking strategy
   */
  AutoTrackingStrategy autoTrackingStrategy();

  /**
   * Disable server-side offset tracking.
   *
   * <p>Useful when {@link #singleActiveConsumer()} is enabled and an external store is used for
   * offset tracking. This avoids automatic server-side offset tracking to kick in.
   *
   * @return this builder instance
   * @since 0.6.0
   */
  ConsumerBuilder noTrackingStrategy();

  /**
   * Configure the filtering.
   *
   * <p>RabbitMQ 3.13 or more is required.
   *
   * @return the filtering configuration
   */
  FilterConfiguration filter();

  /**
   * Configure flow of messages.
   *
   * @return the flow configuration
   * @since 0.11.0
   * @see ConsumerFlowStrategy#creditOnChunkArrival()
   * @see MessageHandler.Context#processed()
   */
  FlowConfiguration flow();

  /**
   * Create the configured {@link Consumer}
   *
   * @return the configured consumer
   */
  Consumer build();

  /** Manual tracking strategy. */
  interface ManualTrackingStrategy {

    /**
     * Interval to check if the last requested stored offset has been actually stored.
     *
     * <p>Default is 5 seconds.
     *
     * @param checkInterval
     * @return the manual tracking strategy
     */
    ManualTrackingStrategy checkInterval(Duration checkInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /** Auto-tracking strategy. */
  interface AutoTrackingStrategy {

    /**
     * Number of messages before storing.
     *
     * <p>Default is 10,000.
     *
     * @param messageCountBeforeStorage
     * @return the auto-tracking strategy
     */
    AutoTrackingStrategy messageCountBeforeStorage(int messageCountBeforeStorage);

    /**
     * Interval to check and stored the last received offset in case of inactivity.
     *
     * <p>Default is 5 seconds.
     *
     * @param flushInterval
     * @return the auto-tracking strategy
     */
    AutoTrackingStrategy flushInterval(Duration flushInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /**
   * Message flow configuration.
   *
   * <p>The default configuration uses {@link ConsumerFlowStrategy#creditOnChunkArrival()}.
   *
   * @since 0.11.0
   * @see ConsumerFlowStrategy#creditOnChunkArrival()
   * @see MessageHandler.Context#processed()
   */
  interface FlowConfiguration {

    /**
     * The number of initial credits for the subscription.
     *
     * <p>Default is 10.
     *
     * <p>This calls uses {@link ConsumerFlowStrategy#creditOnChunkArrival(int)}.
     *
     * <p>Use a small value like 1 for streams with large chunks (several hundreds of messages per
     * chunk) and higher values (5 or more) for streams with small chunks (1 or a few messages per
     * chunk).
     *
     * @param initialCredits the number of initial credits
     * @return this configuration instance
     * @see ConsumerFlowStrategy#creditOnChunkArrival(int)
     */
    FlowConfiguration initialCredits(int initialCredits);

    /**
     * Flow strategy to use
     *
     * @param strategy the strategy to use
     * @return this configuration instance
     * @since 0.12.0
     * @see ConsumerFlowStrategy
     * @see ConsumerFlowStrategy#creditOnChunkArrival()
     * @see ConsumerFlowStrategy#creditOnChunkArrival(int)
     * @see ConsumerFlowStrategy#creditWhenHalfMessagesProcessed()
     * @see ConsumerFlowStrategy#creditWhenHalfMessagesProcessed(int)
     * @see ConsumerFlowStrategy#creditOnProcessedMessageCount(int, double)
     */
    FlowConfiguration strategy(ConsumerFlowStrategy strategy);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /**
   * Filter configuration.
   *
   * <p>RabbitMQ 3.13 or more is required.
   */
  interface FilterConfiguration {

    /**
     * Set the filter values.
     *
     * @param filterValues
     * @return this filter configuration instance
     */
    FilterConfiguration values(String... filterValues);

    /**
     * Client-side filtering logic, occurring after the server-side filtering.
     *
     * <p>It must be consistent with the requested filter {@link #values( String...)} and the {@link
     * #matchUnfiltered()} flag.
     *
     * @param filter a predicate that returns <code>true</code> if a message should go to the {@link
     *     MessageHandler}
     * @return this filter configuration instance
     */
    FilterConfiguration postFilter(Predicate<Message> filter);

    /**
     * Whether messages without a filter value should be sent as well.
     *
     * <p>Default is false.
     *
     * @return this filter configuration instance
     */
    FilterConfiguration matchUnfiltered();

    /**
     * Whether messages without a filter value should be sent as well.
     *
     * <p>Default is false.
     *
     * @param matchUnfiltered
     * @return this filter configuration instance
     */
    FilterConfiguration matchUnfiltered(boolean matchUnfiltered);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }
}

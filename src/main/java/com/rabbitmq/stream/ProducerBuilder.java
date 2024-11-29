// Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.compression.Compression;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/** API to create and configure a {@link Producer}. */
public interface ProducerBuilder {

  /**
   * The producer name for deduplication (<b>read the <a
   * href="https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#outbound-message-deduplication">documentation</a>
   * before use</b>).
   *
   * <p>There must be only one producer instance at the same time using a given name.
   *
   * @param name
   * @return this builder instance
   * @see MessageBuilder#publishingId(long)
   * @see <a
   *     href="https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#outbound-message-deduplication">Deduplication
   *     documentation</a>
   */
  ProducerBuilder name(String name);

  /**
   * The stream to send messages to.
   *
   * @param stream
   * @return this builder instance
   */
  ProducerBuilder stream(String stream);

  /**
   * The super stream to send messages to.
   *
   * <p>This is an experimental API, subject to change.
   *
   * @param superStream
   * @return this builder instance
   * @see #routing(Function)
   */
  ProducerBuilder superStream(String superStream);

  /**
   * The number of messages to put in a sub-entry of a publish frame.
   *
   * <p>The default is 1 (no sub-entry batching).
   *
   * @param subEntrySize
   * @return this builder instance
   */
  ProducerBuilder subEntrySize(int subEntrySize);

  /**
   * Compression algorithm to use to compress a batch of sub-entries.
   *
   * <p>Compression can take advantage of similarity in messages to significantly reduce the size of
   * the sub-entry batch. This translates to less bandwidth and storage used, at the cost of more
   * CPU usage to compress and decompress on the client side. Note the server is not involved in the
   * compression/decompression process.
   *
   * <p>Default is no compression.
   *
   * @param compression
   * @return this builder instance
   * @see Compression
   */
  ProducerBuilder compression(Compression compression);

  /**
   * The maximum number of messages to accumulate before sending them to the broker.
   *
   * <p>Default is 100.
   *
   * @param batchSize
   * @return this builder instance
   */
  ProducerBuilder batchSize(int batchSize);

  /**
   * Period to send a batch of messages.
   *
   * <p>Default is 100 ms.
   *
   * @param batchPublishingDelay
   * @return this builder instance
   */
  ProducerBuilder batchPublishingDelay(Duration batchPublishingDelay);

  /**
   * Adapt batch size depending on ingress rate.
   *
   * <p>A dynamic-batch approach improves latency for low ingress rates. It can be counterproductive
   * for sustained high ingress rates.
   *
   * <p>Set this flag to <code>true</code> if you want as little delay as possible between calling
   * {@link Producer#send(Message, ConfirmationHandler)} and the message being sent to the broker.
   *
   * <p>Set this flag to <code>false</code> if latency is not critical for your use case and you
   * want the highest throughput possible for both publishing and consuming.
   *
   * <p>Dynamic batch is not activated by default (<code>dynamicBatch = false</code>).
   *
   * <p>Dynamic batch is experimental.
   *
   * @param dynamicBatch
   * @return this builder instance
   * @since 0.20.0
   */
  ProducerBuilder dynamicBatch(boolean dynamicBatch);

  /**
   * The maximum number of unconfirmed outbound messages.
   *
   * <p>{@link Producer#send(Message, ConfirmationHandler)} will start blocking when the limit is
   * reached.
   *
   * <p>Default is 10,000.
   *
   * @param maxUnconfirmedMessages
   * @return this builder instance
   */
  ProducerBuilder maxUnconfirmedMessages(int maxUnconfirmedMessages);

  /**
   * Time before the client calls the confirm callback to signal outstanding unconfirmed messages
   * timed out.
   *
   * <p>Default is 30 seconds.
   *
   * @param timeout
   * @return this builder instance
   */
  ProducerBuilder confirmTimeout(Duration timeout);

  /**
   * Time before enqueueing of a message fail when the maximum number of unconfirmed is reached.
   *
   * <p>Default is 10 seconds.
   *
   * <p>Set the value to {@link Duration#ZERO} if there should be no timeout.
   *
   * @param timeout
   * @return this builder instance
   */
  ProducerBuilder enqueueTimeout(Duration timeout);

  /**
   * Whether to republish unconfirmed messages after recovery.
   *
   * <p>Default is <code>true</code> (unconfirmed messages are republished after recovery).
   *
   * <p>Set to <code>false</code> to not republish unconfirmed messages and get a negative {@link
   * ConfirmationStatus} for unconfirmed messages.
   *
   * <p>Note setting this flag to <code>false</code> translates to at-most-once semantics, that is
   * published messages may be lost, unless the publishing application retries publishing them.
   *
   * @param retryOnRecovery retry flag
   * @return this builder instance
   * @since 0.19.0
   */
  ProducerBuilder retryOnRecovery(boolean retryOnRecovery);

  /**
   * Logic to extract a filter value from a message.
   *
   * <p>RabbitMQ 3.13 or more is required.
   *
   * @param filterValueExtractor
   * @return this builder instance
   */
  ProducerBuilder filterValue(Function<Message, String> filterValueExtractor);

  /**
   * Create the {@link Producer} instance.
   *
   * @return the configured producer
   */
  Producer build();

  /**
   * Configure the routing for super streams (partitioned streams).
   *
   * <p>This is an experimental API, subject to change.
   *
   * <p>The to-be-created producer will be a composite producer when this method is called. It will
   * use the routing configuration to find out where a message should be routed. The application
   * developer must provide the logic to extract a "routing key" from a message, which will decide
   * the destination(s) of the message.
   *
   * <p>The default routing strategy hashes the routing key to choose the stream (partition) to send
   * the message to.
   *
   * <p>Note the routing key extraction logic is required only when the built-in routing strategies
   * are used. It can set to <code>null</code> when a custom {@link RoutingStrategy} is set with
   * {@link #routing(Function)}.
   *
   * @param routingKeyExtractor the logic to extract a routing key from a message
   * @return the routing configuration instance
   * @see RoutingConfiguration
   */
  RoutingConfiguration routing(Function<Message, String> routingKeyExtractor);

  /**
   * Routing configuration for super streams (partitioned streams).
   *
   * <p>This is an experimental API, subject to change.
   */
  interface RoutingConfiguration {

    /**
     * Enable the "hash" routing strategy (the default).
     *
     * <p>The default hash algorithm is 32-bit MurmurHash3.
     *
     * @return the routing configuration instance
     */
    RoutingConfiguration hash();

    /**
     * Enable the "hash" routing strategy with a specific hash algorithm.
     *
     * @param hash
     * @return the routing configuration instance
     */
    RoutingConfiguration hash(ToIntFunction<String> hash);

    /**
     * Enable the "key" routing strategy.
     *
     * <p>It consists in using the "route" command of the RabbitMQ Stream protocol to determine the
     * streams to send a message to.
     *
     * @return the routing configuration instance
     */
    RoutingConfiguration key();

    /**
     * Set the routing strategy to use.
     *
     * <p>Providing the routing strategy provides control over the streams a message is routed to
     * (routing key extraction logic if relevant and destination(s) decision).
     *
     * @param routingStrategy
     * @return the routing configuration instance
     */
    RoutingConfiguration strategy(RoutingStrategy routingStrategy);

    /**
     * Go back to the producer builder.
     *
     * @return the producer builder
     */
    ProducerBuilder producerBuilder();
  }
}

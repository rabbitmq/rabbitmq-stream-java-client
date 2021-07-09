// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
   * The logical name of the producer.
   *
   * <p>Set a value to enable de-duplication.
   *
   * @param name
   * @return this builder instance
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
   * The number of messages to put in a sub-entry of a publish frame.
   *
   * <p>The default is 1 (no sub-entry batching).
   *
   * @param subEntrySize
   * @return this builder instance
   */
  ProducerBuilder subEntrySize(int subEntrySize);

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
   * Routing strategy for super streams. Experimental!
   *
   * @param routingKeyExtractor
   * @param routingType
   * @return this builder instance
   */
  ProducerBuilder routing(Function<Message, String> routingKeyExtractor, RoutingType routingType);

  /**
   * Routing strategy for super streams. Experimental!
   *
   * @param routingKeyExtractor
   * @param routingType
   * @param hash
   * @return this builder instance
   */
  ProducerBuilder routing(
      Function<Message, String> routingKeyExtractor,
      RoutingType routingType,
      ToIntFunction<String> hash);

  /**
   * Create the {@link Producer} instance.
   *
   * @return the configured producer
   */
  Producer build();

  enum RoutingType {
    HASH,
    KEY
  }
}

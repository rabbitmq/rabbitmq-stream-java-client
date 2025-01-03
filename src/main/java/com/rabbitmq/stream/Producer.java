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

/**
 * API to send messages to a RabbitMQ Stream.
 *
 * <p>Instances are created and configured with a {@link ProducerBuilder}.
 *
 * <p>Implementations are expected to be thread-safe, RabbitMQ Streams' are.
 *
 * @see ProducerBuilder
 * @see Environment#producerBuilder()
 */
public interface Producer extends AutoCloseable {

  /**
   * Return a {@link MessageBuilder} to create a {@link Message}.
   *
   * <p>A {@link MessageBuilder} instance is meant to create only {@link Message} instance.
   *
   * @return a single-usage {@link MessageBuilder}
   * @see MessageBuilder
   */
  MessageBuilder messageBuilder();

  /**
   * Get the last publishing ID for a named producer.
   *
   * <p>The value can be mapped to a business index to restart publishing where a previous
   * incarnation of the producer left off.
   *
   * @return the last publishing ID for this named producer
   */
  long getLastPublishingId();

  /**
   * Publish a message.
   *
   * @param message the message
   * @param confirmationHandler the callback when the message is confirmed or failed
   */
  void send(Message message, ConfirmationHandler confirmationHandler);

  /** Close the producer. */
  @Override
  void close();
}

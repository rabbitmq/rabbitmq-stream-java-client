// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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
 * API to consume messages from a RabbitMQ Stream.
 *
 * <p>Instances are configured and created with a {@link ConsumerBuilder}.
 *
 * @see ConsumerBuilder
 * @see Environment#consumerBuilder()
 */
public interface Consumer extends AutoCloseable {

  /**
   * Store the offset.
   *
   * @param offset
   */
  void store(long offset);

  /** Close the consumer. */
  @Override
  void close();

  /**
   * The stored offset for this named consumer on the configured stream.
   *
   * @return stored offset
   * @since 0.6.0
   * @throws NoOffsetException if no offset is stored for this name on the stream
   */
  long storedOffset();
}

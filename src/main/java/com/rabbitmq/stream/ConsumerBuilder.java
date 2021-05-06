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

import java.time.Duration;

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
   * Enable {@link ManualCommitStrategy}.
   *
   * @return the manual commit strategy
   */
  ManualCommitStrategy manualCommitStrategy();

  /**
   * Enable {@link AutoCommitStrategy}.
   *
   * <p>This is the default commit strategy.
   *
   * @return the auto-commit strategy
   */
  AutoCommitStrategy autoCommitStrategy();

  /**
   * Create the configured {@link Consumer}
   *
   * @return the configured consumer
   */
  Consumer build();

  /** Manual commit strategy. */
  interface ManualCommitStrategy {

    /**
     * Interval to check if the last requested committed offset has been actually committed.
     *
     * <p>Default is 5 seconds.
     *
     * @param checkInterval
     * @return the manual commit strategy
     */
    ManualCommitStrategy checkInterval(Duration checkInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /** Auto-commit strategy. */
  interface AutoCommitStrategy {

    /**
     * Number of messages before committing.
     *
     * <p>Default is 10,000.
     *
     * @param messageCountBeforeCommit
     * @return the auto-commit strategy
     */
    AutoCommitStrategy messageCountBeforeCommit(int messageCountBeforeCommit);

    /**
     * Interval to check and commit the last received offset in case of inactivity.
     *
     * <p>Default is 5 seconds.
     *
     * @param flushInterval
     * @return the auto-commit strategy
     */
    AutoCommitStrategy flushInterval(Duration flushInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }
}

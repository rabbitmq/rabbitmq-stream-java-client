// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
 * The {@link Environment} is the main entry point to a node or a cluster of nodes. {@link Producer}
 * and {@link Consumer} instances are created from an {@link Environment} instance. An {@link
 * Environment} can also create and delete streams.
 *
 * <p>Applications are supposed to use a single {@link Environment} instance to interact with a
 * cluster.
 *
 * <p>Use {@link Environment#builder()} to configure and create an {@link Environment} instance.
 *
 * <p>{@link Environment} instances are expected to be thread-safe.
 *
 * @see EnvironmentBuilder
 */
public interface Environment extends AutoCloseable {

  /**
   * Create a builder to configure and create an {@link Environment}
   *
   * @return the environment builder
   * @see EnvironmentBuilder
   */
  static EnvironmentBuilder builder() {
    try {
      return (EnvironmentBuilder)
          Class.forName("com.rabbitmq.stream.impl.StreamEnvironmentBuilder")
              .getConstructor()
              .newInstance();
    } catch (Exception e) {
      throw new StreamException("Error while creating stream environment builder", e);
    }
  }

  /**
   * Return a {@link StreamCreator} to configure and create a stream.
   *
   * @return the stream creator
   * @see StreamCreator
   */
  StreamCreator streamCreator();

  /**
   * Delete a stream
   *
   * @param stream the stream to delete
   * @since 0.15.0
   */
  void deleteStream(String stream);

  /**
   * Delete a super stream.
   *
   * <p>Requires RabbitMQ 3.13.0 or more.
   *
   * @param superStream the super stream to delete
   */
  void deleteSuperStream(String superStream);

  /**
   * Query statistics on a stream.
   *
   * <p>Requires RabbitMQ 3.11 or more.
   *
   * @param stream the stream to get statistics from
   * @return statistics on the stream
   * @throws UnsupportedOperationException if the broker does not support this command
   */
  StreamStats queryStreamStats(String stream);

  /**
   * Return whether a stream exists or not.
   *
   * @param stream the stream to check the existence
   * @return true if stream exists, false if it does not exist
   * @throws StreamException if response code is different from {@link Constants#RESPONSE_CODE_OK}
   *     or {@link Constants#RESPONSE_CODE_STREAM_DOES_NOT_EXIST}
   */
  boolean streamExists(String stream);

  /**
   * Create a {@link ProducerBuilder} to configure and create a {@link Producer}.
   *
   * @return the producer builder
   * @see ProducerBuilder
   */
  ProducerBuilder producerBuilder();

  /**
   * Create a {@link ConsumerBuilder} to configure and create a {@link Consumer}
   *
   * @return the consumer builder
   * @see ConsumerBuilder
   */
  ConsumerBuilder consumerBuilder();

  /** Close the environment and its resources. */
  @Override
  void close();
}

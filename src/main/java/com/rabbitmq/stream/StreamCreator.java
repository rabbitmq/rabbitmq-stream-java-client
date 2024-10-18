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

import java.time.Duration;
import java.util.function.Function;

/** API to configure and create a stream. */
public interface StreamCreator {

  /** Segment size is limited to 3 GB. */
  ByteCapacity MAX_SEGMENT_SIZE = ByteCapacity.from("3GB");

  /**
   * The name of the stream.
   *
   * <p>Alias for {@link #name(String)}.
   *
   * @param stream
   * @return this creator instance
   */
  StreamCreator stream(String stream);

  /**
   * The name of the (super) stream.
   *
   * @param name
   * @return this creator instance
   * @since 0.15.0
   */
  StreamCreator name(String name);

  /**
   * The maximum size of the stream before it gets truncated.
   *
   * @param byteCapacity
   * @return this creator instance
   */
  StreamCreator maxLengthBytes(ByteCapacity byteCapacity);

  /**
   * The maximum size of each stream segments.
   *
   * <p>Maximum size is {@link StreamCreator#MAX_SEGMENT_SIZE} (3 GB).
   *
   * @param byteCapacity
   * @return this creator instance
   */
  StreamCreator maxSegmentSizeBytes(ByteCapacity byteCapacity);

  /**
   * The maximum age of a stream before it gets truncated.
   *
   * @param maxAge
   * @return this creator instance
   */
  StreamCreator maxAge(Duration maxAge);

  /**
   * The {@link LeaderLocator} strategy.
   *
   * @param leaderLocator
   * @return this creator instance
   */
  StreamCreator leaderLocator(LeaderLocator leaderLocator);

  /**
   * Set the size of the stream chunk filters.
   *
   * <p>Must be between 16 and 255 bytes, default is 16.
   *
   * <p>Use a bloom filter calculator to size the filter accordingly to the possible number of
   * filter values and the acceptable rate of false positives (RabbitMQ Stream uses 2 hash
   * functions).
   *
   * @param size (in bytes)
   * @return this creator instance
   * @see ProducerBuilder#filterValue(Function)
   * @see ConsumerBuilder#filter()
   */
  StreamCreator filterSize(int size);

  /**
   * Set the number of initial members the stream should have.
   *
   * @param initialMemberCount initial number of nodes
   * @return this creator instance
   * @see <a href="https://www.rabbitmq.com/docs/streams#replication-factor">Initial Replication
   *     Factor</a>
   */
  StreamCreator initialMemberCount(int initialMemberCount);

  /**
   * Configure the super stream to create.
   *
   * <p>Requires RabbitMQ 3.13.0 or more.
   *
   * @return the super stream configuration
   * @since 0.15.0
   */
  SuperStreamConfiguration superStream();

  /**
   * Create the stream.
   *
   * <p>This method is idempotent: the stream exists when it returns.
   */
  void create();

  /** The leader locator strategy. */
  enum LeaderLocator {

    /**
     * The stream leader will be on the node the client is connected to.
     *
     * <p>This is the default value.
     */
    CLIENT_LOCAL("client-local"),

    /**
     * The leader will be the node hosting the minimum number of stream leaders, if there are
     * overall less than 1000 queues, or a random node, if there are overall more than 1000 queues.
     *
     * <p>Available as of RabbitMQ 3.10.
     *
     * <p>Default value for RabbitMQ 3.10+.
     */
    BALANCED("balanced"),

    /**
     * The stream leader will be a random node of the cluster.
     *
     * <p>Deprecated as of RabbitMQ 3.10, same as {@link LeaderLocator#BALANCED}.
     */
    RANDOM("random"),

    /**
     * The stream leader will be on the node with the least number of stream leaders.
     *
     * <p>Deprecated as of RabbitMQ 3.10, same as {@link LeaderLocator#BALANCED}.
     *
     * <p>Default value for RabbitMQ 3.9.
     */
    LEAST_LEADERS("least-leaders");

    String value;

    LeaderLocator(String value) {
      this.value = value;
    }

    public static LeaderLocator from(String value) {
      for (LeaderLocator leaderLocator : values()) {
        if (leaderLocator.value.equals(value)) {
          return leaderLocator;
        }
      }
      throw new IllegalArgumentException("Unknown leader locator value: " + value);
    }

    public String value() {
      return this.value;
    }
  }

  /**
   * Super stream configuration.
   *
   * @since 0.15.0
   */
  interface SuperStreamConfiguration {

    /**
     * The number of partitions of the super stream.
     *
     * <p>Mutually exclusive with {@link #bindingKeys(String...)}. Default is 3.
     *
     * @param partitions
     * @return this super stream configuration instance
     */
    SuperStreamConfiguration partitions(int partitions);

    /**
     * The binding keys to use when declaring the super stream partitions.
     *
     * <p>Mutually exclusive with {@link #partitions(int)}. Default is null.
     *
     * @param bindingKeys
     * @return this super stream configuration instance
     */
    SuperStreamConfiguration bindingKeys(String... bindingKeys);

    /**
     * Go back to the creator.
     *
     * @return the stream creator
     */
    StreamCreator creator();
  }
}

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

/** API to configure and create a stream. */
public interface StreamCreator {

  /**
   * The name of the stream
   *
   * @param stream
   * @return this creator instance
   */
  StreamCreator stream(String stream);

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
   * Create the stream.
   *
   * <p>This method is idempotent: the stream exists when it returns.
   */
  void create();

  /** The leader locator strategy. */
  enum LeaderLocator {
    /** The stream leader will be on the node the client is connected to. */
    CLIENT_LOCAL("client-local"),

    /** The stream leader will be a random node of the cluster. */
    RANDOM("random"),

    /** The stream leader will be on the node with the least number of stream leaders. */
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
}

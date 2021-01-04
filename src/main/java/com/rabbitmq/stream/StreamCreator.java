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

public interface StreamCreator {

  StreamCreator stream(String stream);

  StreamCreator maxLengthBytes(ByteCapacity byteCapacity);

  StreamCreator maxSegmentSizeBytes(ByteCapacity byteCapacity);

  StreamCreator maxAge(Duration maxAge);

  StreamCreator leaderLocator(LeaderLocator leaderLocator);

  void create();

  enum LeaderLocator {
    CLIENT_LOCAL("client-local"),
    RANDOM("random"),
    LEAST_LEADERS("least-leaders");

    String value;

    LeaderLocator(String value) {
      this.value = value;
    }

    public String value() {
      return this.value;
    }

    public static LeaderLocator from(String value) {
      for (LeaderLocator leaderLocator : values()) {
        if (leaderLocator.value.equals(value)) {
          return leaderLocator;
        }
      }
      throw new IllegalArgumentException("Unknown leader locator value: " + value);
    }
  }
}

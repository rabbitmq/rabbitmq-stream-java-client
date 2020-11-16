// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamException;
import java.time.Duration;

class StreamStreamCreator implements StreamCreator {

  private final StreamEnvironment environment;
  private final Client.StreamParametersBuilder streamParametersBuilder =
      new Client.StreamParametersBuilder().leaderLocator(LeaderLocator.LEAST_LEADERS);
  private String stream;

  StreamStreamCreator(StreamEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public StreamCreator stream(String stream) {
    this.stream = stream;
    return this;
  }

  @Override
  public StreamCreator maxLengthBytes(ByteCapacity byteCapacity) {
    streamParametersBuilder.maxLengthBytes(byteCapacity);
    return this;
  }

  @Override
  public StreamCreator maxSegmentSizeBytes(ByteCapacity byteCapacity) {
    streamParametersBuilder.maxSegmentSizeBytes(byteCapacity);
    return this;
  }

  @Override
  public StreamCreator maxAge(Duration maxAge) {
    streamParametersBuilder.maxAge(maxAge);
    return this;
  }

  @Override
  public StreamCreator leaderLocator(LeaderLocator leaderLocator) {
    streamParametersBuilder.leaderLocator(leaderLocator);
    return this;
  }

  @Override
  public void create() {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null");
    }
    Client.Response response =
        environment.locator().create(stream, streamParametersBuilder.build());
    if (!response.isOk()) {
      throw new StreamException(
          "Error while creating stream " + stream, response.getResponseCode());
    }
  }
}

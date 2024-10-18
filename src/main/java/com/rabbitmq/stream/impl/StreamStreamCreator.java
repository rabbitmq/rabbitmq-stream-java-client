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
package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.namedFunction;
import static java.util.stream.Collectors.toList;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

class StreamStreamCreator implements StreamCreator {

  private final StreamEnvironment environment;
  private final Client.StreamParametersBuilder streamParametersBuilder =
      new Client.StreamParametersBuilder().leaderLocator(LeaderLocator.LEAST_LEADERS);
  private String name;
  private DefaultSuperStreamConfiguration superStreamConfiguration;

  StreamStreamCreator(StreamEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public StreamCreator stream(String stream) {
    this.name = stream;
    return this;
  }

  @Override
  public StreamCreator name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public StreamCreator maxLengthBytes(ByteCapacity byteCapacity) {
    streamParametersBuilder.maxLengthBytes(byteCapacity);
    return this;
  }

  @Override
  public StreamCreator maxSegmentSizeBytes(ByteCapacity byteCapacity) {
    if (byteCapacity != null && byteCapacity.compareTo(MAX_SEGMENT_SIZE) > 0) {
      throw new IllegalArgumentException(
          "The maximum segment size cannot be more than " + MAX_SEGMENT_SIZE);
    }
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
  public StreamCreator filterSize(int size) {
    streamParametersBuilder.filterSize(size);
    return this;
  }

  @Override
  public StreamCreator initialMemberCount(int initialMemberCount) {
    streamParametersBuilder.initialMemberCount(initialMemberCount);
    return this;
  }

  @Override
  public StreamCreator argument(String key, String value) {
    streamParametersBuilder.put(key, value);
    return this;
  }

  @Override
  public SuperStreamConfiguration superStream() {
    if (this.superStreamConfiguration == null) {
      this.superStreamConfiguration = new DefaultSuperStreamConfiguration(this);
    }
    return this.superStreamConfiguration;
  }

  @Override
  public void create() {
    if (name == null) {
      throw new IllegalArgumentException("Stream name cannot be null");
    }
    Function<Client, Client.Response> function;
    boolean superStream = this.superStreamConfiguration != null;
    if (superStream) {
      List<String> partitions, bindingKeys;
      if (this.superStreamConfiguration.bindingKeys == null) {
        partitions =
            IntStream.range(0, this.superStreamConfiguration.partitions)
                .mapToObj(i -> this.name + "-" + i)
                .collect(toList());
        bindingKeys =
            IntStream.range(0, this.superStreamConfiguration.partitions)
                .mapToObj(String::valueOf)
                .collect(toList());
      } else {
        partitions =
            this.superStreamConfiguration.bindingKeys.stream()
                .map(rk -> this.name + "-" + rk)
                .collect(toList());
        bindingKeys = this.superStreamConfiguration.bindingKeys;
      }
      function =
          namedFunction(
              c ->
                  c.createSuperStream(
                      this.name, partitions, bindingKeys, streamParametersBuilder.build()),
              "Creation of super stream '%s'",
              this.name);
    } else {
      function =
          namedFunction(
              c -> c.create(name, streamParametersBuilder.build()),
              "Creation of stream '%s'",
              this.name);
    }
    this.environment.maybeInitializeLocator();
    Client.Response response = environment.locatorOperation(function);
    if (!response.isOk()
        && response.getResponseCode() != Constants.RESPONSE_CODE_STREAM_ALREADY_EXISTS) {
      String label = superStream ? "super stream" : "stream";
      throw new StreamException(
          "Error while creating "
              + label
              + " '"
              + name
              + "' ("
              + formatConstant(response.getResponseCode())
              + ")",
          response.getResponseCode());
    }
  }

  private static class DefaultSuperStreamConfiguration implements SuperStreamConfiguration {

    private final StreamCreator creator;

    private int partitions = 3;
    private List<String> bindingKeys = null;

    private DefaultSuperStreamConfiguration(StreamCreator creator) {
      this.creator = creator;
    }

    @Override
    public SuperStreamConfiguration partitions(int partitions) {
      if (partitions <= 0) {
        throw new IllegalArgumentException("The number of partitions must be greater than 0");
      }
      this.partitions = partitions;
      this.bindingKeys = null;
      return this;
    }

    @Override
    public SuperStreamConfiguration bindingKeys(String... bindingKeys) {
      if (bindingKeys == null || bindingKeys.length == 0) {
        throw new IllegalArgumentException("There must be at least 1 binding key");
      }
      this.bindingKeys = Arrays.asList(bindingKeys);
      this.partitions = -1;
      return this;
    }

    @Override
    public StreamCreator creator() {
      return this.creator;
    }
  }
}

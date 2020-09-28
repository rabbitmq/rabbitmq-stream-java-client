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

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import java.time.Duration;

class StreamConsumerBuilder implements ConsumerBuilder {

  private final StreamEnvironment environment;

  private String stream;
  private OffsetSpecification offsetSpecification = OffsetSpecification.first();
  private MessageHandler messageHandler;
  private String name;
  private DefaultAutoCommitStrategy autoCommitStrategy;
  private DefaultManualCommitStrategy manualCommitStrategy;

  public StreamConsumerBuilder(StreamEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public ConsumerBuilder stream(String stream) {
    this.stream = stream;
    return this;
  }

  @Override
  public ConsumerBuilder offset(OffsetSpecification offsetSpecification) {
    this.offsetSpecification = offsetSpecification;
    return this;
  }

  @Override
  public ConsumerBuilder messageHandler(MessageHandler messageHandler) {
    this.messageHandler = messageHandler;
    return this;
  }

  @Override
  public ConsumerBuilder name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public ManualCommitStrategy manualCommitStrategy() {
    this.manualCommitStrategy = new DefaultManualCommitStrategy(this);
    this.autoCommitStrategy = null;
    return this.manualCommitStrategy;
  }

  @Override
  public AutoCommitStrategy autoCommitStrategy() {
    this.autoCommitStrategy = new DefaultAutoCommitStrategy(this);
    this.manualCommitStrategy = null;
    return this.autoCommitStrategy;
  }

  @Override
  public Consumer build() {
    if (this.stream == null) {
      throw new IllegalArgumentException("stream cannot be null");
    }
    if (this.name == null
        && (this.autoCommitStrategy != null || this.manualCommitStrategy != null)) {
      throw new IllegalArgumentException("A name must be set if a commit strategy is specified");
    }

    CommitConfiguration commitConfiguration;
    if (this.autoCommitStrategy != null) {
      commitConfiguration =
          new CommitConfiguration(
              true,
              true,
              this.autoCommitStrategy.messageCountBeforeCommit,
              this.autoCommitStrategy.flushInterval);
    } else if (this.name != null) {
      commitConfiguration = new CommitConfiguration(true, false, -1, Duration.ZERO);
    } else {
      commitConfiguration = new CommitConfiguration(false, false, -1, Duration.ZERO);
    }

    StreamConsumer consumer =
        new StreamConsumer(
            this.stream,
            this.offsetSpecification,
            this.messageHandler,
            this.name,
            this.environment,
            commitConfiguration);
    environment.addConsumer(consumer);
    return consumer;
  }

  static class CommitConfiguration {

    private final boolean enabled;
    private final boolean auto;

    private final int autoMessageCountBeforeCommit;
    private final Duration autoFlushInterval;

    CommitConfiguration(
        boolean enabled,
        boolean auto,
        int autoMessageCountBeforeCommit,
        Duration autoFlushInterval) {
      this.enabled = enabled;
      this.auto = auto;
      this.autoMessageCountBeforeCommit = autoMessageCountBeforeCommit;
      this.autoFlushInterval = autoFlushInterval;
    }

    boolean auto() {
      return this.auto;
    }

    boolean enabled() {
      return this.enabled;
    }

    public int autoMessageCountBeforeCommit() {
      return autoMessageCountBeforeCommit;
    }

    public Duration autoFlushInterval() {
      return autoFlushInterval;
    }
  }

  private static final class DefaultAutoCommitStrategy implements AutoCommitStrategy {

    private final StreamConsumerBuilder builder;
    private int messageCountBeforeCommit = 10_000;
    private Duration flushInterval = Duration.ofSeconds(5);

    private DefaultAutoCommitStrategy(StreamConsumerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public AutoCommitStrategy messageCountBeforeCommit(int messageCountBeforeCommit) {
      if (messageCountBeforeCommit <= 0) {
        throw new IllegalArgumentException(
            "the number of messages before committing must be positive");
      }
      this.messageCountBeforeCommit = messageCountBeforeCommit;
      return this;
    }

    @Override
    public AutoCommitStrategy flushInterval(Duration flushInterval) {
      if (flushInterval.toMillis() >= 1000) {
        throw new IllegalArgumentException("the flush interval cannot be shorter than 1 second");
      }
      this.flushInterval = flushInterval;
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }
  }

  private static final class DefaultManualCommitStrategy implements ManualCommitStrategy {

    private final StreamConsumerBuilder builder;
    private Duration checkInterval = Duration.ofSeconds(5);

    private DefaultManualCommitStrategy(StreamConsumerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public ManualCommitStrategy checkInterval(Duration checkInterval) {
      this.checkInterval = checkInterval;
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }
  }
}

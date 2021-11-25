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
package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.ConsumerUpdateListener;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.SubscriptionListener;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class StreamConsumerBuilder implements ConsumerBuilder {

  private static final int NAME_MAX_SIZE = 256; // server-side limitation
  private final StreamEnvironment environment;

  private String stream, superStream;
  private OffsetSpecification offsetSpecification = null;
  private MessageHandler messageHandler;
  private String name;
  private DefaultAutoTrackingStrategy autoTrackingStrategy;
  private DefaultManualTrackingStrategy manualTrackingStrategy;
  private boolean lazyInit = false;
  private SubscriptionListener subscriptionListener = subscriptionContext -> {};
  private Map<String, String> subscriptionProperties = new ConcurrentHashMap<>();
  private ConsumerUpdateListener consumerUpdateListener;

  public StreamConsumerBuilder(StreamEnvironment environment) {
    this.environment = environment;
  }

  @Override
  public ConsumerBuilder stream(String stream) {
    this.stream = stream;
    return this;
  }

  @Override
  public ConsumerBuilder superStream(String superStream) {
    this.superStream = superStream;
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

  MessageHandler messageHandler() {
    return this.messageHandler;
  }

  @Override
  public ConsumerBuilder name(String name) {
    if (name == null || name.length() > NAME_MAX_SIZE) {
      throw new IllegalArgumentException(
          "The consumer name must be non-null and under 256 characters");
    }
    this.name = name;
    return this;
  }

  @Override
  public ConsumerBuilder singleActiveConsumer() {
    this.subscriptionProperties.put("single-active-consumer", "true");
    return this;
  }

  @Override
  public ConsumerBuilder consumerUpdateListener(ConsumerUpdateListener consumerUpdateListener) {
    this.consumerUpdateListener = consumerUpdateListener;
    return this;
  }

  @Override
  public ConsumerBuilder subscriptionListener(SubscriptionListener subscriptionListener) {
    if (subscriptionListener == null) {
      throw new IllegalArgumentException("The subscription listener cannot be null");
    }
    this.subscriptionListener = subscriptionListener;
    return this;
  }

  @Override
  public ManualTrackingStrategy manualTrackingStrategy() {
    this.manualTrackingStrategy = new DefaultManualTrackingStrategy(this);
    this.autoTrackingStrategy = null;
    return this.manualTrackingStrategy;
  }

  @Override
  public AutoTrackingStrategy autoTrackingStrategy() {
    this.autoTrackingStrategy = new DefaultAutoTrackingStrategy(this);
    this.manualTrackingStrategy = null;
    return this.autoTrackingStrategy;
  }

  StreamConsumerBuilder lazyInit(boolean lazyInit) {
    this.lazyInit = lazyInit;
    return this;
  }

  @Override
  public Consumer build() {
    if (this.stream == null && this.superStream == null) {
      throw new IllegalArgumentException("A stream must be specified");
    }
    if (this.stream != null && this.superStream != null) {
      throw new IllegalArgumentException("Stream and superStream cannot be set at the same time");
    }
    if (this.messageHandler == null) {
      throw new IllegalArgumentException("A message handler must be set");
    }
    if (this.name == null
        && (this.autoTrackingStrategy != null || this.manualTrackingStrategy != null)) {
      throw new IllegalArgumentException("A name must be set if a tracking strategy is specified");
    }
    if (Utils.isSac(this.subscriptionProperties) && this.name == null) {
      throw new IllegalArgumentException("A name must be set if single active consumer is enabled");
    }

    this.environment.maybeInitializeLocator();
    TrackingConfiguration trackingConfiguration;
    if (this.autoTrackingStrategy != null) {
      trackingConfiguration =
          new TrackingConfiguration(
              true,
              true,
              this.autoTrackingStrategy.messageCountBeforeStorage,
              this.autoTrackingStrategy.flushInterval,
              Duration.ZERO);
    } else if (this.manualTrackingStrategy != null) {
      trackingConfiguration =
          new TrackingConfiguration(
              true, false, -1, Duration.ZERO, this.manualTrackingStrategy.checkInterval);
    } else if (this.name != null) {
      // the default tracking strategy
      trackingConfiguration =
          new TrackingConfiguration(true, true, 10_000, Duration.ofSeconds(5), Duration.ZERO);
    } else {
      trackingConfiguration =
          new TrackingConfiguration(false, false, -1, Duration.ZERO, Duration.ZERO);
    }

    Consumer consumer;
    if (this.stream != null) {
      consumer =
          new StreamConsumer(
              this.stream,
              this.offsetSpecification,
              this.messageHandler,
              this.name,
              this.environment,
              trackingConfiguration,
              this.lazyInit,
              this.subscriptionListener,
              this.subscriptionProperties,
              this.consumerUpdateListener);
      environment.addConsumer((StreamConsumer) consumer);
    } else {
      if (Utils.isSac(this.subscriptionProperties)) {
        this.subscriptionProperties.put("super-stream", this.superStream);
      }
      consumer =
          new SuperStreamConsumer(this, this.superStream, this.environment, trackingConfiguration);
    }
    return consumer;
  }

  static class TrackingConfiguration {

    private final boolean enabled;
    private final boolean auto;

    private final int autoMessageCountBeforeStorage;
    private final Duration autoFlushInterval;
    private final Duration manualCheckInterval;

    TrackingConfiguration(
        boolean enabled,
        boolean auto,
        int autoMessageCountBeforeStorage,
        Duration autoFlushInterval,
        Duration manualCheckInterval) {
      this.enabled = enabled;
      this.auto = auto;
      this.autoMessageCountBeforeStorage = autoMessageCountBeforeStorage;
      this.autoFlushInterval = autoFlushInterval;
      this.manualCheckInterval = manualCheckInterval;
    }

    boolean auto() {
      return this.auto;
    }

    boolean manual() {
      return !auto();
    }

    boolean enabled() {
      return this.enabled;
    }

    public int autoMessageCountBeforeStorage() {
      return autoMessageCountBeforeStorage;
    }

    public Duration autoFlushInterval() {
      return autoFlushInterval;
    }

    public Duration manualCheckInterval() {
      return manualCheckInterval;
    }
  }

  private static final class DefaultAutoTrackingStrategy implements AutoTrackingStrategy {

    private final StreamConsumerBuilder builder;
    private int messageCountBeforeStorage = 10_000;
    private Duration flushInterval = Duration.ofSeconds(5);

    private DefaultAutoTrackingStrategy(StreamConsumerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public AutoTrackingStrategy messageCountBeforeStorage(int messageCountBeforeStorage) {
      if (messageCountBeforeStorage <= 0) {
        throw new IllegalArgumentException(
            "the number of messages before storing must be positive");
      }
      this.messageCountBeforeStorage = messageCountBeforeStorage;
      return this;
    }

    @Override
    public AutoTrackingStrategy flushInterval(Duration flushInterval) {
      if (flushInterval.toMillis() <= 1000) {
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

  private static final class DefaultManualTrackingStrategy implements ManualTrackingStrategy {

    private final StreamConsumerBuilder builder;
    private Duration checkInterval = Duration.ofSeconds(5);

    private DefaultManualTrackingStrategy(StreamConsumerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public ManualTrackingStrategy checkInterval(Duration checkInterval) {
      if (checkInterval.toMillis() <= 1000 && !checkInterval.isZero()) {
        throw new IllegalArgumentException("the check interval cannot be shorter than 1 second");
      }
      this.checkInterval = checkInterval;
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }
  }

  StreamConsumerBuilder duplicate() {
    StreamConsumerBuilder duplicate = new StreamConsumerBuilder(this.environment);
    for (Field field : StreamConsumerBuilder.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      field.setAccessible(true);
      try {
        field.set(duplicate, field.get(this));
      } catch (IllegalAccessException e) {
        throw new StreamException("Error while duplicating stream producer builder", e);
      }
    }
    return duplicate;
  }
}

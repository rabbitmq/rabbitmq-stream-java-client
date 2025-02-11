// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Utils.SUBSCRIPTION_PROPERTY_FILTER_PREFIX;
import static com.rabbitmq.stream.impl.Utils.SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED;

import com.rabbitmq.stream.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

class StreamConsumerBuilder implements ConsumerBuilder {

  private static final int NAME_MAX_SIZE = Client.MAX_REFERENCE_SIZE; // server-side limitation
  private static final TrackingConfiguration DISABLED_TRACKING_CONFIGURATION =
      new TrackingConfiguration(false, false, -1, Duration.ZERO, Duration.ZERO);
  private final StreamEnvironment environment;
  private final Map<String, String> subscriptionProperties = new ConcurrentHashMap<>();
  private String stream, superStream;
  private OffsetSpecification offsetSpecification = null;
  private MessageHandler messageHandler;
  private String name;
  private DefaultAutoTrackingStrategy autoTrackingStrategy;
  private DefaultManualTrackingStrategy manualTrackingStrategy;
  private boolean noTrackingStrategy = false;
  private boolean lazyInit = false;
  private SubscriptionListener subscriptionListener = subscriptionContext -> {};
  private final DefaultFlowConfiguration flowConfiguration = new DefaultFlowConfiguration(this);
  private ConsumerUpdateListener consumerUpdateListener;
  private DefaultFilterConfiguration filterConfiguration;

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
    if (name == null || name.length() >= NAME_MAX_SIZE) {
      throw new IllegalArgumentException(
          "The consumer name must be non-null and less than 256 characters");
    }
    this.name = name;
    return this;
  }

  @Override
  public ConsumerBuilder singleActiveConsumer() {
    this.subscriptionProperties.put(Utils.SUBSCRIPTION_PROPERTY_SAC, "true");
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
  @SuppressFBWarnings("AT_STALE_THREAD_WRITE_OF_PRIMITIVE")
  public ManualTrackingStrategy manualTrackingStrategy() {
    this.manualTrackingStrategy = new DefaultManualTrackingStrategy(this);
    this.autoTrackingStrategy = null;
    this.noTrackingStrategy = false;
    return this.manualTrackingStrategy;
  }

  @Override
  @SuppressFBWarnings("AT_STALE_THREAD_WRITE_OF_PRIMITIVE")
  public AutoTrackingStrategy autoTrackingStrategy() {
    this.autoTrackingStrategy = new DefaultAutoTrackingStrategy(this);
    this.manualTrackingStrategy = null;
    this.noTrackingStrategy = false;
    return this.autoTrackingStrategy;
  }

  @Override
  @SuppressFBWarnings("AT_STALE_THREAD_WRITE_OF_PRIMITIVE")
  public ConsumerBuilder noTrackingStrategy() {
    this.noTrackingStrategy = true;
    this.autoTrackingStrategy = null;
    this.manualTrackingStrategy = null;
    return this;
  }

  @Override
  public FlowConfiguration flow() {
    return this.flowConfiguration;
  }

  @SuppressFBWarnings("AT_STALE_THREAD_WRITE_OF_PRIMITIVE")
  StreamConsumerBuilder lazyInit(boolean lazyInit) {
    this.lazyInit = lazyInit;
    return this;
  }

  @Override
  public FilterConfiguration filter() {
    if (this.filterConfiguration == null) {
      this.filterConfiguration = new DefaultFilterConfiguration(this);
    }
    return this.filterConfiguration;
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
        && !this.noTrackingStrategy
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
    } else if (this.noTrackingStrategy) {
      trackingConfiguration = DISABLED_TRACKING_CONFIGURATION;
    } else if (this.name != null) {
      // the default tracking strategy
      trackingConfiguration =
          new TrackingConfiguration(true, true, 10_000, Duration.ofSeconds(5), Duration.ZERO);
    } else {
      trackingConfiguration = DISABLED_TRACKING_CONFIGURATION;
    }

    MessageHandler handler;
    if (this.filterConfiguration == null) {
      handler = this.messageHandler;
    } else {
      this.filterConfiguration.validate();
      AtomicInteger i = new AtomicInteger(0);
      this.filterConfiguration.filterValues.forEach(
          v ->
              this.subscriptionProperties.put(
                  SUBSCRIPTION_PROPERTY_FILTER_PREFIX + i.getAndIncrement(), v));
      this.subscriptionProperties.put(
          SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED,
          this.filterConfiguration.matchUnfiltered ? "true" : "false");
      final Predicate<Message> filter = this.filterConfiguration.filter;
      final MessageHandler delegate = this.messageHandler;
      handler =
          (context, message) -> {
            if (filter.test(message)) {
              delegate.handle(context, message);
            }
          };
    }

    handler = this.environment.observationCollector().subscribe(handler);

    Consumer consumer;
    if (this.stream != null) {
      consumer =
          new StreamConsumer(
              this.stream,
              this.offsetSpecification,
              handler,
              this.name,
              this.environment,
              trackingConfiguration,
              this.lazyInit,
              this.subscriptionListener,
              this.subscriptionProperties,
              this.consumerUpdateListener,
              this.flowConfiguration.strategy);
      environment.addConsumer((StreamConsumer) consumer);
    } else {
      if (Utils.isSac(this.subscriptionProperties)) {
        this.subscriptionProperties.put(Utils.SUBSCRIPTION_PROPERTY_SUPER_STREAM, this.superStream);
      }
      consumer =
          new SuperStreamConsumer(this, this.superStream, this.environment, trackingConfiguration);
    }
    return consumer;
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

  private static final class DefaultFilterConfiguration implements FilterConfiguration {

    private final StreamConsumerBuilder builder;
    private List<String> filterValues;
    private Predicate<Message> filter;
    private boolean matchUnfiltered = false;

    private DefaultFilterConfiguration(StreamConsumerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public FilterConfiguration values(String... filterValues) {
      if (filterValues == null || filterValues.length == 0) {
        throw new IllegalArgumentException("At least one filter value must be specified");
      }
      this.filterValues = Arrays.asList(filterValues);
      return this;
    }

    @Override
    public FilterConfiguration postFilter(Predicate<Message> filter) {
      this.filter = filter;
      return this;
    }

    @Override
    public FilterConfiguration matchUnfiltered() {
      this.matchUnfiltered = true;
      return this;
    }

    @Override
    public FilterConfiguration matchUnfiltered(boolean matchUnfiltered) {
      this.matchUnfiltered = matchUnfiltered;
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.builder;
    }

    private void validate() {
      if (this.filterValues == null || this.filter == null) {
        throw new IllegalArgumentException("Both filter values and the filter logic must be set");
      }
    }
  }

  private static class DefaultFlowConfiguration implements FlowConfiguration {

    private final ConsumerBuilder consumerBuilder;

    private DefaultFlowConfiguration(ConsumerBuilder consumerBuilder) {
      this.consumerBuilder = consumerBuilder;
    }

    private ConsumerFlowStrategy strategy = ConsumerFlowStrategy.creditOnChunkArrival(10);

    @Override
    public FlowConfiguration initialCredits(int initialCredits) {
      if (initialCredits <= 0) {
        throw new IllegalArgumentException("Credits must be positive");
      }
      this.strategy = ConsumerFlowStrategy.creditOnChunkArrival(initialCredits);
      return this;
    }

    @Override
    public FlowConfiguration strategy(ConsumerFlowStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    @Override
    public ConsumerBuilder builder() {
      return this.consumerBuilder;
    }
  }

  // to help testing
  public ConsumerUpdateListener consumerUpdateListener() {
    return consumerUpdateListener;
  }
}

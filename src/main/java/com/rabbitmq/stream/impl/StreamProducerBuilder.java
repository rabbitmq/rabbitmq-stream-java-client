// Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.RoutingStrategy;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.compression.Compression;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.ToIntFunction;

class StreamProducerBuilder implements ProducerBuilder {

  static final boolean DEFAULT_DYNAMIC_BATCH =
      Boolean.parseBoolean(System.getProperty("rabbitmq.stream.producer.dynamic.batch", "false"));

  private final StreamEnvironment environment;

  private String name;

  private String stream, superStream;

  private int subEntrySize = 1;

  private Compression compression;

  private int batchSize = 100;

  private Duration batchPublishingDelay = Duration.ofMillis(100);

  private int maxUnconfirmedMessages = 10_000;

  private Duration confirmTimeout = Duration.ofSeconds(30);

  private Duration enqueueTimeout = Duration.ofSeconds(10);

  private boolean retryOnRecovery = true;

  private DefaultRoutingConfiguration routingConfiguration;

  private Function<Message, String> filterValueExtractor;

  private boolean dynamicBatch = DEFAULT_DYNAMIC_BATCH;

  StreamProducerBuilder(StreamEnvironment environment) {
    this.environment = environment;
  }

  public StreamProducerBuilder stream(String stream) {
    this.stream = stream;
    return this;
  }

  @Override
  public ProducerBuilder name(String name) {
    this.name = name;
    return this;
  }

  @Override
  public ProducerBuilder superStream(String superStream) {
    this.superStream = superStream;
    return this;
  }

  public StreamProducerBuilder batchSize(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("the batch size must be greater than 0");
    }
    this.batchSize = batchSize;
    return this;
  }

  @Override
  public ProducerBuilder subEntrySize(int subEntrySize) {
    if (subEntrySize < 0) {
      throw new IllegalArgumentException("the sub-entry size must be greater than 0");
    }
    this.subEntrySize = subEntrySize;
    return this;
  }

  @Override
  public ProducerBuilder compression(Compression compression) {
    this.compression = compression;
    return this;
  }

  @Override
  public StreamProducerBuilder batchPublishingDelay(Duration batchPublishingDelay) {
    this.batchPublishingDelay = batchPublishingDelay;
    return this;
  }

  @Override
  public ProducerBuilder dynamicBatch(boolean dynamicBatch) {
    this.dynamicBatch = dynamicBatch;
    return this;
  }

  @Override
  public ProducerBuilder maxUnconfirmedMessages(int maxUnconfirmedMessages) {
    if (maxUnconfirmedMessages <= 0) {
      throw new IllegalArgumentException(
          "the maximum number of unconfirmed messages must be greater than 0");
    }
    this.maxUnconfirmedMessages = maxUnconfirmedMessages;
    return this;
  }

  @Override
  public ProducerBuilder confirmTimeout(Duration timeout) {
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("the confirm timeout cannot be negative");
    }
    if (timeout.compareTo(Duration.ofSeconds(1)) < 0 && !timeout.isZero()) {
      throw new IllegalArgumentException("the confirm timeout cannot be less than 1 second");
    }
    this.confirmTimeout = timeout;
    return this;
  }

  @Override
  public ProducerBuilder enqueueTimeout(Duration timeout) {
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("the enqueue timeout cannot be negative");
    }
    this.enqueueTimeout = timeout;
    return this;
  }

  @Override
  public ProducerBuilder retryOnRecovery(boolean retryOnRecovery) {
    this.retryOnRecovery = retryOnRecovery;
    return this;
  }

  @Override
  public ProducerBuilder filterValue(Function<Message, String> filterValueExtractor) {
    this.filterValueExtractor = filterValueExtractor;
    return this;
  }

  @Override
  public RoutingConfiguration routing(Function<Message, String> routingKeyExtractor) {
    this.routingConfiguration = new DefaultRoutingConfiguration(this);
    this.routingConfiguration.routingKeyExtractor = routingKeyExtractor;
    return this.routingConfiguration;
  }

  void resetRouting() {
    this.routingConfiguration = null;
  }

  public Producer build() {
    if (this.stream == null && this.superStream == null) {
      throw new IllegalArgumentException("A stream must be specified");
    }
    if (this.stream != null && this.superStream != null) {
      throw new IllegalArgumentException("Stream and superStream cannot be set at the same time");
    }
    if (subEntrySize == 1 && compression != null) {
      throw new IllegalArgumentException(
          "Sub-entry batching must be enabled to enable compression");
    }
    if (subEntrySize > 1 && filterValueExtractor != null) {
      throw new IllegalArgumentException("Filtering is not supported with sub-entry batching");
    }
    if (subEntrySize > 1 && compression == null) {
      compression = Compression.NONE;
    }
    this.environment.maybeInitializeLocator();
    Producer producer;

    if (this.stream != null && this.routingConfiguration != null) {
      throw new IllegalArgumentException(
          "A super stream must be specified when a routing configuration is set");
    }

    if (this.routingConfiguration != null && this.superStream == null) {
      throw new IllegalArgumentException(
          "A super stream must be specified when a routing configuration is set");
    }

    if (this.routingConfiguration == null && this.superStream != null) {
      throw new IllegalArgumentException(
          "A routing configuration must specified when a super stream is set");
    }

    if (this.stream != null) {
      producer =
          new StreamProducer(
              name,
              stream,
              subEntrySize,
              batchSize,
              dynamicBatch,
              compression,
              batchPublishingDelay,
              maxUnconfirmedMessages,
              confirmTimeout,
              enqueueTimeout,
              retryOnRecovery,
              filterValueExtractor,
              environment);
      this.environment.addProducer((StreamProducer) producer);
    } else {
      RoutingStrategy routingStrategy = this.routingConfiguration.routingStrategy;
      if (routingStrategy == null) {
        if (this.routingConfiguration.hash == null) {
          routingStrategy =
              new RoutingKeyRoutingStrategy(this.routingConfiguration.routingKeyExtractor);
        } else {
          routingStrategy =
              new HashRoutingStrategy(
                  this.routingConfiguration.routingKeyExtractor, this.routingConfiguration.hash);
        }
      }
      producer =
          new SuperStreamProducer(
              this, this.name, this.superStream, routingStrategy, this.environment);
    }
    return producer;
  }

  StreamProducerBuilder duplicate() {
    StreamProducerBuilder duplicate = new StreamProducerBuilder(this.environment);
    for (Field field : StreamProducerBuilder.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        field.setAccessible(true);
        try {
          field.set(duplicate, field.get(this));
        } catch (IllegalAccessException e) {
          throw new StreamException("Error while duplicating stream producer builder", e);
        }
      }
    }
    return duplicate;
  }

  static final class DefaultRoutingConfiguration implements RoutingConfiguration {

    private final StreamProducerBuilder producerBuilder;

    private Function<Message, String> routingKeyExtractor;

    private RoutingStrategy routingStrategy;

    private ToIntFunction<String> hash = HashUtils.MURMUR3;

    DefaultRoutingConfiguration(StreamProducerBuilder producerBuilder) {
      this.producerBuilder = producerBuilder;
    }

    @Override
    public RoutingConfiguration hash() {
      if (this.hash == null) {
        this.hash = HashUtils.MURMUR3;
      }
      this.routingStrategy = null;
      return this;
    }

    @Override
    public RoutingConfiguration hash(ToIntFunction<String> hash) {
      this.hash = hash;
      this.routingStrategy = null;
      return this;
    }

    @Override
    public RoutingConfiguration key() {
      this.hash = null;
      this.routingStrategy = null;
      return this;
    }

    @Override
    public RoutingConfiguration strategy(RoutingStrategy routingStrategy) {
      this.routingStrategy = routingStrategy;
      return this;
    }

    @Override
    public ProducerBuilder producerBuilder() {
      return this.producerBuilder;
    }
  }
}

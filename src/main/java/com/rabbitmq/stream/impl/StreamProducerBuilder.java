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

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.ToIntFunction;

class StreamProducerBuilder implements ProducerBuilder {

  private final StreamEnvironment environment;

  private String name;

  private String stream;

  private int subEntrySize = 1;

  private int batchSize = 100;

  private Duration batchPublishingDelay = Duration.ofMillis(100);

  private int maxUnconfirmedMessages = 10_000;

  private Duration confirmTimeout = Duration.ofSeconds(30);

  private Duration enqueueTimeout = Duration.ofSeconds(10);

  private Function<Message, String> routingKeyExtractor;

  private RoutingType routingType;

  private ToIntFunction<String> hash = HashUtils.MURMUR3;

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

  public StreamProducerBuilder batchPublishingDelay(Duration batchPublishingDelay) {
    this.batchPublishingDelay = batchPublishingDelay;
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
    if (timeout.compareTo(Duration.ofSeconds(1)) < 0) {
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
  public ProducerBuilder routing(
      Function<Message, String> routingKeyExtractor, RoutingType routingType) {
    return this.routing(routingKeyExtractor, routingType, HashUtils.MURMUR3);
  }

  @Override
  public ProducerBuilder routing(
      Function<Message, String> routingKeyExtractor,
      RoutingType routingType,
      ToIntFunction<String> hash) {
    if ((routingKeyExtractor == null && routingType == null)
        || (routingKeyExtractor != null && routingType != null)) {
      this.routingKeyExtractor = routingKeyExtractor;
      this.routingType = routingType;
    } else {
      throw new IllegalArgumentException(
          "both routing key extractor and routing type must be " + "non-null");
    }
    return this;
  }

  public Producer build() {
    Producer producer;
    if (this.routingKeyExtractor == null) {
      producer =
          new StreamProducer(
              name,
              stream,
              subEntrySize,
              batchSize,
              batchPublishingDelay,
              maxUnconfirmedMessages,
              confirmTimeout,
              enqueueTimeout,
              environment);
      this.environment.addProducer((StreamProducer) producer);
    } else {
      ToIntFunction<String> hashFunction = this.hash == null ? HashUtils.MURMUR3 : this.hash;
      RoutingStrategy routingStrategy =
          this.routingType == RoutingType.HASH
              ? new HashRoutingStrategy(
                  this.stream, this.routingKeyExtractor, this.environment, hashFunction)
              : new RoutingKeyRoutingStrategy(
                  this.stream, this.routingKeyExtractor, this.environment);
      producer = new SuperStreamProducer(this, stream, routingStrategy, environment);
    }
    return producer;
  }

  StreamProducerBuilder duplicate() {
    StreamProducerBuilder duplicate = new StreamProducerBuilder(this.environment);
    for (Field field : StreamProducerBuilder.class.getDeclaredFields()) {
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

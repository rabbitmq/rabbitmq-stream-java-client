// Copyright (c) 2021-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Utils.namedFunction;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.RoutingStrategy.Metadata;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperStreamProducer implements Producer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamProducer.class);

  private final RoutingStrategy routingStrategy;
  private final Codec codec;
  private final String superStream;
  private final Map<String, Producer> producers = new ConcurrentHashMap<>();
  private final StreamProducerBuilder producerBuilder;
  private final StreamEnvironment environment;
  private final String name;
  private final Metadata superStreamMetadata;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * passthrough, except when observation collector is not no-op and a message must be sent to
   * several streams. In this it creates a copy of the message with distinct message annotations for
   * each message, so that the collector can populate these messages annotations without collision
   */
  private final MessageInterceptor messageInterceptor;

  SuperStreamProducer(
      StreamProducerBuilder producerBuilder,
      String name,
      String superStream,
      RoutingStrategy routingStrategy,
      StreamEnvironment streamEnvironment) {
    this.routingStrategy = routingStrategy;
    this.codec = streamEnvironment.codec();
    this.name = name;
    this.superStream = superStream;
    this.environment = streamEnvironment;
    this.superStreamMetadata = new DefaultSuperStreamMetadata(this.superStream, this.environment);
    this.producerBuilder = producerBuilder.duplicate();
    this.producerBuilder.stream(null);
    this.producerBuilder.resetRouting();
    this.messageInterceptor =
        environment.observationCollector().isNoop()
            ? (i, msg) -> msg
            : (i, msg) -> i == 0 ? msg : msg.copy();
  }

  @Override
  public MessageBuilder messageBuilder() {
    return codec.messageBuilder();
  }

  @Override
  public long getLastPublishingId() {
    if (this.name != null && !this.name.isEmpty()) {
      List<String> streams =
          this.environment.locatorOperation(
              namedFunction(
                  c -> c.partitions(superStream),
                  "Partition lookup for super stream '%s'",
                  this.superStream));
      long publishingId = 0;
      boolean first = true;
      for (String partition : streams) {
        long pubId =
            this.environment.locatorOperation(
                namedFunction(
                    c -> c.queryPublisherSequence(this.name, partition),
                    "Publisher sequence query for on partition '%s' of super stream '%s', publisher name '%s'",
                    partition,
                    this.superStream,
                    this.name));
        if (first) {
          publishingId = pubId;
          first = false;
        } else {
          if (Long.compareUnsigned(publishingId, pubId) > 0) {
            publishingId = pubId;
          }
        }
      }
      return publishingId;
    } else {
      throw new IllegalStateException("The producer has no name");
    }
  }

  @Override
  public void send(Message message, ConfirmationHandler confirmationHandler) {
    if (canSend()) {
      List<String> streams = this.routingStrategy.route(message, superStreamMetadata);
      if (streams.isEmpty()) {
        confirmationHandler.handle(
            new ConfirmationStatus(message, false, Constants.CODE_NO_ROUTE_FOUND));
      } else if (streams.size() == 1) {
        producer(streams.get(0)).send(message, confirmationHandler);
      } else {
        for (int i = 0; i < streams.size(); i++) {
          Producer producer = producer(streams.get(i));
          producer(streams.get(i)).send(messageInterceptor.apply(i, message), confirmationHandler);
        }
      }
    } else {
      confirmationHandler.handle(
          new ConfirmationStatus(message, false, Constants.CODE_PRODUCER_CLOSED));
    }
  }

  private Producer producer(String stream) {
    return producers.computeIfAbsent(
        stream,
        stream1 -> {
          Producer p = producerBuilder.duplicate().superStream(null).stream(stream1).build();
          return p;
        });
  }

  private boolean canSend() {
    return !this.closed.get();
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      for (Entry<String, Producer> entry : producers.entrySet()) {
        try {
          entry.getValue().close();
        } catch (Exception e) {
          LOGGER.info(
              "Error while closing producer for partition {} of super stream {}: {}",
              entry.getKey(),
              this.superStream,
              e.getMessage());
        }
      }
    }
  }

  private static final class DefaultSuperStreamMetadata implements Metadata {

    private final String superStream;
    private final StreamEnvironment environment;
    private final List<String> partitions;
    private final Map<String, List<String>> routes = new ConcurrentHashMap<>();

    private DefaultSuperStreamMetadata(String superStream, StreamEnvironment environment) {
      this.superStream = superStream;
      this.environment = environment;
      List<String> ps =
          environment.locatorOperation(
              namedFunction(
                  c -> c.partitions(superStream),
                  "Partition lookup for super stream '%s'",
                  superStream));
      if (ps.isEmpty()) {
        throw new IllegalArgumentException("Super stream '" + superStream + "' has no partition");
      }
      this.partitions = new CopyOnWriteArrayList<>(ps);
    }

    @Override
    public List<String> partitions() {
      return partitions;
    }

    @Override
    public List<String> route(String routingKey) {
      return routes.computeIfAbsent(
          routingKey,
          routingKey1 ->
              environment.locatorOperation(
                  namedFunction(
                      c -> c.route(routingKey1, superStream),
                      "Route lookup on super stream '%s' for key '%s'",
                      this.superStream,
                      routingKey1)));
    }
  }

  @FunctionalInterface
  private interface MessageInterceptor {

    Message apply(int partitionIndex, Message message);
  }
}

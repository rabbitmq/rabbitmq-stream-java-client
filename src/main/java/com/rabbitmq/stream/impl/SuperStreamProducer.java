// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Producer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperStreamProducer implements Producer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamProducer.class);

  private final RoutingStrategy routingStrategy;
  private final Codec codec;
  private final String superStream;
  private final Map<String, Producer> producers = new ConcurrentHashMap<>();
  private final StreamProducerBuilder producerBuilder;

  SuperStreamProducer(
      StreamProducerBuilder producerBuilder,
      String superStream,
      RoutingStrategy routingStrategy,
      StreamEnvironment streamEnvironment) {
    this.routingStrategy = routingStrategy;
    this.codec = streamEnvironment.codec();
    this.superStream = superStream;
    this.producerBuilder = producerBuilder.duplicate();
    this.producerBuilder.stream(null);
    this.producerBuilder.resetRouting();
  }

  @Override
  public MessageBuilder messageBuilder() {
    return codec.messageBuilder();
  }

  @Override
  public long getLastPublishingId() {
    // TODO get all partitions for this super stream, query the last publishing ID for each of team,
    // return the highest (or the lowest, because duplicates will be filtered out anyway?)
    return 0;
  }

  @Override
  public void send(Message message, ConfirmationHandler confirmationHandler) {
    // TODO handle when the stream is not found (no partition found for the message)
    // and call the confirmation handler with a failure
    String stream = this.routingStrategy.route(message);
    Producer producer =
        producers.computeIfAbsent(
            stream, stream1 -> producerBuilder.duplicate().stream(stream1).build());
    producer.send(message, confirmationHandler);
  }

  @Override
  public void close() {
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

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

import com.rabbitmq.stream.Consumer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperStreamConsumer implements Consumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamConsumer.class);
  private final String superStream;
  private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();

  SuperStreamConsumer(
      StreamConsumerBuilder builder, String superStream, StreamEnvironment environment) {
    this.superStream = superStream;
    for (String partition : environment.locator().partitions(superStream)) {
      Consumer consumer = builder.duplicate().superStream(null).stream(partition).build();
      consumers.put(partition, consumer);
      LOGGER.debug("Created consumer on stream '{}' for super stream '{}'", partition, superStream);
    }
  }

  @Override
  public void store(long offset) {
    throw new UnsupportedOperationException(
        "Consumer#store(long) does not work for super streams, use MessageHandler.Context#storeOffset() instead");
  }

  @Override
  public void close() {
    for (Entry<String, Consumer> entry : consumers.entrySet()) {
      LOGGER.debug(
          "Closing consumer for partition '{}' of super stream {}",
          entry.getKey(),
          this.superStream);
      try {
        entry.getValue().close();
      } catch (Exception e) {
        LOGGER.info(
            "Error while closing consumer for partition {} of super stream {}: {}",
            entry.getKey(),
            this.superStream,
            e.getMessage());
      }
    }
  }
}

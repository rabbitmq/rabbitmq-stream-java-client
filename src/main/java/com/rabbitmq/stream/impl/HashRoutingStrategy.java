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

import com.rabbitmq.stream.Message;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

class HashRoutingStrategy implements RoutingStrategy {

  private final Function<Message, String> routingKeyExtractor;

  private final StreamEnvironment env;

  private final String superStream;

  private final List<String> partitions;

  HashRoutingStrategy(
      String superStream, Function<Message, String> routingKeyExtractor, StreamEnvironment env) {
    this.routingKeyExtractor = routingKeyExtractor;
    this.env = env;
    this.superStream = superStream;
    // TODO use async retry to get locator
    List<String> ps = this.env.locator().partitions(superStream);
    this.partitions = new CopyOnWriteArrayList<>(ps);
  }

  @Override
  public String route(Message message) {
    String routingKey = routingKeyExtractor.apply(message);
    // TODO use hash function like murmur or xxHash to hash routing key
    int hash = routingKey.hashCode();
    return this.partitions.get((hash & 0x7FFFFFFF) % this.partitions.size());
  }
}

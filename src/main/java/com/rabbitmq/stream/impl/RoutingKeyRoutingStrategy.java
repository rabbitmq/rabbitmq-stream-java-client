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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class RoutingKeyRoutingStrategy implements RoutingStrategy {

  private final Function<Message, String> routingKeyExtractor;

  private final Map<String, String> routingKeysToStreams = new ConcurrentHashMap<>();

  private final StreamEnvironment env;

  private final String superStream;

  RoutingKeyRoutingStrategy(
      String superStream, Function<Message, String> routingKeyExtractor, StreamEnvironment env) {
    this.routingKeyExtractor = routingKeyExtractor;
    this.env = env;
    this.superStream = superStream;
  }

  @Override
  public String route(Message message) {
    String routingKey = this.routingKeyExtractor.apply(message);
    String stream =
        routingKeysToStreams.computeIfAbsent(
            routingKey,
            routingKey1 -> {
              // TODO retry on locator lookup
              return env.locator().route(routingKey1, superStream);
            });
    return stream;
  }
}

// Copyright (c) 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import com.rabbitmq.stream.RoutingStrategy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class RoutingKeyRoutingStrategy implements RoutingStrategy {

  private final Function<Message, String> routingKeyExtractor;

  private final Map<String, List<String>> routingKeysToStreams = new ConcurrentHashMap<>();

  RoutingKeyRoutingStrategy(Function<Message, String> routingKeyExtractor) {
    this.routingKeyExtractor = routingKeyExtractor;
  }

  @Override
  public List<String> route(Message message, Metadata metadata) {
    String routingKey = this.routingKeyExtractor.apply(message);
    List<String> streams =
        routingKeysToStreams.computeIfAbsent(
            routingKey, routingKey1 -> metadata.route(routingKey1));
    return streams;
  }
}

// Copyright (c) 2021-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
// Inc. and/or its subsidiaries.
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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;

class HashRoutingStrategy implements RoutingStrategy {

  private final Function<Message, String> routingKeyExtractor;

  private final ToIntFunction<String> hash;

  HashRoutingStrategy(Function<Message, String> routingKeyExtractor, ToIntFunction<String> hash) {
    this.routingKeyExtractor = routingKeyExtractor;
    this.hash = hash;
  }

  @Override
  public List<String> route(Message message, Metadata metadata) {
    List<String> partitions = metadata.partitions();
    if (partitions.isEmpty()) {
      return Collections.emptyList();
    } else {
      String routingKey = routingKeyExtractor.apply(message);
      int hashValue = hash.applyAsInt(routingKey);
      return Collections.singletonList(
          partitions.get(Integer.remainderUnsigned(hashValue, partitions.size())));
    }
  }
}

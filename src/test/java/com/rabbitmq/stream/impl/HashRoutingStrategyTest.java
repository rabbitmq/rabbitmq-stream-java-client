// Copyright (c) 2022-2023 Broadcom. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.RoutingStrategy;
import com.rabbitmq.stream.RoutingStrategy.Metadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class HashRoutingStrategyTest {

  @Test
  void routeShouldComputeAppropriatePartitions() {
    List<String> keys =
        IntStream.range(1, 11).mapToObj(i -> "hello" + i).collect(Collectors.toList());
    RoutingStrategy routingStrategy =
        new HashRoutingStrategy(
            new Function<Message, String>() {
              int count = 0;

              @Override
              public String apply(Message message) {
                return keys.get(count++);
              }
            },
            HashUtils.MURMUR3);

    Map<String, String> expectedRoutes =
        new HashMap<String, String>() {
          {
            put("hello1", "invoices-02");
            put("hello2", "invoices-01");
            put("hello3", "invoices-02");
            put("hello4", "invoices-03");
            put("hello5", "invoices-01");
            put("hello6", "invoices-03");
            put("hello7", "invoices-01");
            put("hello8", "invoices-02");
            put("hello9", "invoices-01");
            put("hello10", "invoices-03");
          }
        };

    Metadata metadata =
        new MetadataAdapter() {
          List<String> partitions = Arrays.asList("invoices-01", "invoices-02", "invoices-03");

          @Override
          public List<String> partitions() {
            return partitions;
          }
        };
    for (String key : keys) {
      List<String> partitions = routingStrategy.route(null, metadata);
      assertThat(partitions).hasSize(1).element(0).isEqualTo(expectedRoutes.get(key));
    }
  }

  @Test
  void shouldReturnEmptyListIfNoPartition() {
    RoutingStrategy routingStrategy = new HashRoutingStrategy(m -> "rk", HashUtils.MURMUR3);
    Metadata metadata =
        new MetadataAdapter() {
          @Override
          public List<String> partitions() {
            return Collections.emptyList();
          }
        };
    assertThat(routingStrategy.route(null, metadata)).isEmpty();
  }

  private static class MetadataAdapter implements Metadata {

    @Override
    public List<String> partitions() {
      return null;
    }

    @Override
    public List<String> route(String routingKey) {
      return null;
    }
  }
}

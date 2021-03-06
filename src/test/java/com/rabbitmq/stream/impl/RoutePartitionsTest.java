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

import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class RoutePartitionsTest {

  TestUtils.ClientFactory cf;

  Connection connection;

  int partitions = 3;
  String superStream;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    connection = new ConnectionFactory().newConnection();
    superStream = TestUtils.streamName(info);
  }

  @AfterEach
  void tearDown() throws Exception {
    deleteSuperStreamTopology(connection, superStream, partitions);
    connection.close();
  }

  @Test
  void routeShouldReturnNullWhenExchangeDoesNotExist() {
    assertThat(cf.get().route("", UUID.randomUUID().toString())).isNull();
  }

  @Test
  void partitionsShouldReturnEmptyListWhenExchangeDoesNotExist() {
    assertThat((cf.get()).partitions(UUID.randomUUID().toString())).isEmpty();
  }

  @Test
  void routeShouldReturnNullWhenNoStreamForRoutingKey() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitions);

    Client client = cf.get();
    assertThat(client.route("0", superStream)).isEqualTo(superStream + "-0");
    assertThat(client.route("42", superStream)).isNull();
  }

  @Test
  void partitionsShouldReturnEmptyListWhenThereIsNoBinding() throws Exception {
    declareSuperStreamTopology(connection, superStream, 0);

    Client client = cf.get();
    assertThat(client.partitions(superStream)).isEmpty();
  }

  @Test
  void routeTopologyWithPartitionCount() throws Exception {
    declareSuperStreamTopology(connection, superStream, 3);

    Client client = cf.get();
    List<String> streams = client.partitions(superStream);
    assertThat(streams)
        .hasSize(partitions)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, partitions).mapToObj(i -> superStream + "-" + i).collect(toList()));
    assertThat(client.route("0", superStream)).isEqualTo(superStream + "-0");
    assertThat(client.route("1", superStream)).isEqualTo(superStream + "-1");
    assertThat(client.route("2", superStream)).isEqualTo(superStream + "-2");
  }
}

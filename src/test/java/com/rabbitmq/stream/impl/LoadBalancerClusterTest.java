// Copyright (c) 2024 Broadcom. All Rights Reserved.
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

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ConsumerCoordinatorInfo;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfNotCluster;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@DisabledIfNotCluster
@StreamTestInfrastructure
public class LoadBalancerClusterTest {

  private static final int LB_PORT = 5555;
  private static final SubscriptionListener NO_OP_SUBSCRIPTION_LISTENER = subscriptionContext -> {};
  private static final Runnable NO_OP_TRACKING_CLOSING_CALLBACK = () -> {};

  @Mock StreamEnvironment environment;
  @Mock StreamConsumer consumer;
  AutoCloseable mocks;
  TestUtils.ClientFactory cf;
  String stream;
  EventLoopGroup eventLoopGroup;
  Client locator;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    locator = cf.get(new Client.ClientParameters().port(LB_PORT));
    when(environment.locator()).thenReturn(locator);
    when(environment.clientParametersCopy())
        .thenReturn(new Client.ClientParameters().eventLoopGroup(eventLoopGroup).port(LB_PORT));
    Address loadBalancerAddress = new Address("localhost", LB_PORT);
    when(environment.addressResolver()).thenReturn(address -> loadBalancerAddress);
    when(environment.locatorOperation(any())).thenCallRealMethod();
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pickConsumersAmongCandidates(boolean forceReplica) {
    int maxSubscriptionsPerClient = 2;
    int subscriptionCount = maxSubscriptionsPerClient * 10;
    try (ConsumersCoordinator c =
        new ConsumersCoordinator(
            environment,
            maxSubscriptionsPerClient,
            type -> "consumer-connection",
            Utils.coordinatorClientFactory(this.environment, Duration.ofMillis(10)),
            forceReplica,
            Utils.brokerPicker())) {

      IntStream.range(0, subscriptionCount)
          .forEach(
              ignored -> {
                c.subscribe(
                    consumer,
                    stream,
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap(),
                    flowStrategy());
              });

      Client.StreamMetadata metadata = locator.metadata(stream).get(stream);
      Set<Client.Broker> allowedNodes = new HashSet<>(metadata.getReplicas());
      if (!forceReplica) {
        allowedNodes.add(metadata.getLeader());
      }

      ConsumerCoordinatorInfo info = MonitoringTestUtils.extract(c);
      assertThat(info.consumerCount()).isEqualTo(subscriptionCount);
      Set<Client.Broker> usedNodes =
          info.clients().stream()
              .map(m -> m.node().split(":"))
              .map(np -> new Client.Broker(np[0], parseInt(np[1])))
              .collect(toSet());
      assertThat(usedNodes).hasSameSizeAs(allowedNodes).containsAll(allowedNodes);
    }
  }

  private static ConsumerFlowStrategy flowStrategy() {
    return ConsumerFlowStrategy.creditOnChunkArrival(10);
  }
}

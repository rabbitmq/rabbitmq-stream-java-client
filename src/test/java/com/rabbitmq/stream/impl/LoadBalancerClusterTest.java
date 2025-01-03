// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ConsumerCoordinatorInfo;
import com.rabbitmq.stream.impl.MonitoringTestUtils.EnvironmentInfo;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ProducersCoordinatorInfo;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfNotCluster;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  @Mock StreamProducer producer;
  AutoCloseable mocks;
  TestUtils.ClientFactory cf;
  String stream;
  EventLoopGroup eventLoopGroup;
  Client locator;
  static final Address LOAD_BALANCER_ADDRESS = new Address("localhost", LB_PORT);

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    locator = cf.get(new Client.ClientParameters().port(LB_PORT));
    StreamEnvironment.Locator l = new StreamEnvironment.Locator(-1, new Address("localhost", 5555));
    l.client(locator);
    when(environment.locator()).thenReturn(l);
    when(environment.clientParametersCopy())
        .thenReturn(new Client.ClientParameters().eventLoopGroup(eventLoopGroup).port(LB_PORT));
    when(environment.addressResolver()).thenReturn(address -> LOAD_BALANCER_ADDRESS);
    when(environment.locatorOperation(any())).thenCallRealMethod();
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pickConsumersAmongCandidates(boolean forceReplica) throws Exception {
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

      waitAtMost(
          () -> locator.metadata(stream).get(stream).hasReplicas(),
          "Stream '%s' should have replicas");

      range(0, subscriptionCount)
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
      Set<Broker> allowedNodes = new HashSet<>(metadata.getReplicas());
      if (!forceReplica) {
        allowedNodes.add(metadata.getLeader());
      }

      ConsumerCoordinatorInfo info = MonitoringTestUtils.extract(c);
      assertThat(info.consumerCount()).isEqualTo(subscriptionCount);
      Set<Broker> usedNodes =
          info.clients().stream()
              .map(m -> m.node().split(":"))
              .map(np -> new Broker(np[0], parseInt(np[1])))
              .collect(toSet());
      assertThat(usedNodes).hasSameSizeAs(allowedNodes).containsAll(allowedNodes);
    }
  }

  @Test
  void pickProducersAmongCandidatesIfInstructed() {
    boolean forceLeader = true;
    when(consumer.stream()).thenReturn(stream);
    int maxAgentPerClient = 2;
    int agentCount = maxAgentPerClient * 10;
    try (ProducersCoordinator c =
        new ProducersCoordinator(
            environment,
            maxAgentPerClient,
            maxAgentPerClient,
            type -> "producer-connection",
            Utils.coordinatorClientFactory(this.environment, Duration.ofMillis(10)),
            forceLeader)) {

      range(0, agentCount)
          .forEach(
              ignored -> {
                c.registerProducer(producer, null, stream);
                c.registerTrackingConsumer(consumer);
              });

      Client.StreamMetadata metadata = locator.metadata(stream).get(stream);
      Set<Broker> allowedNodes = new HashSet<>(Collections.singleton(metadata.getLeader()));
      if (!forceLeader) {
        allowedNodes.addAll(metadata.getReplicas());
      }

      ProducersCoordinatorInfo info = MonitoringTestUtils.extract(c);
      assertThat(info.producerCount()).isEqualTo(agentCount);
      assertThat(info.trackingConsumerCount()).isEqualTo(agentCount);
      Set<Broker> usedNodes =
          info.nodesConnected().stream()
              .map(n -> n.split(":"))
              .map(np -> new Broker(np[0], parseInt(np[1])))
              .collect(toSet());
      assertThat(usedNodes).hasSameSizeAs(allowedNodes).containsAll(allowedNodes);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void producersConsumersShouldSpreadAccordingToDataLocalitySettings(boolean forceLocality) {
    int maxPerConnection = 2;
    int agentCount = maxPerConnection * 20;
    StreamEnvironmentBuilder builder = (StreamEnvironmentBuilder) Environment.builder();
    builder
        .producerNodeRetryDelay(Duration.ofMillis(10))
        .consumerNodeRetryDelay(Duration.ofMillis(10));
    try (Environment env =
        builder
            .port(LB_PORT)
            .forceReplicaForConsumers(forceLocality)
            .forceReplicaForConsumers(forceLocality)
            .addressResolver(addr -> LOAD_BALANCER_ADDRESS)
            .maxProducersByConnection(maxPerConnection)
            .maxConsumersByConnection(maxPerConnection)
            .forceLeaderForProducers(forceLocality)
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder()
            .build()) {
      TestUtils.Sync consumeSync = TestUtils.sync(agentCount * agentCount);
      Set<Integer> consumersThatReceived = ConcurrentHashMap.newKeySet(agentCount);
      List<Producer> producers = new ArrayList<>();
      range(0, agentCount)
          .forEach(
              index -> {
                producers.add(env.producerBuilder().stream(stream).build());
                env.consumerBuilder().stream(stream)
                    .messageHandler(
                        (ctx, msg) -> {
                          consumersThatReceived.add(index);
                          consumeSync.down();
                        })
                    .offset(OffsetSpecification.first())
                    .build();
              });
      producers.forEach(p -> p.send(p.messageBuilder().build(), ctx -> {}));
      assertThat(consumeSync).completes();
      assertThat(consumersThatReceived).containsAll(range(0, agentCount).boxed().collect(toSet()));

      EnvironmentInfo info = MonitoringTestUtils.extract(env);
      ProducersCoordinatorInfo producerInfo = info.getProducers();
      ConsumerCoordinatorInfo consumerInfo = info.getConsumers();

      assertThat(producerInfo.producerCount()).isEqualTo(agentCount);
      assertThat(consumerInfo.consumerCount()).isEqualTo(agentCount);

      Client.StreamMetadata metadata = locator.metadata(stream).get(stream);

      Function<Collection<String>, Set<Broker>> toBrokers =
          nodes ->
              nodes.stream()
                  .map(n -> n.split(":"))
                  .map(n -> new Broker(n[0], parseInt(n[1])))
                  .collect(toSet());
      Set<Broker> usedNodes = toBrokers.apply(producerInfo.nodesConnected());
      assertThat(usedNodes).contains(metadata.getLeader());
      if (forceLocality) {
        assertThat(usedNodes).hasSize(1);
      } else {
        assertThat(usedNodes).hasSize(metadata.getReplicas().size() + 1);
        assertThat(usedNodes).containsAll(metadata.getReplicas());
      }

      usedNodes = toBrokers.apply(consumerInfo.nodesConnected());
      assertThat(usedNodes).containsAll(metadata.getReplicas());
      if (forceLocality) {
        assertThat(usedNodes).hasSameSizeAs(metadata.getReplicas());
      } else {
        assertThat(usedNodes).hasSize(metadata.getReplicas().size() + 1);
        assertThat(usedNodes).contains(metadata.getLeader());
      }
    }
  }

  private static ConsumerFlowStrategy flowStrategy() {
    return ConsumerFlowStrategy.creditOnChunkArrival(10);
  }
}

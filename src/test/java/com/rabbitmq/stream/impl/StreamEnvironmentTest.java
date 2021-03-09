// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.MonitoringTestUtils.EnvironmentInfo;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamEnvironmentTest {

  static EventLoopGroup eventLoopGroup;

  EnvironmentBuilder environmentBuilder;

  String stream;

  @BeforeAll
  static void initAll() {
    eventLoopGroup = new NioEventLoopGroup();
  }

  @AfterAll
  static void afterAll() throws Exception {
    eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
  }

  @BeforeEach
  void init() {
    environmentBuilder = Environment.builder();
    ((StreamEnvironmentBuilder) environmentBuilder).hostResolver(h -> "localhost");
    environmentBuilder.eventLoopGroup(eventLoopGroup);
  }

  @Test
  void environmentCreationShouldFailWithIncorrectCredentialsInUri() {
    assertThatThrownBy(
            () ->
                environmentBuilder.uri("rabbitmq-stream://bad:credentials@localhost:5551").build())
        .isInstanceOf(AuthenticationFailureException.class);
  }

  @Test
  void environmentCreationShouldFailWithIncorrectVirtualHostInUri() {
    assertThatThrownBy(
            () ->
                environmentBuilder
                    .uri("rabbitmq-stream://guest:guest@localhost:5551/dummy")
                    .build())
        .isInstanceOf(StreamException.class)
        .hasMessageContaining(String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE));
  }

  @Test
  void environmentCreationShouldFailWithUrlUsingWrongPort() {
    assertThatThrownBy(
            () ->
                environmentBuilder
                    .uri("rabbitmq-stream://guest:guest@localhost:4242/%2f")
                    .build()
                    .close())
        .hasMessageContaining("Connection refused");
  }

  @Test
  void environmentCreationShouldFailWhenUrlHasNoScheme() {
    assertThatThrownBy(
            () -> environmentBuilder.uri("guest:guest@localhost:5551/%2f").build().close())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scheme")
        .hasMessageContaining("rabbitmq-stream");
  }

  @Test
  void environmentCreationShouldSucceedWithUrlContainingAllCorrectInformation() {
    environmentBuilder.uri("guest:guest@localhost:5551/%2f").build().close();
  }

  @Test
  void producersAndConsumersShouldBeClosedWhenEnvironmentIsClosed() {
    Environment environment = environmentBuilder.build();
    Collection<Producer> producers =
        IntStream.range(0, 2)
            .mapToObj(i -> environment.producerBuilder().stream(stream).build())
            .collect(Collectors.toList());
    Collection<Consumer> consumers =
        IntStream.range(0, 2)
            .mapToObj(
                i ->
                    environment.consumerBuilder().stream(stream)
                        .name(UUID.randomUUID().toString())
                        .messageHandler((offset, message) -> {})
                        .build())
            .collect(Collectors.toList());

    producers.forEach(producer -> assertThat(((StreamProducer) producer).isOpen()).isTrue());
    consumers.forEach(consumer -> assertThat(((StreamConsumer) consumer).isOpen()).isTrue());

    EnvironmentInfo environmentInfo = MonitoringTestUtils.extract(environment);
    assertThat(environmentInfo.getLocator()).isNotNull();
    assertThat(environmentInfo.getProducers())
        .hasSize(1)
        .element(0)
        .extracting(pool -> pool.getClients())
        .asList()
        .hasSize(1);
    assertThat(environmentInfo.getProducers().get(0).getClients().get(0).getProducerCount())
        .isEqualTo(2);
    assertThat(
            environmentInfo.getProducers().get(0).getClients().get(0).getCommittingConsumerCount())
        .isEqualTo(2);
    assertThat(environmentInfo.getConsumers())
        .hasSize(1)
        .element(0)
        .extracting(pool -> pool.getClients())
        .asList()
        .hasSize(1);
    assertThat(environmentInfo.getConsumers().get(0).getClients().get(0).getConsumerCount())
        .isEqualTo(2);

    environment.close();

    producers.forEach(producer -> assertThat(((StreamProducer) producer).isOpen()).isFalse());
    consumers.forEach(consumer -> assertThat(((StreamConsumer) consumer).isOpen()).isFalse());

    environmentInfo = MonitoringTestUtils.extract(environment);
    assertThat(environmentInfo.getLocator()).isNull();
    assertThat(environmentInfo.getProducers()).isEmpty();
    assertThat(environmentInfo.getConsumers()).isEmpty();
  }

  @Test
  void growShrinkResourcesWhenProducersConsumersAreOpenedAndClosed(TestInfo info) throws Exception {
    int messageCount = 100;
    int streamCount = 20;
    int producersCount = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT * 3 + 10;
    int consumersCount = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT * 2 + 10;

    try (Environment environment = environmentBuilder.build()) {
      List<String> streams =
          IntStream.range(0, streamCount)
              .mapToObj(i -> streamName(info))
              .map(
                  s -> {
                    environment.streamCreator().stream(s).create();
                    return s;
                  })
              .collect(Collectors.toCollection(() -> new CopyOnWriteArrayList<>()));

      CountDownLatch confirmLatch = new CountDownLatch(messageCount * producersCount);
      CountDownLatch consumeLatch = new CountDownLatch(messageCount * producersCount);

      List<Producer> producers =
          IntStream.range(0, producersCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(i % streams.size());
                    return environment.producerBuilder().stream(s).build();
                  })
              .collect(Collectors.toList());

      List<Consumer> consumers =
          IntStream.range(0, consumersCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(new Random().nextInt(streams.size()));
                    return environment.consumerBuilder().stream(s)
                        .messageHandler((offset, message) -> consumeLatch.countDown())
                        .build();
                  })
              .collect(Collectors.toList());

      producers.stream()
          .parallel()
          .forEach(
              producer -> {
                IntStream.range(0, messageCount)
                    .forEach(
                        messageIndex -> {
                          producer.send(
                              producer.messageBuilder().addData("".getBytes()).build(),
                              confirmationStatus -> {
                                if (confirmationStatus.isConfirmed()) {
                                  confirmLatch.countDown();
                                }
                              });
                        });
              });

      assertThat(confirmLatch.await(10, SECONDS)).isTrue();
      assertThat(consumeLatch.await(10, SECONDS)).isTrue();

      EnvironmentInfo environmentInfo = MonitoringTestUtils.extract(environment);
      assertThat(environmentInfo.getProducers()).hasSize(1);
      int producerManagerCount = environmentInfo.getProducers().get(0).getClients().size();
      assertThat(producerManagerCount).isPositive();
      assertThat(environmentInfo.getConsumers()).hasSize(1);
      int consumerManagerCount = environmentInfo.getConsumers().get(0).getClients().size();
      assertThat(consumerManagerCount).isPositive();

      java.util.function.Consumer<AutoCloseable> closing =
          agent -> {
            try {
              agent.close();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
      Collections.reverse(producers);
      List<Producer> subProducers =
          producers.subList(0, ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT);
      subProducers.forEach(closing);

      Collections.reverse(consumers);
      List<Consumer> subConsumers =
          consumers.subList(0, ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT);
      subConsumers.forEach(closing);

      producers.removeAll(subProducers);
      consumers.removeAll(subConsumers);

      environmentInfo = MonitoringTestUtils.extract(environment);
      assertThat(environmentInfo.getProducers()).hasSize(1);
      assertThat(environmentInfo.getProducers().get(0).getClients())
          .hasSizeLessThan(producerManagerCount);
      assertThat(environmentInfo.getConsumers()).hasSize(1);
      assertThat(environmentInfo.getConsumers().get(0).getClients())
          .hasSizeLessThan(consumerManagerCount);

      producers.forEach(closing);
      consumers.forEach(closing);

      streams.stream().forEach(stream -> environment.deleteStream(stream));
    }
  }

  @Test
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void locatorShouldReconnectIfConnectionIsLost(TestInfo info) throws Exception {
    try (Environment environment =
        environmentBuilder
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)))
            .build()) {
      String s = streamName(info);
      environment.streamCreator().stream(s).create();
      environment.deleteStream(s);
      Host.killConnection("rabbitmq-stream-locator");
      assertThatThrownBy(() -> environment.streamCreator().stream("whatever").create())
          .isInstanceOf(StreamException.class);
      assertThatThrownBy(() -> environment.deleteStream("whatever"))
          .isInstanceOf(StreamException.class);
      assertThatThrownBy(() -> environment.producerBuilder().stream(stream).build())
          .isInstanceOf(StreamException.class);
      assertThatThrownBy(() -> environment.consumerBuilder().stream(stream).build())
          .isInstanceOf(StreamException.class);

      Producer producer = null;
      int timeout = 10_000;
      int waited = 0;
      int interval = 1_000;
      while (producer == null && waited < timeout) {
        try {
          Thread.sleep(interval);
          waited += interval;
          producer = environment.producerBuilder().stream(stream).build();
        } catch (StreamException e) {
        }
      }
      assertThat(producer).isNotNull();
    }
  }

  @Test
  void createDelete(TestInfo info) throws Exception {
    try (Environment environment = environmentBuilder.build();
        Client client = new Client()) {

      String s = streamName(info);
      environment.streamCreator().stream(s).create();
      Client.StreamMetadata metadata = client.metadata(s).get(s);
      assertThat(metadata.isResponseOk()).isTrue();
      environment.deleteStream(s);
      metadata = client.metadata(s).get(s);
      assertThat(metadata.isResponseOk()).isFalse();
      assertThat(metadata.getResponseCode())
          .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }
  }
}

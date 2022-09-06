// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.ExceptionConditions.responseCode;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.localhostTls;
import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ChannelCustomizer;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamStats;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import com.rabbitmq.stream.impl.MonitoringTestUtils.EnvironmentInfo;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslHandler;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamEnvironmentTest {

  static EventLoopGroup eventLoopGroup;

  EnvironmentBuilder environmentBuilder;

  String stream;
  TestUtils.ClientFactory cf;

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
    environmentBuilder.addressResolver(
        add -> add.port() == Client.DEFAULT_PORT ? localhost() : localhostTls());
    environmentBuilder.eventLoopGroup(eventLoopGroup);
  }

  @Test
  void environmentCreationShouldFailWithIncorrectCredentialsInUri() {
    assertThatThrownBy(
            () ->
                environmentBuilder.uri("rabbitmq-stream://bad:credentials@localhost:5552").build())
        .isInstanceOf(AuthenticationFailureException.class);
  }

  @Test
  void environmentCreationShouldFailWithIncorrectVirtualHostInUri() {
    assertThatThrownBy(
            () ->
                environmentBuilder
                    .uri("rabbitmq-stream://guest:guest@localhost:5552/dummy")
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
                    .addressResolver(address -> new Address("localhost", 4242))
                    .build()
                    .close())
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(ConnectException.class)
        .hasRootCauseMessage("Connection refused");
  }

  @Test
  void environmentCreationShouldFailWhenUrlHasNoScheme() {
    assertThatThrownBy(
            () -> environmentBuilder.uri("guest:guest@localhost:5552/%2f").build().close())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scheme")
        .hasMessageContaining("rabbitmq-stream");
  }

  @Test
  void environmentCreationShouldSucceedWithUrlContainingAllCorrectInformation() {
    environmentBuilder.uri("rabbitmq-stream://guest:guest@localhost:5552/%2f").build().close();
  }

  @DisabledIfTlsNotEnabled
  @Test
  void environmentCreationShouldSucceedWhenUsingTls() {
    ChannelCustomizer channelCustomizer =
        ch -> {
          SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
          if (sslHandler != null) {
            SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
            sslParameters.setServerNames(Collections.singletonList(new SNIHostName("localhost")));
            sslHandler.engine().setSSLParameters(sslParameters);
          }
        };
    environmentBuilder
        .uri("rabbitmq-stream+tls://guest:guest@localhost:5551/%2f")
        .channelCustomizer(channelCustomizer)
        .tls()
        .trustEverything()
        .environmentBuilder()
        .build()
        .close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void producersAndConsumersShouldBeClosedWhenEnvironmentIsClosed(boolean lazyInit) {
    Environment environment = environmentBuilder.lazyInitialization(lazyInit).build();
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
    assertThat(environmentInfo.getProducers().get(0).getClients().get(0).getTrackingConsumerCount())
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
  void environmentPublishersConsumersShouldCloseSuccessfullyWhenBrokerIsDown() throws Exception {
    Environment environment =
        environmentBuilder
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(10)))
            .build();
    CountDownLatch consumeLatch = new CountDownLatch(2);
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .messageHandler((context, message) -> consumeLatch.countDown())
            .build();
    // will be closed by the environment
    environment.consumerBuilder().stream(stream)
        .messageHandler((context, message) -> consumeLatch.countDown())
        .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    // will be closed by the environment
    environment.producerBuilder().stream(stream).build();

    producer.send(
        producer.messageBuilder().addData("".getBytes(StandardCharsets.UTF_8)).build(),
        confirmationStatus -> {});

    latchAssert(consumeLatch).completes();

    try {
      Host.rabbitmqctl("stop_app");
      producer.close();
      consumer.close();
      environment.close();
    } finally {
      Host.rabbitmqctl("start_app");
    }
    waitAtMost(
        30,
        () -> {
          Client client = cf.get();
          Map<String, StreamMetadata> metadata = client.metadata(stream);
          return metadata.containsKey(stream) && metadata.get(stream).isResponseOk();
        });
  }

  @Test
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void locatorShouldReconnectIfConnectionIsLost(TestInfo info) throws Exception {
    try (Environment environment =
        environmentBuilder
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(1)))
            .build()) {
      String s = streamName(info);
      environment.streamCreator().stream(s).create();
      environment.deleteStream(s);
      Host.killConnection("rabbitmq-stream-locator-0");
      environment.streamCreator().stream(s).create();
      try {
        Producer producer = environment.producerBuilder().stream(s).build();
        Consumer consumer =
            environment.consumerBuilder().stream(s)
                .messageHandler((context, message) -> {})
                .build();
        producer.close();
        consumer.close();
      } finally {
        environment.deleteStream(s);
      }
    }
  }

  @Test
  void createDelete(TestInfo info) {
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

  @Test
  void createStreamWithDifferentParametersShouldThrowException(TestInfo info) {
    String s = streamName(info);
    Client client = cf.get();
    try (Environment env = environmentBuilder.build()) {
      env.streamCreator().stream(s).maxAge(Duration.ofDays(1)).create();
      assertThatThrownBy(() -> env.streamCreator().stream(s).maxAge(Duration.ofDays(4)).create())
          .isInstanceOf(StreamException.class)
          .has(responseCode(Constants.RESPONSE_CODE_PRECONDITION_FAILED));
    } finally {
      assertThat(client.delete(s).isOk()).isTrue();
    }
  }

  @Test
  void streamCreationShouldBeIdempotent(TestInfo info) {
    String s = streamName(info);
    Client client = cf.get();
    try (Environment env = environmentBuilder.build()) {
      Duration retention = Duration.ofDays(4);
      env.streamCreator().stream(s).maxAge(retention).create();
      env.streamCreator().stream(s).maxAge(retention).create();
    } finally {
      assertThat(client.delete(s).isOk()).isTrue();
    }
  }

  @Test
  void instanciationShouldSucceedWhenLazyInitIsEnabledAndHostIsNotKnown() {
    String dummyHost = UUID.randomUUID().toString();
    Address dummyAddress = new Address(dummyHost, Client.DEFAULT_PORT);
    try (Environment env =
        environmentBuilder
            .host(dummyHost)
            .addressResolver(a -> dummyAddress)
            .lazyInitialization(true)
            .build()) {

      StreamCreator streamCreator = env.streamCreator().stream("should not have been created");
      assertThatThrownBy(() -> streamCreator.create()).isInstanceOf(StreamException.class);
      assertThatThrownBy(() -> env.deleteStream("should not exist"))
          .isInstanceOf(StreamException.class);
      ProducerBuilder producerBuilder = env.producerBuilder().stream(this.stream);
      assertThatThrownBy(() -> producerBuilder.build()).isInstanceOf(StreamException.class);
      ConsumerBuilder consumerBuilder =
          env.consumerBuilder().stream(this.stream).messageHandler((context, message) -> {});
      assertThatThrownBy(() -> consumerBuilder.build()).isInstanceOf(StreamException.class);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createPublishConsumeDelete(boolean lazyInit, TestInfo info) {
    try (Environment env = environmentBuilder.lazyInitialization(lazyInit).build()) {
      String s = streamName(info);
      env.streamCreator().stream(s).create();
      int messageCount = 50_000;
      CountDownLatch confirmLatch = new CountDownLatch(messageCount);
      CountDownLatch consumeLatch = new CountDownLatch(messageCount);

      Producer producer = env.producerBuilder().stream(s).build();
      ConfirmationHandler confirmationHandler = confirmationStatus -> confirmLatch.countDown();
      IntStream.range(0, messageCount)
          .forEach(
              i -> {
                Message message =
                    producer.messageBuilder().addData("".getBytes(StandardCharsets.UTF_8)).build();
                producer.send(message, confirmationHandler);
              });

      latchAssert(confirmLatch).completes();

      Consumer consumer =
          env.consumerBuilder().stream(s)
              .offset(OffsetSpecification.first())
              .messageHandler((context, message) -> consumeLatch.countDown())
              .build();

      latchAssert(consumeLatch).completes();

      producer.close();
      consumer.close();
      env.deleteStream(s);
    }
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void queryStreamStatsShouldReturnFirstOffsetAndCommittedOffset() throws Exception {
    try (Environment env = environmentBuilder.build()) {
      StreamStats stats = env.queryStreamStats(stream);
      assertThatThrownBy(() -> stats.firstOffset()).isInstanceOf(NoOffsetException.class);
      assertThatThrownBy(() -> stats.committedChunkId()).isInstanceOf(NoOffsetException.class);

      int publishCount = 20_000;
      TestUtils.publishAndWaitForConfirms(cf, publishCount, stream);

      StreamStats stats2 = env.queryStreamStats(stream);
      assertThat(stats2.firstOffset()).isZero();
      assertThat(stats2.committedChunkId()).isPositive();

      CountDownLatch latch = new CountDownLatch(publishCount);
      AtomicLong committedChunkId = new AtomicLong();
      env.consumerBuilder().stream(stream)
          .offset(OffsetSpecification.first())
          .messageHandler(
              (context, message) -> {
                committedChunkId.set(context.committedChunkId());
                latch.countDown();
              })
          .build();

      assertThat(latch.await(10, SECONDS)).isTrue();
      assertThat(committedChunkId.get()).isPositive();
      assertThat(committedChunkId).hasValue(stats2.committedChunkId());
    }
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void queryStreamStatsShouldThrowExceptionWhenStreamDoesNotExist() {
    try (Environment env = environmentBuilder.build()) {
      assertThatThrownBy(() -> env.queryStreamStats("does not exist"))
          .isInstanceOf(StreamDoesNotExistException.class);
    }
  }
}

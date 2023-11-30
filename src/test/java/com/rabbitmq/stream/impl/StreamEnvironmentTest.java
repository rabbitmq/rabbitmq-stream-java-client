// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
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

import static com.rabbitmq.stream.impl.TestUtils.ExceptionConditions.responseCode;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.localhostTls;
import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Host.ConnectionInfo;
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
import com.rabbitmq.stream.impl.MonitoringTestUtils.ConsumerCoordinatorInfo;
import com.rabbitmq.stream.impl.MonitoringTestUtils.EnvironmentInfo;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslHandler;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
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
    environmentBuilder.netty().eventLoopGroup(eventLoopGroup);
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
    java.util.function.Consumer<Channel> channelCustomizer =
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
        .netty()
        .channelCustomizer(channelCustomizer)
        .environmentBuilder()
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
        range(0, 2)
            .mapToObj(i -> environment.producerBuilder().stream(stream).build())
            .collect(toList());
    Collection<Consumer> consumers =
        range(0, 2)
            .mapToObj(
                i ->
                    environment.consumerBuilder().stream(stream)
                        .name(UUID.randomUUID().toString())
                        .messageHandler((offset, message) -> {})
                        .build())
            .collect(toList());

    producers.forEach(producer -> assertThat(((StreamProducer) producer).isOpen()).isTrue());
    consumers.forEach(consumer -> assertThat(((StreamConsumer) consumer).isOpen()).isTrue());

    EnvironmentInfo environmentInfo = MonitoringTestUtils.extract(environment);
    assertThat(environmentInfo.getLocators()).isNotEmpty();
    assertThat(environmentInfo.getProducers().clientCount()).isEqualTo(1);
    assertThat(environmentInfo.getProducers().producerCount()).isEqualTo(2);
    assertThat(environmentInfo.getProducers().trackingConsumerCount()).isEqualTo(2);
    ConsumerCoordinatorInfo consumerInfo = environmentInfo.getConsumers();
    assertThat(consumerInfo.nodesConnected()).hasSize(1);
    assertThat(consumerInfo.clients()).hasSize(1);
    assertThat(consumerInfo.consumerCount()).isEqualTo(2);

    environment.close();

    producers.forEach(producer -> assertThat(((StreamProducer) producer).isOpen()).isFalse());
    consumers.forEach(consumer -> assertThat(((StreamConsumer) consumer).isOpen()).isFalse());

    environmentInfo = MonitoringTestUtils.extract(environment);
    assertThat(environmentInfo.getLocators()).isNotEmpty();
    assertThat(environmentInfo.getProducers().clientCount()).isZero();
    assertThat(environmentInfo.getConsumers().clients()).isEmpty();
  }

  @Test
  void growShrinkResourcesWhenProducersConsumersAreOpenedAndClosed(TestInfo info) throws Exception {
    int messageCount = 100;
    int streamCount = 20;
    int producersCount = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT * 3 + 10;
    int consumersCount = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT * 2 + 10;

    try (Environment environment = environmentBuilder.rpcTimeout(Duration.ofSeconds(20)).build()) {
      List<String> streams =
          range(0, streamCount)
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
          range(0, producersCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(i % streams.size());
                    return environment.producerBuilder().stream(s).build();
                  })
              .collect(toList());

      List<Consumer> consumers =
          range(0, consumersCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(new Random().nextInt(streams.size()));
                    return environment.consumerBuilder().stream(s)
                        .messageHandler((offset, message) -> consumeLatch.countDown())
                        .build();
                  })
              .collect(toList());

      producers.stream()
          .parallel()
          .forEach(
              producer -> {
                range(0, messageCount)
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

      latchAssert(confirmLatch).completes();
      latchAssert(consumeLatch).completes();

      EnvironmentInfo environmentInfo = MonitoringTestUtils.extract(environment);
      assertThat(environmentInfo.getProducers().nodesConnected()).hasSize(1);
      int producerManagerCount = environmentInfo.getProducers().clientCount();
      assertThat(producerManagerCount).isPositive();
      ConsumerCoordinatorInfo consumerInfo = environmentInfo.getConsumers();
      assertThat(consumerInfo.nodesConnected()).hasSize(1);
      int consumerManagerCount = consumerInfo.clients().size();
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
      assertThat(environmentInfo.getProducers().nodesConnected()).hasSize(1);
      assertThat(environmentInfo.getProducers().clientCount()).isLessThan(producerManagerCount);
      consumerInfo = environmentInfo.getConsumers();
      assertThat(consumerInfo.nodesConnected()).hasSize(1);
      assertThat(consumerInfo.clients()).hasSizeLessThan(consumerManagerCount);

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
  void locatorShouldReconnectIfConnectionIsLost(TestInfo info) {
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
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void shouldHaveSeveralLocatorsWhenSeveralUrisSpecifiedAndShouldRecoverThemIfClosed(TestInfo info)
      throws Exception {
    List<String> uris =
        range(0, 3).mapToObj(ignored -> "rabbitmq-stream://localhost:5552").collect(toList());
    try (Environment environment =
        environmentBuilder
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(1)))
            .uris(uris)
            .build()) {
      String s = streamName(info);
      environment.streamCreator().stream(s).create();
      environment.deleteStream(s);

      Supplier<List<String>> locatorConnectionNamesSupplier =
          () ->
              Host.listConnections().stream()
                  .map(ConnectionInfo::clientProvidedName)
                  .filter(name -> name.contains("-locator-"))
                  .collect(toList());
      List<String> locatorConnectionNames = locatorConnectionNamesSupplier.get();
      assertThat(locatorConnectionNames).hasSameSizeAs(uris);

      locatorConnectionNames.forEach(connectionName -> Host.killConnection(connectionName));

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
      waitAtMost(() -> locatorConnectionNamesSupplier.get().size() == uris.size());
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

  @ParameterizedTest
  @ValueSource(ints = {3, 5})
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_13_0)
  void superStreamCreationSetPartitions(int partitionCount, TestInfo info) {
    int defaultPartitionCount = 3;
    String s = streamName(info);
    Client client = cf.get();
    Environment env = environmentBuilder.build();
    try {
      StreamCreator.SuperStreamConfiguration configuration =
          env.streamCreator().name(s).superStream();
      if (partitionCount != defaultPartitionCount) {
        configuration.partitions(partitionCount);
      }
      configuration.creator().create();

      assertThat(client.partitions(s))
          .hasSize(partitionCount)
          .containsAll(
              IntStream.range(0, partitionCount).mapToObj(i -> s + "-" + i).collect(toList()));
      IntStream.range(0, partitionCount)
          .forEach(
              i -> assertThat(client.route(String.valueOf(i), s)).hasSize(1).contains(s + "-" + i));
    } finally {
      env.deleteSuperStream(s);
      env.close();
      assertThat(client.partitions(s)).isEmpty();
    }
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_13_0)
  void superStreamCreationSetBindingKeys(TestInfo info) {
    List<String> bindingKeys = Arrays.asList("a", "b", "c", "d", "e");
    String s = streamName(info);
    Client client = cf.get();
    Environment env = environmentBuilder.build();
    try {
      env.streamCreator()
          .name(s)
          .superStream()
          .bindingKeys(bindingKeys.toArray(new String[] {}))
          .creator()
          .create();

      assertThat(client.partitions(s))
          .hasSize(bindingKeys.size())
          .containsAll(bindingKeys.stream().map(rk -> s + "-" + rk).collect(toList()));
      bindingKeys.forEach(bk -> assertThat(client.route(bk, s)).hasSize(1).contains(s + "-" + bk));
    } finally {
      env.deleteSuperStream(s);
      env.close();
      assertThat(client.partitions(s)).isEmpty();
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
      range(0, messageCount)
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void queryStreamStatsShouldReturnFirstOffsetAndCommittedOffset(boolean lazyInit)
      throws Exception {
    try (Environment env = environmentBuilder.lazyInitialization(lazyInit).build()) {
      StreamStats stats = env.queryStreamStats(stream);
      assertThatThrownBy(stats::firstOffset).isInstanceOf(NoOffsetException.class);
      assertThatThrownBy(stats::committedChunkId).isInstanceOf(NoOffsetException.class);

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
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void queryStreamStatsShouldThrowExceptionWhenStreamDoesNotExist() {
    try (Environment env = environmentBuilder.build()) {
      assertThatThrownBy(() -> env.queryStreamStats("does not exist"))
          .isInstanceOf(StreamDoesNotExistException.class);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void streamExists(boolean lazyInit) {
    AtomicBoolean metadataCalled = new AtomicBoolean(false);
    Function<Client.ClientParameters, Client> clientFactory =
        cp ->
            new Client(cp) {
              @Override
              public Map<String, StreamMetadata> metadata(String... streams) {
                metadataCalled.set(true);
                return super.metadata(streams);
              }
            };
    try (Environment env =
        ((StreamEnvironmentBuilder) environmentBuilder.lazyInitialization(lazyInit))
            .clientFactory(clientFactory)
            .build()) {
      assertThat(env.streamExists(stream)).isTrue();
      assertThat(env.streamExists(UUID.randomUUID().toString())).isFalse();
      assertThat(metadataCalled).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void streamExistsMetadataDataFallback(boolean lazyInit) {
    AtomicInteger metadataCallCount = new AtomicInteger(0);
    Function<Client.ClientParameters, Client> clientFactory =
        cp ->
            new Client(cp) {
              @Override
              StreamStatsResponse streamStats(String stream) {
                throw new UnsupportedOperationException();
              }

              @Override
              public Map<String, StreamMetadata> metadata(String... streams) {
                metadataCallCount.incrementAndGet();
                return super.metadata(streams);
              }
            };
    try (Environment env =
        ((StreamEnvironmentBuilder) environmentBuilder.lazyInitialization(lazyInit))
            .clientFactory(clientFactory)
            .build()) {
      assertThat(env.streamExists(stream)).isTrue();
      assertThat(env.streamExists(UUID.randomUUID().toString())).isFalse();
      assertThat(metadataCallCount).hasValue(2);
    }
  }

  @Test
  void methodsShouldThrowExceptionWhenEnvironmentIsClosed() {
    Environment env = environmentBuilder.build();
    env.close();
    ThrowingCallable[] calls =
        new ThrowingCallable[] {
          () -> env.streamCreator(),
          () -> env.producerBuilder(),
          () -> env.consumerBuilder(),
          () -> env.deleteStream("does not matter"),
          () -> env.queryStreamStats("does not matter"),
          () -> env.streamExists("does not matter")
        };
    Arrays.stream(calls)
        .forEach(call -> assertThatThrownBy(call).isInstanceOf(IllegalStateException.class));
  }

  @Test
  void nettyInitializersAreCalled() {
    AtomicBoolean bootstrapCalled = new AtomicBoolean(false);
    AtomicBoolean channelCalled = new AtomicBoolean(false);
    try (Environment ignored =
        environmentBuilder
            .netty()
            .bootstrapCustomizer(b -> bootstrapCalled.set(true))
            .channelCustomizer(ch -> channelCalled.set(true))
            .environmentBuilder()
            .build()) {}
    assertThat(bootstrapCalled).isTrue();
    assertThat(channelCalled).isTrue();
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  @EnabledIfSystemProperty(named = "os.arch", matches = "amd64")
  void nativeEpollWorksOnLinux() {
    int messageCount = 10_000;
    EventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup();
    try {
      Set<Channel> channels = ConcurrentHashMap.newKeySet();
      try (Environment env =
          environmentBuilder
              .netty()
              .eventLoopGroup(epollEventLoopGroup)
              .bootstrapCustomizer(b -> b.channel(EpollSocketChannel.class))
              .channelCustomizer(ch -> channels.add(ch))
              .environmentBuilder()
              .build()) {
        Producer producer = env.producerBuilder().stream(this.stream).build();
        ConfirmationHandler handler = confirmationStatus -> {};
        IntStream.range(0, messageCount)
            .forEach(
                i ->
                    producer.send(
                        producer
                            .messageBuilder()
                            .addData("hello".getBytes(StandardCharsets.UTF_8))
                            .build(),
                        handler));
        CountDownLatch latch = new CountDownLatch(messageCount);
        env.consumerBuilder().stream(this.stream)
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> latch.countDown())
            .build();
        assertThat(latchAssert(latch)).completes();
      }
      assertThat(channels).isNotEmpty().allMatch(ch -> ch instanceof EpollSocketChannel);
    } finally {
      epollEventLoopGroup.shutdownGracefully(0, 0, SECONDS);
    }
  }
}

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

import static com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay;
import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.metadata;
import static com.rabbitmq.stream.impl.TestUtils.namedConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ConsumersPoolInfo;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsumersCoordinatorTest {

  private static final SubscriptionListener NO_OP_SUBSCRIPTION_LISTENER = subscriptionContext -> {};
  private static final Runnable NO_OP_TRACKING_CLOSING_CALLBACK = () -> {};

  @Mock StreamEnvironment environment;
  @Mock StreamConsumer consumer;
  @Mock Client locator;
  @Mock ClientFactory clientFactory;
  @Mock Client client;
  @Captor ArgumentCaptor<Byte> subscriptionIdCaptor;
  AutoCloseable mocks;

  ConsumersCoordinator coordinator;
  ScheduledExecutorService scheduledExecutorService;
  volatile Client.MetadataListener metadataListener;
  volatile Client.MessageListener messageListener;
  List<Client.MessageListener> messageListeners = new CopyOnWriteArrayList<>();
  volatile Client.ShutdownListener shutdownListener;
  List<Client.ShutdownListener> shutdownListeners =
      new CopyOnWriteArrayList<>(); // when we need several of them in the test
  List<Client.MetadataListener> metadataListeners =
      new CopyOnWriteArrayList<>(); // when we need several of them in the test

  static Duration ms(long ms) {
    return Duration.ofMillis(ms);
  }

  static Stream<Consumer<ConsumersCoordinatorTest>> disruptionArguments() {
    return Stream.of(
        namedConsumer(
            test ->
                test.shutdownListener.handle(
                    new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN)),
            "disconnection"),
        namedConsumer(
            test ->
                test.metadataListener.handle(
                    "stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE),
            "topology change"));
  }

  @BeforeEach
  void init() {
    Client.ClientParameters clientParameters =
        new Client.ClientParameters() {
          @Override
          public Client.ClientParameters metadataListener(
              Client.MetadataListener metadataListener) {
            ConsumersCoordinatorTest.this.metadataListener = metadataListener;
            ConsumersCoordinatorTest.this.metadataListeners.add(metadataListener);
            return super.metadataListener(metadataListener);
          }

          @Override
          public Client.ClientParameters messageListener(Client.MessageListener messageListener) {
            ConsumersCoordinatorTest.this.messageListener = messageListener;
            ConsumersCoordinatorTest.this.messageListeners.add(messageListener);
            return super.messageListener(messageListener);
          }

          @Override
          public Client.ClientParameters shutdownListener(
              Client.ShutdownListener shutdownListener) {
            ConsumersCoordinatorTest.this.shutdownListener = shutdownListener;
            ConsumersCoordinatorTest.this.shutdownListeners.add(shutdownListener);
            return super.shutdownListener(shutdownListener);
          }
        };
    mocks = MockitoAnnotations.openMocks(this);
    when(environment.locator()).thenReturn(locator);
    when(environment.locatorOperation(any())).thenCallRealMethod();
    when(environment.clientParametersCopy()).thenReturn(clientParameters);
    when(environment.addressResolver()).thenReturn(address -> address);

    coordinator =
        new ConsumersCoordinator(
            environment,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            type -> "consumer-connection",
            clientFactory);
  }

  @AfterEach
  void tearDown() throws Exception {
    // just taking the opportunity to check toString() generates valid JSON
    MonitoringTestUtils.extract(coordinator);
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    mocks.close();
  }

  @Test
  void
      shouldRetryUntilGettingExactNodeWithAdvertisedHostNameClientFactoryAndNotExactNodeOnFirstTime() {
    ClientFactory cf =
        context ->
            Utils.connectToAdvertisedNodeClientFactory(
                    context.key(), clientFactory, Duration.ofMillis(1))
                .client(context);
    ConsumersCoordinator c =
        new ConsumersCoordinator(
            environment,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            type -> "consumer-connection",
            cf);

    when(locator.metadata("stream")).thenReturn(metadata(null, replica()));
    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));
    when(client.serverAdvertisedHost()).thenReturn("foo").thenReturn(replica().get(0).getHost());
    when(client.serverAdvertisedPort()).thenReturn(42).thenReturn(replica().get(0).getPort());

    c.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());
    verify(clientFactory, times(2)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
  }

  @Test
  void shouldGetExactNodeImmediatelyWithAdvertisedHostNameClientFactoryAndExactNodeOnFirstTime() {
    ClientFactory cf =
        context ->
            Utils.connectToAdvertisedNodeClientFactory(
                    context.key(), clientFactory, Duration.ofMillis(1))
                .client(context);
    ConsumersCoordinator c =
        new ConsumersCoordinator(
            environment,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            type -> "consumer-connection",
            cf);

    when(locator.metadata("stream")).thenReturn(metadata(null, replica()));
    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));
    when(client.serverAdvertisedHost()).thenReturn(replica().get(0).getHost());
    when(client.serverAdvertisedPort()).thenReturn(replica().get(0).getPort());

    c.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldSubscribeWithEmptyPropertiesWithUnamedConsumer() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    ArgumentCaptor<Map<String, String>> subscriptionPropertiesArgumentCaptor =
        ArgumentCaptor.forClass(Map.class);
    when(client.subscribe(
            anyByte(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            subscriptionPropertiesArgumentCaptor.capture()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    coordinator.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(subscriptionPropertiesArgumentCaptor.getValue()).isEmpty();
  }

  @Test
  void subscribeShouldThrowExceptionWhenNoMetadataForTheStream() {
    assertThatThrownBy(
            () ->
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap()))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void subscribeShouldThrowExceptionWhenStreamDoesNotExist() {
    when(locator.metadata("stream"))
        .thenReturn(metadata("stream", null, null, Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
    assertThatThrownBy(
            () ->
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap()))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void subscribePropagateExceptionWhenClientSubscriptionFails() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);
    String exceptionMessage = "Could not get response in 10000 ms";
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenThrow(new StreamException(exceptionMessage));

    assertThatThrownBy(
            () ->
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap()))
        .isInstanceOf(StreamException.class)
        .hasMessage(exceptionMessage);
    assertThat(MonitoringTestUtils.extract(coordinator)).isEmpty();
  }

  @Test
  void subscribeShouldThrowExceptionWhenMetadataResponseIsNotOk() {
    when(locator.metadata("stream"))
        .thenReturn(metadata("stream", null, null, Constants.RESPONSE_CODE_ACCESS_REFUSED));
    assertThatThrownBy(
            () ->
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void subscribeShouldThrowExceptionIfNoNodeAvailableForStream() {
    when(locator.metadata("stream")).thenReturn(metadata(null, null));
    assertThatThrownBy(
            () ->
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void findBrokersForStreamShouldReturnLeaderIfNoReplicas() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), null));
    assertThat(coordinator.findBrokersForStream("stream")).hasSize(1).contains(leader());
  }

  @Test
  void findBrokersForStreamShouldReturnReplicasIfThereAreSome() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));
    assertThat(coordinator.findBrokersForStream("stream")).hasSize(2).hasSameElementsAs(replicas());
  }

  @Test
  void subscribeShouldSubscribeToStreamAndDispatchMessage_UnsubscribeShouldUnsubscribe() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    AtomicInteger trackingClosingCallbackCalls = new AtomicInteger();
    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.first(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            () -> trackingClosingCallbackCalls.incrementAndGet(),
            (offset, message) -> messageHandlerCalls.incrementAndGet(),
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    messageListener.handle(
        subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());
    assertThat(trackingClosingCallbackCalls).hasValue(1);

    messageListener.handle(
        subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);
  }

  @Test
  void subscribeShouldSubscribeToStreamAndDispatchMessageWithManySubscriptions() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), null));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    Map<Byte, Integer> messageHandlerCalls = new ConcurrentHashMap<>();
    List<Runnable> closingRunnables = new ArrayList<>();
    for (int i = 0; i < ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
      byte subId = b(i);
      Runnable closingRunnable =
          coordinator.subscribe(
              consumer,
              "stream",
              OffsetSpecification.first(),
              null,
              NO_OP_SUBSCRIPTION_LISTENER,
              NO_OP_TRACKING_CLOSING_CALLBACK,
              (offset, message) ->
                  messageHandlerCalls.compute(subId, (k, v) -> (v == null) ? 1 : ++v),
              Collections.emptyMap());
      closingRunnables.add(closingRunnable);
    }

    verify(clientFactory, times(1)).client(any());
    verify(client, times(ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    Runnable messageToEachSubscription =
        () ->
            subscriptionIdCaptor
                .getAllValues()
                .forEach(
                    subscriptionId -> {
                      messageListener.handle(
                          subscriptionId, 0, 0, 0, new WrapperMessageBuilder().build());
                    });
    messageToEachSubscription.run();
    assertThat(messageHandlerCalls).hasSize(ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT);
    messageHandlerCalls.values().forEach(messageCount -> assertThat(messageCount).isEqualTo(1));

    when(client.unsubscribe(anyByte())).thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnables.forEach(closingRunnable -> closingRunnable.run());

    verify(client, times(ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT)).unsubscribe(anyByte());

    // simulating inbound messages again, but they should go nowhere
    messageToEachSubscription.run();
    assertThat(messageHandlerCalls).hasSize(ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT);
    messageHandlerCalls.values().forEach(messageCount -> assertThat(messageCount).isEqualTo(1));
  }

  @Test
  void shouldRedistributeConsumerIfConnectionIsLost() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(100);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replica()))
        .thenReturn(metadata(null, replica())) // for the second consumer
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, replica()));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    StreamConsumer consumerClosedAfterConnectionLost = mock(StreamConsumer.class);
    when(consumerClosedAfterConnectionLost.isOpen()).thenReturn(false);

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.first(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> messageHandlerCalls.incrementAndGet(),
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    messageListener.handle(
        subscriptionIdCaptor.getAllValues().get(0), 1, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    coordinator.subscribe(
        consumerClosedAfterConnectionLost,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());

    verify(client, times(1 + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    Thread.sleep(retryDelay.toMillis() * 5);

    // the consumer connection should be reset after the connection disruption
    verify(consumer, times(1)).setSubscriptionClient(isNull());

    // the second consumer does not re-subscribe because it returns it is not open
    verify(client, times(2 + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(1);
    messageListener.handle(
        subscriptionIdCaptor.getAllValues().get(0), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());

    messageListener.handle(
        subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);
  }

  @Test
  void shouldRedistributeConsumerOnMetadataUpdate() throws Exception {
    BackOffDelayPolicy delayPolicy = fixedWithInitialDelay(ms(100), ms(100));
    when(environment.topologyUpdateBackOffDelayPolicy()).thenReturn(delayPolicy);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    StreamConsumer consumerClosedAfterMetadataUpdate = mock(StreamConsumer.class);
    when(consumerClosedAfterMetadataUpdate.isOpen()).thenReturn(false);

    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.first(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> messageHandlerCalls.incrementAndGet(),
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    coordinator.subscribe(
        consumerClosedAfterMetadataUpdate,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());

    verify(client, times(1 + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    firstMessageListener()
        .handle(
            subscriptionIdCaptor.getAllValues().get(0),
            1,
            0,
            0,
            new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    this.metadataListeners.forEach(
        ml -> ml.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE));

    // the consumer connection should be reset after the metadata update
    verify(consumer, times(1)).setSubscriptionClient(isNull());

    Thread.sleep(delayPolicy.delay(0).toMillis() * 5);

    // the second consumer does not re-subscribe because it returns it is not open
    verify(client, times(2 + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(1);
    lastMessageListener()
        .handle(
            subscriptionIdCaptor.getAllValues().get(0),
            0,
            0,
            0,
            new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());

    lastMessageListener()
        .handle(subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);

    assertThat(coordinator.poolSize()).isZero();
  }

  @Test
  void shouldRetryRedistributionIfMetadataIsNotUpdatedImmediately() throws Exception {
    BackOffDelayPolicy delayPolicy = fixedWithInitialDelay(ms(100), ms(100));
    when(environment.topologyUpdateBackOffDelayPolicy()).thenReturn(delayPolicy);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.first(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> messageHandlerCalls.incrementAndGet(),
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    messageListener.handle(
        subscriptionIdCaptor.getValue(), 1, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    Thread.sleep(delayPolicy.delay(0).toMillis() + delayPolicy.delay(1).toMillis() * 5);

    verify(client, times(2))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(1);
    messageListener.handle(
        subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());

    messageListener.handle(
        subscriptionIdCaptor.getValue(), 0, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(2);

    assertThat(coordinator.poolSize()).isZero();
  }

  @Test
  void metadataUpdate_shouldCloseConsumerIfStreamIsDeleted() throws Exception {
    BackOffDelayPolicy delayPolicy = fixedWithInitialDelay(ms(50), ms(50));
    when(environment.topologyUpdateBackOffDelayPolicy()).thenReturn(delayPolicy);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata("stream", null, null, Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    coordinator.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> messageHandlerCalls.incrementAndGet(),
        Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    messageListener.handle(
        subscriptionIdCaptor.getValue(), 1, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    Thread.sleep(delayPolicy.delay(0).toMillis() * 5);

    verify(consumer, times(1)).closeAfterStreamDeletion();
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
    verify(client, times(0)).unsubscribe(anyByte());

    assertThat(coordinator.poolSize()).isZero();
  }

  @Test
  void metadataUpdate_shouldCloseConsumerIfRetryTimeoutIsReached() throws Exception {
    Duration retryTimeout = Duration.ofMillis(200);
    BackOffDelayPolicy delayPolicy = fixedWithInitialDelay(ms(50), ms(50), ms(200));
    when(environment.topologyUpdateBackOffDelayPolicy()).thenReturn(delayPolicy);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenThrow(new IllegalStateException());

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    AtomicInteger messageHandlerCalls = new AtomicInteger();
    coordinator.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> messageHandlerCalls.incrementAndGet(),
        Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(messageHandlerCalls.get()).isEqualTo(0);
    messageListener.handle(
        subscriptionIdCaptor.getValue(), 1, 0, 0, new WrapperMessageBuilder().build());
    assertThat(messageHandlerCalls.get()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    Thread.sleep(delayPolicy.delay(0).toMillis() + retryTimeout.toMillis() * 2);

    verify(consumer, times(1)).closeAfterStreamDeletion();
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
    verify(client, times(0)).unsubscribe(anyByte());

    assertThat(coordinator.poolSize()).isZero();
  }

  @Test
  void shouldUseNewClientsForMoreThanMaxSubscriptionsAndCloseClientAfterUnsubscriptions() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), null));

    when(clientFactory.client(any())).thenReturn(client);

    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));
    when(client.isOpen()).thenReturn(true);

    int extraSubscriptionCount = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT / 5;
    int subscriptionCount =
        ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT + extraSubscriptionCount;

    List<Runnable> closingRunnables =
        IntStream.range(0, subscriptionCount)
            .mapToObj(
                i ->
                    coordinator.subscribe(
                        consumer,
                        "stream",
                        OffsetSpecification.first(),
                        null,
                        NO_OP_SUBSCRIPTION_LISTENER,
                        NO_OP_TRACKING_CLOSING_CALLBACK,
                        (offset, message) -> {},
                        Collections.emptyMap()))
            .collect(Collectors.toList());

    verify(clientFactory, times(2)).client(any());
    verify(client, times(subscriptionCount))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    when(client.unsubscribe(anyByte())).thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    // we reverse the subscription list to remove the lasts first
    // this frees the second client that should get closed
    Collections.reverse(closingRunnables);
    new ArrayList<>(closingRunnables)
        .stream()
            .limit(subscriptionCount - extraSubscriptionCount * 2)
            .forEach(
                closingRunnable -> {
                  closingRunnable.run();
                  closingRunnables.remove(closingRunnable);
                });

    verify(client, times(1)).close();

    closingRunnables.forEach(closingRunnable -> closingRunnable.run());

    verify(client, times(2)).close();
  }

  @Test
  void shouldRemoveClientSubscriptionManagerFromPoolAfterConnectionDies() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(100);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas().subList(0, 1)));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    int extraSubscriptionCount = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT / 5;
    int subscriptionCount =
        ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT + extraSubscriptionCount;
    IntStream.range(0, subscriptionCount)
        .forEach(
            i -> {
              coordinator.subscribe(
                  consumer,
                  "stream",
                  OffsetSpecification.first(),
                  null,
                  NO_OP_SUBSCRIPTION_LISTENER,
                  NO_OP_TRACKING_CLOSING_CALLBACK,
                  (offset, message) -> {},
                  Collections.emptyMap());
            });
    // the extra is allocated on another client from the same pool
    verify(clientFactory, times(2)).client(any());
    verify(client, times(subscriptionCount))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    // let's kill the first client connection
    shutdownListeners
        .get(0)
        .handle(new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    Thread.sleep(retryDelay.toMillis() * 5);

    // the MAX consumers must have been re-allocated to the existing client and a new one
    // let's add a new subscription to make sure we are still using the same pool
    coordinator.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());

    verify(clientFactory, times(2 + 1)).client(any());
    verify(client, times(subscriptionCount + ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
  }

  @Test
  void shouldRemoveClientSubscriptionManagerFromPoolIfEmptyAfterMetadataUpdate() throws Exception {
    BackOffDelayPolicy delayPolicy = fixedWithInitialDelay(ms(50), ms(50));
    when(environment.topologyUpdateBackOffDelayPolicy()).thenReturn(delayPolicy);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas().subList(0, 1)));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    int extraSubscriptionCount = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT / 5;
    int subscriptionCount =
        ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT + extraSubscriptionCount;
    IntStream.range(0, subscriptionCount)
        .forEach(
            i -> {
              coordinator.subscribe(
                  consumer,
                  "stream",
                  OffsetSpecification.first(),
                  null,
                  NO_OP_SUBSCRIPTION_LISTENER,
                  NO_OP_TRACKING_CLOSING_CALLBACK,
                  (offset, message) -> {},
                  Collections.emptyMap());
            });
    // the extra is allocated on another client from the same pool
    verify(clientFactory, times(2)).client(any());
    verify(client, times(subscriptionCount))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    List<ConsumersPoolInfo> info = MonitoringTestUtils.extract(coordinator);
    assertThat(info)
        .hasSize(1)
        .element(0)
        .extracting(pool -> pool.consumerCount())
        .isEqualTo(subscriptionCount);

    // let's kill the first client connection
    metadataListeners.get(0).handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    Thread.sleep(delayPolicy.delay(0).toMillis() * 5);

    info = MonitoringTestUtils.extract(coordinator);
    assertThat(info)
        .hasSize(1)
        .element(0)
        .extracting(pool -> pool.consumerCount())
        .isEqualTo(subscriptionCount);

    // the MAX consumers must have been re-allocated to the existing client and a new one
    // let's add a new subscription to make sure we are still using the same pool
    coordinator.subscribe(
        consumer,
        "stream",
        OffsetSpecification.first(),
        null,
        NO_OP_SUBSCRIPTION_LISTENER,
        NO_OP_TRACKING_CLOSING_CALLBACK,
        (offset, message) -> {},
        Collections.emptyMap());

    verify(clientFactory, times(2 + 1)).client(any());
    verify(client, times(subscriptionCount + ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT + 1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    info = MonitoringTestUtils.extract(coordinator);
    assertThat(info)
        .hasSize(1)
        .element(0)
        .extracting(pool -> pool.consumerCount())
        .isEqualTo(subscriptionCount + 1);
  }

  @ParameterizedTest
  @MethodSource("disruptionArguments")
  void shouldRestartWhereItLeftOffAfterDisruption(Consumer<ConsumersCoordinatorTest> configurator)
      throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(100);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, replicas()));

    ArgumentCaptor<OffsetSpecification> offsetSpecificationArgumentCaptor =
        ArgumentCaptor.forClass(OffsetSpecification.class);

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            offsetSpecificationArgumentCaptor.capture(),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.first(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> {},
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(0)
        .isEqualTo(OffsetSpecification.first());

    long lastReceivedOffset = 10;
    messageListener.handle(
        subscriptionIdCaptor.getValue(),
        lastReceivedOffset,
        0,
        0,
        new WrapperMessageBuilder().build());

    configurator.accept(this);

    Thread.sleep(retryDelay.toMillis() * 5);

    verify(client, times(2))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(1)
        .isEqualTo(OffsetSpecification.offset(lastReceivedOffset));

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());
  }

  @ParameterizedTest
  @MethodSource("disruptionArguments")
  void shouldReUseInitialOffsetSpecificationAfterDisruptionIfNoMessagesReceived(
      Consumer<ConsumersCoordinatorTest> configurator) throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(100);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, replicas()));

    ArgumentCaptor<OffsetSpecification> offsetSpecificationArgumentCaptor =
        ArgumentCaptor.forClass(OffsetSpecification.class);

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            offsetSpecificationArgumentCaptor.capture(),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            OffsetSpecification.next(),
            null,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> {},
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(0)
        .isEqualTo(OffsetSpecification.next());

    configurator.accept(this);

    Thread.sleep(retryDelay.toMillis() * 5);

    verify(client, times(2))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(1)
        .isEqualTo(OffsetSpecification.next());

    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());
  }

  @ParameterizedTest
  @MethodSource("disruptionArguments")
  @SuppressWarnings("unchecked")
  void shouldUseStoredOffsetOnRecovery(Consumer<ConsumersCoordinatorTest> configurator)
      throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(100);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(consumer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, Collections.emptyList()))
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    String consumerName = "consumer-name";
    long lastStoredOffset = 5;
    long lastReceivedOffset = 10;
    when(client.queryOffset(consumerName, "stream"))
        .thenReturn(new QueryOffsetResponse(Constants.RESPONSE_CODE_OK, 0L))
        .thenReturn(new QueryOffsetResponse(Constants.RESPONSE_CODE_OK, lastStoredOffset));

    ArgumentCaptor<OffsetSpecification> offsetSpecificationArgumentCaptor =
        ArgumentCaptor.forClass(OffsetSpecification.class);
    ArgumentCaptor<Map<String, String>> subscriptionPropertiesArgumentCaptor =
        ArgumentCaptor.forClass(Map.class);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            offsetSpecificationArgumentCaptor.capture(),
            anyInt(),
            subscriptionPropertiesArgumentCaptor.capture()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    Runnable closingRunnable =
        coordinator.subscribe(
            consumer,
            "stream",
            null,
            consumerName,
            NO_OP_SUBSCRIPTION_LISTENER,
            NO_OP_TRACKING_CLOSING_CALLBACK,
            (offset, message) -> {},
            Collections.emptyMap());
    verify(clientFactory, times(1)).client(any());
    verify(client, times(1))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());
    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(0)
        .isEqualTo(OffsetSpecification.next());
    assertThat(subscriptionPropertiesArgumentCaptor.getAllValues())
        .element(0)
        .isEqualTo(Collections.singletonMap("name", "consumer-name"));

    messageListener.handle(
        subscriptionIdCaptor.getValue(),
        lastReceivedOffset,
        0,
        0,
        new WrapperMessageBuilder().build());

    configurator.accept(this);

    Thread.sleep(retryDelay.toMillis() * 5);

    verify(client, times(2))
        .subscribe(anyByte(), anyString(), any(OffsetSpecification.class), anyInt(), anyMap());

    assertThat(offsetSpecificationArgumentCaptor.getAllValues())
        .element(1)
        .isEqualTo(OffsetSpecification.offset(lastStoredOffset + 1))
        .isNotEqualTo(OffsetSpecification.offset(lastReceivedOffset));
    assertThat(subscriptionPropertiesArgumentCaptor.getAllValues())
        .element(1)
        .isEqualTo(Collections.singletonMap("name", "consumer-name"));
    when(client.unsubscribe(subscriptionIdCaptor.getValue()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    closingRunnable.run();
    verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());
  }

  @Test
  void subscribeUnsubscribeInDifferentThreadsShouldNotDeadlock() throws Exception {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);
    when(client.subscribe(
            subscriptionIdCaptor.capture(),
            anyString(),
            any(OffsetSpecification.class),
            anyInt(),
            anyMap()))
        .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));
    when(client.unsubscribe(anyByte())).thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

    ExecutorService executorService = Executors.newFixedThreadPool(2);

    try {
      Runnable subUnsub =
          () -> {
            Runnable closingRunnable =
                coordinator.subscribe(
                    consumer,
                    "stream",
                    OffsetSpecification.first(),
                    null,
                    NO_OP_SUBSCRIPTION_LISTENER,
                    NO_OP_TRACKING_CLOSING_CALLBACK,
                    (offset, message) -> {},
                    Collections.emptyMap());

            closingRunnable.run();
          };
      CountDownLatch latch = new CountDownLatch(2);
      executorService.submit(
          () -> {
            int count = 0;
            while (count++ < 10) {
              subUnsub.run();
            }
            latch.countDown();
          });
      executorService.submit(
          () -> {
            int count = 0;
            while (count++ < 10) {
              subUnsub.run();
            }
            latch.countDown();
          });

      assertThat(latchAssert(latch)).completes();
    } finally {
      executorService.shutdownNow();
    }
  }

  Client.Broker leader() {
    return new Client.Broker("leader", -1);
  }

  List<Client.Broker> replicas() {
    return Arrays.asList(new Client.Broker("replica1", -1), new Client.Broker("replica2", -1));
  }

  List<Client.Broker> replica() {
    return replicas().subList(0, 1);
  }

  private MessageListener firstMessageListener() {
    return this.messageListeners.get(0);
  }

  private MessageListener lastMessageListener() {
    return this.messageListeners.get(messageListeners.size() - 1);
  }
}

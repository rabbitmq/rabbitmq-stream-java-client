// Copyright (c) 2022-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_3_11_14;
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.ko;
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.ok;
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.responseCode;
import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.ConsumerUpdateListener;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast311Condition;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfRabbitMqCtlNotSet;
import io.github.bucket4j.Bucket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({
  TestUtils.StreamTestInfrastructureExtension.class,
  BrokerVersionAtLeast311Condition.class
})
@TestUtils.SingleActiveConsumer
public class SacClientTest {

  String stream;
  TestUtils.ClientFactory cf;

  private static Map<Byte, Boolean> consumerStates(int number) {
    Map<Byte, Boolean> consumerStates = new ConcurrentHashMap<>(number);
    IntStream.range(0, number).forEach(i -> consumerStates.put(b(i), false));
    return consumerStates;
  }

  private static Map<Byte, AtomicInteger> receivedMessages(int subscriptionCount) {
    Map<Byte, AtomicInteger> receivedMessages = new ConcurrentHashMap<>(subscriptionCount);
    IntStream.range(0, subscriptionCount)
        .forEach(i -> receivedMessages.put(b(i), new AtomicInteger(0)));
    return receivedMessages;
  }

  private static ClientParameters withConnectionName(String connectionName) {
    return new ClientParameters().clientProperty("connection_name", connectionName);
  }

  @Test
  void secondSubscriptionShouldTakeOverAfterFirstOneUnsubscribes() throws Exception {
    Client writerClient = cf.get();
    int messageCount = 5_000;
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    Map<Byte, Boolean> consumerStates = consumerStates(2);
    Map<Byte, AtomicInteger> receivedMessages = receivedMessages(2);
    String consumerName = "foo";
    ClientParameters clientParameters =
        new ClientParameters()
            .chunkListener(TestUtils.credit())
            .messageListener(
                (subscriptionId,
                    offset,
                    chunkTimestamp,
                    committedChunkId,
                    chunkContext,
                    message) -> {
                  lastReceivedOffset.set(offset);
                  receivedMessages.get(subscriptionId).incrementAndGet();
                })
            .consumerUpdateListener(
                (client, subscriptionId, active) -> {
                  consumerStates.put(subscriptionId, active);
                  long storedOffset = writerClient.queryOffset(consumerName, stream).getOffset();
                  if (storedOffset == 0) {
                    return OffsetSpecification.first();
                  } else {
                    return OffsetSpecification.offset(storedOffset + 1);
                  }
                });
    Client client = cf.get(clientParameters);

    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("single-active-consumer", "true");
    parameters.put("name", consumerName);
    Response response = client.subscribe(b(0), stream, OffsetSpecification.first(), 2, parameters);
    assertThat(response.isOk()).isTrue();
    response = client.subscribe(b(1), stream, OffsetSpecification.first(), 2, parameters);
    assertThat(response.isOk()).isTrue();
    waitAtMost(() -> consumerStates.get(b(0)));
    assertThat(consumerStates)
        .hasSize(2)
        .containsEntry(b(0), Boolean.TRUE)
        .containsEntry(b(1), Boolean.FALSE);

    waitAtMost(() -> receivedMessages.get(b(0)).get() == messageCount);

    assertThat(lastReceivedOffset).hasPositiveValue();
    writerClient.storeOffset(consumerName, stream, lastReceivedOffset.get());
    waitAtMost(
        () ->
            writerClient.queryOffset(consumerName, stream).getOffset() == lastReceivedOffset.get());

    long firstWaveLimit = lastReceivedOffset.get();
    response = client.unsubscribe(b(0));
    assertThat(response.isOk()).isTrue();

    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);

    waitAtMost(() -> consumerStates.get(b(1)) == true);

    waitAtMost(
        () -> receivedMessages.getOrDefault(b(1), new AtomicInteger(0)).get() == messageCount);
    assertThat(lastReceivedOffset).hasValueGreaterThan(firstWaveLimit);

    response = client.unsubscribe(b(1));
    assertThat(response.isOk()).isTrue();
  }

  @Test
  void consumerUpdateListenerShouldBeCalledOnlyWhenConsumerGetsActivated() throws Exception {
    StringBuffer consumerUpdateHistory = new StringBuffer();
    Client client =
        cf.get(
            new ClientParameters()
                .consumerUpdateListener(
                    (client1, subscriptionId, active) -> {
                      consumerUpdateHistory.append(
                          String.format("<%d.%b>", subscriptionId, active));
                      return null;
                    }));
    String consumerName = "foo";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("single-active-consumer", "true");
    parameters.put("name", consumerName);
    Response response = client.subscribe(b(0), stream, OffsetSpecification.first(), 2, parameters);
    assertThat(response.isOk()).isTrue();
    waitAtMost(() -> consumerUpdateHistory.toString().equals("<0.true>"));
    for (int i = 1; i < 10; i++) {
      byte subscriptionId = b(i);
      response =
          client.subscribe(subscriptionId, stream, OffsetSpecification.first(), 2, parameters);
      assertThat(response.isOk()).isTrue();
    }

    for (int i = 0; i < 9; i++) {
      byte subscriptionId = b(i);
      response = client.unsubscribe(subscriptionId);
      assertThat(response.isOk()).isTrue();
      waitAtMost(
          () ->
              consumerUpdateHistory
                  .toString()
                  .contains(String.format("<%d.%b>", subscriptionId + 1, true)));
    }
    response = client.unsubscribe(b(9));
    assertThat(response.isOk()).isTrue();
  }

  @Test
  void noConsumerUpdateOnConnectionClosingIfSubscriptionNotUnsubscribed() throws Exception {
    AtomicInteger consumerUpdateCount = new AtomicInteger(0);
    Client client =
        cf.get(
            new ClientParameters()
                .consumerUpdateListener(
                    (client1, subscriptionId, active) -> {
                      consumerUpdateCount.incrementAndGet();
                      return null;
                    }));
    String consumerName = "foo";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("single-active-consumer", "true");
    parameters.put("name", consumerName);
    Response response = client.subscribe(b(0), stream, OffsetSpecification.first(), 2, parameters);
    assertThat(response.isOk()).isTrue();
    response = client.subscribe(b(1), stream, OffsetSpecification.first(), 2, parameters);
    assertThat(response.isOk()).isTrue();
    waitAtMost(() -> consumerUpdateCount.get() == 1);

    client.close();
    assertThat(consumerUpdateCount).hasValue(1);
  }

  @Test
  void singleActiveConsumerShouldRolloverWhenAnotherJoinsPartition(TestInfo info) throws Exception {
    Client writerClient = cf.get();
    int messageCount = 5_000;
    Map<Byte, Boolean> consumerStates = consumerStates(2);
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    Map<Byte, AtomicInteger> receivedMessages = receivedMessages(2);
    String superStream = streamName(info);
    String consumerName = "foo";
    Client configurationClient = cf.get();
    try {
      declareSuperStreamTopology(configurationClient, superStream, 3);
      // working with the second partition
      String partition = superStream + "-1";

      Client client =
          cf.get(
              new ClientParameters()
                  .consumerUpdateListener(
                      (client1, subscriptionId, active) -> {
                        boolean previousState = consumerStates.get(subscriptionId);

                        OffsetSpecification result;

                        if (previousState == false && active == true) {
                          long storedOffset =
                              writerClient.queryOffset(consumerName, partition).getOffset();
                          result =
                              storedOffset == 0
                                  ? OffsetSpecification.first()
                                  : OffsetSpecification.offset(storedOffset + 1);
                        } else if (previousState == true && active == false) {
                          writerClient.storeOffset(
                              consumerName, partition, lastReceivedOffset.get());
                          try {
                            waitAtMost(
                                () ->
                                    writerClient.queryOffset(consumerName, partition).getOffset()
                                        == lastReceivedOffset.get());
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }
                          result = OffsetSpecification.none();
                        } else {
                          throw new IllegalStateException(
                              "There should no SAC transition from "
                                  + previousState
                                  + " to "
                                  + active);
                        }
                        consumerStates.put(subscriptionId, active);
                        return result;
                      })
                  .chunkListener(TestUtils.credit())
                  .messageListener(
                      (subscriptionId,
                          offset,
                          chunkTimestamp,
                          committedChunkId,
                          chunkContext,
                          message) -> {
                        lastReceivedOffset.set(offset);
                        receivedMessages.get(subscriptionId).incrementAndGet();
                      }));
      Map<String, String> parameters = new HashMap<>();
      parameters.put("single-active-consumer", "true");
      parameters.put("name", consumerName);
      parameters.put("super-stream", superStream);
      Response response =
          client.subscribe(b(0), partition, OffsetSpecification.first(), 2, parameters);
      assertThat(response.isOk()).isTrue();
      waitAtMost(() -> consumerStates.get(b(0)) == true);

      TestUtils.publishAndWaitForConfirms(cf, messageCount, partition);

      waitAtMost(() -> receivedMessages.get(b(0)).get() == messageCount);
      assertThat(lastReceivedOffset).hasPositiveValue();
      long firstWaveLimit = lastReceivedOffset.get();

      response = client.subscribe(b(1), partition, OffsetSpecification.first(), 2, parameters);
      assertThat(response.isOk()).isTrue();

      waitAtMost(() -> consumerStates.get(b(0)) == false);
      waitAtMost(() -> consumerStates.get(b(1)) == true);

      TestUtils.publishAndWaitForConfirms(cf, messageCount, partition);

      waitAtMost(() -> receivedMessages.get(b(1)).get() == messageCount);
      assertThat(lastReceivedOffset).hasValueGreaterThan(firstWaveLimit);

      // clean unsubscription, storing the offset
      writerClient.storeOffset(consumerName, partition, lastReceivedOffset.get());
      waitAtMost(
          () ->
              writerClient.queryOffset(consumerName, partition).getOffset()
                  == lastReceivedOffset.get());

      response = client.unsubscribe(b(1));
      assertThat(response.isOk()).isTrue();
      waitAtMost(() -> consumerStates.get(b(0)) == true);
      assertThat(consumerStates).containsEntry(b(1), true); // should not change when unsubscribing

      response = client.unsubscribe(b(0));
      assertThat(response.isOk()).isTrue();

      assertThat(receivedMessages.values().stream().mapToInt(AtomicInteger::get).sum())
          .isEqualTo(messageCount * 2);

    } finally {
      deleteSuperStreamTopology(configurationClient, superStream);
    }
  }

  @Test
  void singleActiveConsumersShouldSpreadOnSuperStreamPartitions(TestInfo info) throws Exception {
    Map<Byte, Boolean> consumerStates = consumerStates(3 * 3);
    String superStream = streamName(info);
    String consumerName = "foo";
    Client configurationClient = cf.get();
    // subscription distribution
    // client 1: 0, 1, 2 / client 2: 3, 4, 5, / client 3: 6, 7, 8
    try {
      declareSuperStreamTopology(configurationClient, superStream, 3);
      List<String> partitions =
          IntStream.range(0, 3).mapToObj(i -> superStream + "-" + i).collect(toList());
      ConsumerUpdateListener consumerUpdateListener =
          (client1, subscriptionId, active) -> {
            consumerStates.put(subscriptionId, active);
            return null;
          };
      Client client1 =
          cf.get(new ClientParameters().consumerUpdateListener(consumerUpdateListener));
      Map<String, String> subscriptionProperties = new HashMap<>();
      subscriptionProperties.put("single-active-consumer", "true");
      subscriptionProperties.put("name", consumerName);
      subscriptionProperties.put("super-stream", superStream);
      AtomicInteger subscriptionCounter = new AtomicInteger(0);
      AtomicReference<Client> client = new AtomicReference<>();
      Consumer<String> subscriptionCallback =
          partition -> {
            Response response =
                client
                    .get()
                    .subscribe(
                        b(subscriptionCounter.getAndIncrement()),
                        partition,
                        OffsetSpecification.first(),
                        2,
                        subscriptionProperties);
            assertThat(response).is(ok());
          };

      client.set(client1);
      partitions.forEach(subscriptionCallback);

      waitAtMost(
          () -> consumerStates.get(b(0)) && consumerStates.get(b(1)) && consumerStates.get(b(2)));

      Client client2 =
          cf.get(new ClientParameters().consumerUpdateListener(consumerUpdateListener));

      client.set(client2);
      partitions.forEach(subscriptionCallback);

      waitAtMost(
          () -> consumerStates.get(b(0)) && consumerStates.get(b(4)) && consumerStates.get(b(2)));

      Client client3 =
          cf.get(new ClientParameters().consumerUpdateListener(consumerUpdateListener));

      client.set(client3);
      partitions.forEach(subscriptionCallback);

      waitAtMost(
          () -> consumerStates.get(b(0)) && consumerStates.get(b(4)) && consumerStates.get(b(8)));

      Consumer<String> unsubscriptionCallback =
          partition -> {
            int subId = subscriptionCounter.getAndIncrement();
            Response response = client.get().unsubscribe(b(subId));
            assertThat(response).is(ok());
            consumerStates.put(b(subId), false);
          };

      subscriptionCounter.set(0);
      client.set(client1);
      partitions.forEach(unsubscriptionCallback);

      waitAtMost(
          () -> consumerStates.get(b(3)) && consumerStates.get(b(7)) && consumerStates.get(b(5)));

      client.set(client2);
      partitions.forEach(unsubscriptionCallback);

      waitAtMost(
          () -> consumerStates.get(b(6)) && consumerStates.get(b(7)) && consumerStates.get(b(8)));

      client.set(client3);
      partitions.forEach(unsubscriptionCallback);
    } finally {
      deleteSuperStreamTopology(configurationClient, superStream);
    }
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void killingConnectionsShouldTriggerConsumerUpdateNotification() throws Exception {
    Map<String, Boolean> consumerStates = new ConcurrentHashMap<>();
    List<String> consumerNames = IntStream.range(0, 5).mapToObj(i -> "foo-" + i).collect(toList());

    int connectionCount = Cli.listConnections().size();

    for (String consumerName : consumerNames) {
      Client c0 =
          cf.get(
              withConnectionName(consumerName + "-connection-0")
                  .consumerUpdateListener(
                      (client, subscriptionId, active) -> {
                        consumerStates.put(consumerName + "-connection-0", active);
                        return null;
                      }));
      connectionCount++;
      Client c1 =
          cf.get(
              withConnectionName(consumerName + "-connection-1")
                  .consumerUpdateListener(
                      (client, subscriptionId, active) -> {
                        consumerStates.put(consumerName + "-connection-1", active);
                        return null;
                      }));
      connectionCount++;

      Map<String, String> subscriptionProperties = new HashMap<>();
      subscriptionProperties.put("single-active-consumer", "true");
      subscriptionProperties.put("name", consumerName);

      Response response =
          c0.subscribe(b(0), stream, OffsetSpecification.first(), 2, subscriptionProperties);
      assertThat(response).is(ok());
      waitAtMost(() -> consumerStates.containsKey(consumerName + "-connection-0"));
      response = c1.subscribe(b(0), stream, OffsetSpecification.first(), 2, subscriptionProperties);
      assertThat(response).is(ok());
      response = c0.subscribe(b(1), stream, OffsetSpecification.first(), 2, subscriptionProperties);
      assertThat(response).is(ok());
    }

    int cCount = connectionCount;
    waitAtMost(() -> Cli.listConnections().size() == cCount);

    for (String consumerName : consumerNames) {
      Cli.killConnection(consumerName + "-connection-0");
      waitAtMost(
          () ->
              consumerStates.containsKey(consumerName + "-connection-1")
                  && consumerStates.get(consumerName + "-connection-1"));
    }
  }

  @Test
  void superStreamRebalancingShouldWorkWhilePublishing(TestInfo info) throws Exception {
    Map<Byte, Boolean> consumerStates = consumerStates(2);
    String superStream = streamName(info);
    String consumerName = "foo";
    Client configurationClient = cf.get();
    AtomicBoolean keepPublishing = new AtomicBoolean(true);
    try {
      declareSuperStreamTopology(configurationClient, superStream, 3);
      // we use the second partition because a rebalancing occurs
      // when the second consumer joins
      String partitionInUse = superStream + "-1";

      Client publisher = cf.get();
      publisher.declarePublisher(b(0), null, partitionInUse);
      new Thread(
              () -> {
                long rate = 50_000;
                Bucket bucket =
                    Bucket.builder()
                        .addLimit(limit -> limit.capacity(rate).refillGreedy(rate, ofSeconds(1)))
                        .build();
                while (keepPublishing.get()) {
                  try {
                    bucket.asBlocking().consume(1);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                  publisher.publish(
                      b(0),
                      Collections.singletonList(
                          publisher
                              .messageBuilder()
                              .addData("hello".getBytes(StandardCharsets.UTF_8))
                              .build()));
                }
              })
          .start();

      AtomicLong lastDispatchedOffset = new AtomicLong(0);
      ConsumerUpdateListener consumerUpdateListener =
          (client1, subscriptionId, active) -> {
            consumerStates.put(subscriptionId, active);
            return lastDispatchedOffset.get() == 0
                ? OffsetSpecification.first()
                : OffsetSpecification.offset(lastDispatchedOffset.get());
          };
      CountDownLatch receivedMessagesLatch = new CountDownLatch(100);
      MessageListener messageListener =
          (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext, message) -> {
            lastDispatchedOffset.set(offset);
            receivedMessagesLatch.countDown();
          };
      ClientParameters clientParameters =
          new ClientParameters()
              .chunkListener(TestUtils.credit())
              .messageListener(messageListener)
              .consumerUpdateListener(consumerUpdateListener);
      Client client1 = cf.get(clientParameters);
      Map<String, String> subscriptionProperties = new HashMap<>();
      subscriptionProperties.put("single-active-consumer", "true");
      subscriptionProperties.put("name", consumerName);
      subscriptionProperties.put("super-stream", superStream);
      AtomicInteger subscriptionCounter = new AtomicInteger(0);
      AtomicReference<Client> client = new AtomicReference<>();
      Consumer<String> subscriptionCallback =
          partition -> {
            Response response =
                client
                    .get()
                    .subscribe(
                        b(subscriptionCounter.getAndIncrement()),
                        partition,
                        OffsetSpecification.first(),
                        10,
                        subscriptionProperties);
            assertThat(response).is(ok());
          };

      client.set(client1);
      subscriptionCallback.accept(partitionInUse);

      waitAtMost(() -> consumerStates.get(b(0)));

      latchAssert(receivedMessagesLatch).completes();

      Client client2 = cf.get(clientParameters);

      client.set(client2);
      subscriptionCallback.accept(partitionInUse);

      waitAtMost(() -> consumerStates.get(b(1)));

      Response response = client1.unsubscribe(b(0));
      assertThat(response).is(ok());
      response = client2.unsubscribe(b(1));
      assertThat(response).is(ok());
    } finally {
      keepPublishing.set(false);
      deleteSuperStreamTopology(configurationClient, superStream);
    }
  }

  @Test
  void singleActiveConsumerMustHaveName() {
    Client client = cf.get();
    Response response =
        client.subscribe(
            b(0),
            stream,
            OffsetSpecification.first(),
            10,
            Collections.singletonMap("single-active-consumer", "true"));
    assertThat(response).is(ko()).has(responseCode(Constants.RESPONSE_CODE_PRECONDITION_FAILED));
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  @BrokerVersionAtLeast(RABBITMQ_3_11_14)
  void connectionShouldBeClosedIfConsumerUpdateTakesTooLong() throws Exception {
    Duration timeout = ofSeconds(1);
    try {
      Cli.setEnv("request_timeout", String.valueOf(timeout.getSeconds()));
      CountDownLatch shutdownLatch = new CountDownLatch(1);
      Client client =
          cf.get(
              new ClientParameters()
                  .consumerUpdateListener(
                      (c, subscriptionId, active) -> {
                        try {
                          Thread.sleep(timeout.multipliedBy(2).toMillis());
                        } catch (InterruptedException e) {
                          throw new RuntimeException(e);
                        }
                        return OffsetSpecification.first();
                      })
                  .shutdownListener(shutdownContext -> shutdownLatch.countDown()));
      Map<String, String> parameters = new HashMap<>();
      parameters.put("single-active-consumer", "true");
      parameters.put("name", "foo");
      Response response =
          client.subscribe(b(0), stream, OffsetSpecification.first(), 1, parameters);
      assertThat(response).is(ok());

      assertThat(latchAssert(shutdownLatch)).completes(timeout.multipliedBy(5));
    } finally {
      Cli.setEnv("request_timeout", "60000");
    }
  }
}

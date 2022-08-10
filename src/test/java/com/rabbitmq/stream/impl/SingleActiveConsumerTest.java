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

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SingleActiveConsumerTest {

  String stream;
  TestUtils.ClientFactory cf;

  @Test
  void secondSubscriptionShouldTakeOverAfterFirstOneUnsubscribes() throws Exception {
    Client writerClient = cf.get();
    int messageCount = 10000;
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    Map<Byte, Boolean> consumerStates = new ConcurrentHashMap<>();
    Map<Byte, AtomicInteger> receivedMessages = new ConcurrentHashMap<>();
    receivedMessages.put(b(0), new AtomicInteger(0));
    receivedMessages.put(b(1), new AtomicInteger(0));
    CountDownLatch consumerUpdateLatch = new CountDownLatch(2);
    String consumerName = "foo";
    ClientParameters clientParameters =
        new ClientParameters()
            .chunkListener(
                (client, subscriptionId, offset, msgCount, dataSize) ->
                    client.credit(subscriptionId, 1))
            .messageListener(
                (subscriptionId, offset, chunkTimestamp, committedChunkId, message) -> {
                  lastReceivedOffset.set(offset);
                  receivedMessages.get(subscriptionId).incrementAndGet();
                })
            .consumerUpdateListener(
                (client, subscriptionId, active) -> {
                  consumerStates.put(subscriptionId, active);
                  consumerUpdateLatch.countDown();
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
    latchAssert(consumerUpdateLatch).completes();
    assertThat(consumerStates)
        .hasSize(2)
        .containsEntry(b(0), Boolean.TRUE)
        .containsEntry(b(1), Boolean.FALSE);

    waitAtMost(
        () -> receivedMessages.getOrDefault(b(0), new AtomicInteger(0)).get() == messageCount);

    assertThat(lastReceivedOffset).hasPositiveValue();
    client.storeOffset(consumerName, stream, lastReceivedOffset.get());
    waitAtMost(() -> client.queryOffset(consumerName, stream).getOffset() == lastReceivedOffset.get());

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
  void consumerUpdateListenerShouldBeCalledInOrder() throws Exception {
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
      waitAtMost(
          () ->
              consumerUpdateHistory
                  .toString()
                  .contains(String.format("<%d.%b>", subscriptionId, false)));
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
    waitAtMost(() -> consumerUpdateCount.get() == 2);

    client.close();
    assertThat(consumerUpdateCount).hasValue(2);
  }
}

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SingleActiveConsumerTest {

  String stream;
  TestUtils.ClientFactory cf;

  @Test
  void subscribe() throws Exception {
    int messageCount = 10000;
    Map<Byte, Boolean> consumerStates = new ConcurrentHashMap<>();
    Map<Byte, AtomicInteger> receivedMessages = new ConcurrentHashMap<>();
    receivedMessages.put(b(0), new AtomicInteger(0));
    receivedMessages.put(b(1), new AtomicInteger(0));
    CountDownLatch consumerUpdateLatch = new CountDownLatch(2);
    ClientParameters clientParameters =
        new ClientParameters()
            .chunkListener(
                (client, subscriptionId, offset, msgCount, dataSize) ->
                    client.credit(subscriptionId, 1))
            .messageListener(
                (subscriptionId, offset, chunkTimestamp, committedChunkId, message) ->
                    receivedMessages.get(subscriptionId).incrementAndGet())
            .consumerUpdateListener(
                (client, subscriptionId, active) -> {
                  consumerStates.put(subscriptionId, active);
                  consumerUpdateLatch.countDown();
                  return null;
                });
    Client client = cf.get(clientParameters);

    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("single-active-consumer", "true");
    parameters.put("name", "foo");
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
  }
}

// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.ok;
import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class ClientFlowControlTest {

  String stream;
  TestUtils.ClientFactory cf;

  @Test
  void longProcessingShouldNotBlockOtherServerFrames(TestInfo info) {
    int messageCount = 100_000;
    AtomicInteger processedCount = new AtomicInteger(0);
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    CountDownLatch metadataLatch = new CountDownLatch(1);
    AtomicReference<String> deletedStream = new AtomicReference<>();
    Client consumerClient =
        cf.get(
            new ClientParameters()
                .chunkListener(
                    (client, subscriptionId, offset, msgCount, dataSize) ->
                        client.credit(subscriptionId, 1))
                .messageListener(
                    (subscriptionId, offset, chunkTimestamp, committedChunkId, message) -> {
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      processedCount.incrementAndGet();
                    })
                .metadataListener(
                    (stream, code) -> {
                      deletedStream.set(stream);
                      metadataLatch.countDown();
                    }));

    String toBeDeletedStream = TestUtils.streamName(info);
    assertThat(consumerClient.create(toBeDeletedStream)).is(ok());
    assertThat(
            consumerClient.subscribe(
                TestUtils.b(0), toBeDeletedStream, OffsetSpecification.first(), 1))
        .is(ok());
    assertThat(consumerClient.subscribe(TestUtils.b(1), stream, OffsetSpecification.first(), 1))
        .is(ok());

    Client deletionClient = cf.get();
    assertThat(deletionClient.delete(toBeDeletedStream)).is(ok());
    assertThat(latchAssert(metadataLatch)).completes();
    assertThat(deletedStream).hasValue(toBeDeletedStream);
    assertThat(consumerClient.unsubscribe(b(1))).is(ok());
    assertThat(processedCount).hasValueLessThan(messageCount);
  }
}

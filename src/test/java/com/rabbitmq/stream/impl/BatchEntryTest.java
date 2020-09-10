// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class BatchEntryTest {

  static final Charset UTF8 = StandardCharsets.UTF_8;

  String stream;
  TestUtils.ClientFactory cf;

  @Test
  void batchPublish() throws Exception {
    int batchCount = 100;
    int messagesInBatch = 30;
    CountDownLatch publishLatch = new CountDownLatch(batchCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    IntStream.range(0, batchCount)
        .forEach(
            batchIndex -> {
              MessageBatch messageBatch = new MessageBatch(MessageBatch.Compression.NONE);
              IntStream.range(0, messagesInBatch)
                  .forEach(
                      messageIndex -> {
                        messageBatch.add(
                            publisher
                                .messageBuilder()
                                .addData(
                                    ("batch " + batchIndex + " message " + messageIndex)
                                        .getBytes(UTF8))
                                .build());
                      });
              publisher.publishBatches(stream, (byte) 1, Collections.singletonList(messageBatch));
            });

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    Set<String> bodies = ConcurrentHashMap.newKeySet(batchCount * messagesInBatch);
    CountDownLatch consumeLatch = new CountDownLatch(batchCount * messagesInBatch);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(
                    (client, subscriptionId, offset, messageCount, dataSize) ->
                        client.credit(subscriptionId, 1))
                .messageListener(
                    (subscriptionId, offset, message) -> {
                      bodies.add(new String(message.getBodyAsBinary(), UTF8));
                      consumeLatch.countDown();
                    }));

    Client.Response response =
        consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(bodies).hasSize(batchCount * messagesInBatch);
    IntStream.range(0, batchCount)
        .forEach(
            batchIndex -> {
              IntStream.range(0, messagesInBatch)
                  .forEach(
                      messageIndex -> {
                        String expected = "batch " + batchIndex + " message " + messageIndex;
                        assertThat(bodies).contains(expected);
                      });
            });
  }
}

// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.compression.Compression;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class OutboundMappingCallbackTest {

  TestUtils.ClientFactory cf;
  String stream;

  @Test
  void publishList() throws Exception {
    int batchSize = 10;
    int batchNumber = 1000;
    int messageCount = batchSize * batchNumber;
    CountDownLatch mappingLatch = new CountDownLatch(messageCount);
    CountDownLatch confirmLatch = new CountDownLatch(messageCount);
    Set<Long> mapped = ConcurrentHashMap.newKeySet(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> confirmLatch.countDown()));
    client.declarePublisher(b(1), null, stream);
    IntStream.range(0, batchNumber)
        .forEach(
            i -> {
              List<Message> messages =
                  IntStream.range(0, batchSize)
                      .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                      .map(body -> client.messageBuilder().addData(body).build())
                      .collect(Collectors.toList());
              client.publish(
                  b(1),
                  messages,
                  (publishingId, original) -> {
                    assertThat(original).isNotNull().isInstanceOf(Message.class);
                    mapped.add(publishingId);
                    mappingLatch.countDown();
                  });
            });

    assertThat(mappingLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(mapped).hasSize(messageCount);
  }

  @Test
  void publishBatches() throws Exception {
    int subEntryCount = 10;
    int messagesInFrameCount = 100;
    int frameCount = 1000;
    CountDownLatch mappingLatch = new CountDownLatch(frameCount * messagesInFrameCount);
    CountDownLatch confirmLatch = new CountDownLatch(frameCount * messagesInFrameCount);
    Set<Long> mapped = ConcurrentHashMap.newKeySet(frameCount * messagesInFrameCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> confirmLatch.countDown()));
    client.declarePublisher(b(1), null, stream);
    IntStream.range(0, frameCount)
        .forEach(
            frameIndex -> {
              List<MessageBatch> batches = new ArrayList<>(messagesInFrameCount);
              IntStream.range(0, messagesInFrameCount)
                  .forEach(
                      batchIndex -> {
                        List<Message> messages =
                            IntStream.range(0, subEntryCount)
                                .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                                .map(body -> client.messageBuilder().addData(body).build())
                                .collect(Collectors.toList());
                        MessageBatch batch = new MessageBatch(Compression.NONE, messages);
                        batches.add(batch);
                      });

              client.publishBatches(
                  b(1),
                  batches,
                  (publishingId, original) -> {
                    assertThat(original).isNotNull().isInstanceOf(MessageBatch.class);
                    mapped.add(publishingId);
                    mappingLatch.countDown();
                  });
            });

    assertThat(mappingLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(mapped).hasSize(frameCount * messagesInFrameCount);
  }
}

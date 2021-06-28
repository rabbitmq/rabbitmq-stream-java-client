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
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.compression.CommonsCompressCompressionCodecFactory;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class CompressionTest {
  static final Charset UTF8 = StandardCharsets.UTF_8;

  String stream;
  TestUtils.ClientFactory cf;

  @Test
  void publishConsumeCompressedMessages() {
    CompressionCodecFactory compressionCodecFactory = new CommonsCompressCompressionCodecFactory();
    int batchCount = 100;
    int messagesInBatch = 30;
    CountDownLatch publishLatch = new CountDownLatch(batchCount);
    Client publisher =
        cf.get(
            new ClientParameters()
                .compressionCodecFactory(compressionCodecFactory)
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    Response response = publisher.declarePublisher(b(0), null, stream);
    assertThat(response.isOk()).isTrue();
    IntStream.range(0, batchCount)
        .forEach(
            batchIndex -> {
              MessageBatch messageBatch = new MessageBatch(Compression.GZIP);
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
              publisher.publishBatches(b(0), Collections.singletonList(messageBatch));
            });

    assertThat(latchAssert(publishLatch)).completes();

    Set<String> bodies = ConcurrentHashMap.newKeySet(batchCount * messagesInBatch);
    CountDownLatch consumeLatch = new CountDownLatch(batchCount * messagesInBatch);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .compressionCodecFactory(compressionCodecFactory)
                .chunkListener(
                    (client, subscriptionId, offset, messageCount, dataSize) ->
                        client.credit(subscriptionId, 1))
                .messageListener(
                    (subscriptionId, offset, message) -> {
                      bodies.add(new String(message.getBodyAsBinary(), UTF8));
                      consumeLatch.countDown();
                    }));

    response = consumer.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    assertThat(latchAssert(consumeLatch)).completes();
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

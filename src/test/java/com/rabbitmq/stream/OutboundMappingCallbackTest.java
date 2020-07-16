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

package com.rabbitmq.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

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
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, batchNumber).forEach(i -> {
            List<Message> messages = IntStream.range(0, batchSize)
                    .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                    .map(body -> client.messageBuilder().addData(body).build())
                    .collect(Collectors.toList());
            client.publish(stream, messages, (publishingId, original) -> {
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
    void publishSingle() throws Exception {
        int messageCount = 10_000;
        CountDownLatch mappingLatch = new CountDownLatch(messageCount);
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        Set<Long> mapped = ConcurrentHashMap.newKeySet(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, messageCount).forEach(i -> {
            client.publish(stream, client
                    .messageBuilder()
                    .addData(String.valueOf(i).getBytes())
                    .build(), (publishingId, original) -> {
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
    void publishByteArrayList() throws Exception {
        int batchSize = 10;
        int batchNumber = 1000;
        int messageCount = batchSize * batchNumber;
        CountDownLatch mappingLatch = new CountDownLatch(messageCount);
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        Set<Long> mapped = ConcurrentHashMap.newKeySet(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, batchNumber).forEach(i -> {
            List<byte[]> messages = IntStream.range(0, batchSize)
                    .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                    .collect(Collectors.toList());
            client.publishBinary(stream, messages, (publishingId, original) -> {
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
    void publishByteArray() throws Exception {
        int messageCount = 10_000;
        CountDownLatch mappingLatch = new CountDownLatch(messageCount);
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        Set<Long> mapped = ConcurrentHashMap.newKeySet(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, messageCount).forEach(i -> {
            client.publish(stream, String.valueOf(i).getBytes(), (publishingId, original) -> {
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
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, frameCount).forEach(frameIndex -> {
            List<MessageBatch> batches = new ArrayList<>(messagesInFrameCount);
            IntStream.range(0, messagesInFrameCount).forEach(batchIndex -> {
                List<Message> messages = IntStream.range(0, subEntryCount)
                        .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                        .map(body -> client.messageBuilder().addData(body).build())
                        .collect(Collectors.toList());
                MessageBatch batch = new MessageBatch(MessageBatch.Compression.NONE, messages);
                batches.add(batch);
            });

            client.publishBatches(stream, batches, (publishingId, original) -> {
                assertThat(original).isNotNull().isInstanceOf(MessageBatch.class);
                mapped.add(publishingId);
                mappingLatch.countDown();
            });
        });

        assertThat(mappingLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(mapped).hasSize(frameCount * messagesInFrameCount);
    }

    @Test
    void publishBatch() throws Exception {
        int subEntryCount = 100;
        int frameCount = 1000;
        CountDownLatch mappingLatch = new CountDownLatch(frameCount);
        CountDownLatch confirmLatch = new CountDownLatch(frameCount);
        Set<Long> mapped = ConcurrentHashMap.newKeySet(frameCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> confirmLatch.countDown()));
        IntStream.range(0, frameCount).forEach(frameIndex -> {
            List<Message> messages = IntStream.range(0, subEntryCount)
                    .mapToObj(messageIndex -> String.valueOf(messageIndex).getBytes())
                    .map(body -> client.messageBuilder().addData(body).build())
                    .collect(Collectors.toList());
            MessageBatch batch = new MessageBatch(MessageBatch.Compression.NONE, messages);

            client.publishBatch(stream, batch, (publishingId, original) -> {
                assertThat(original).isNotNull().isInstanceOf(MessageBatch.class);
                mapped.add(publishingId);
                mappingLatch.countDown();
            });
        });


        assertThat(mappingLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(mapped).hasSize(frameCount);
    }

}

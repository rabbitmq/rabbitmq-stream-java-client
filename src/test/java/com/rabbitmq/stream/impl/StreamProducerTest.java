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

import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamProducerTest {

    String stream;
    EventLoopGroup eventLoopGroup;

    Environment environment;

    TestUtils.ClientFactory cf;

    @BeforeEach
    void init() {
        environment = Environment.builder()
                .eventLoopGroup(eventLoopGroup)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        environment.close();
    }

    @Test
    void send() throws Exception {
        int batchSize = 100;
        int messageCount = 10_000 * batchSize + 1; // don't want a multiple of batch size
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Producer producer = environment.producerBuilder()
                .stream(stream)
                .batchSize(batchSize)
                .build();
        AtomicLong count = new AtomicLong(0);
        IntStream.range(0, messageCount).forEach(i -> {
            producer.send(producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {
                count.incrementAndGet();
                publishLatch.countDown();
            });
        });
        boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
    }

    @Test
    void sendWithSubEntryBatches() throws Exception {
        int batchSize = 100;
        int messagesInBatch = 10;
        int messageCount = 1_000 * batchSize + 1; // don't want a multiple of batch size
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Producer producer = environment.producerBuilder()
                .stream(stream)
                .subEntrySize(messagesInBatch)
                .batchSize(batchSize)
                .build();
        IntStream.range(0, messageCount).forEach(i -> {
            producer.send(producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {
                publishLatch.countDown();
            });
        });
        boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
    }

    @Test
    void sendToNonExistingStreamShouldReturnUnconfirmedStatus() throws Exception {
        Client client = cf.get();
        String s = UUID.randomUUID().toString();
        Client.Response response = client.create(s);
        assertThat(response.isOk()).isTrue();

        Producer producer = environment.producerBuilder()
                .stream(s)
                .build();

        response = client.delete(s);
        assertThat(response.isOk()).isTrue();

        CountDownLatch confirmationLatch = new CountDownLatch(1);
        AtomicReference<ConfirmationStatus> confirmationStatusReference = new AtomicReference<>();
        producer.send(producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {
            confirmationStatusReference.set(confirmationStatus);
            confirmationLatch.countDown();
        });

        assertThat(confirmationLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(confirmationStatusReference.get()).isNotNull();
        assertThat(confirmationStatusReference.get().isConfirmed()).isFalse();
        assertThat(confirmationStatusReference.get().getCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }

}

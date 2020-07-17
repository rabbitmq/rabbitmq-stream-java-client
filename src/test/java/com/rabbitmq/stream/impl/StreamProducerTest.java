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

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamProducerTest {

    String stream;
    EventLoopGroup eventLoopGroup;

    Environment environment;

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
                .batchSize(100)
                .build();
        IntStream.range(0, messageCount).forEach(i -> {
            producer.send(producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {
                publishLatch.countDown();
            });
        });
        boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
    }

}

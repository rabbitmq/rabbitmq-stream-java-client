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

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamConsumerTest {

    String stream;
    EventLoopGroup eventLoopGroup;
    TestUtils.ClientFactory cf;

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
    void consume() throws Exception {
        int messageCount = 100_000;
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> publishLatch.countDown()));

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream,
                Collections.singletonList(client.messageBuilder().addData("".getBytes()).build())));

        assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);

        Consumer consumer = environment.consumerBuilder()
                .stream(stream)
                .messageHandler((offset, message) -> consumeLatch.countDown())
                .build();

        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

        consumer.close();
    }

}

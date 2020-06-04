// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@Disabled
public class AmqpInteroperabilityTest {

    String stream;
    TestUtils.ClientFactory cf;

    @Test
    void publishToStreamQueueConsumeFromStream() throws Exception {
        int messageCount = 10000;
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try (Connection amqpConnection = connectionFactory.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.confirmSelect();
            for (int i = 0; i < messageCount; i++) {
                c.basicPublish("", stream, null, ("amqp " + i).getBytes(StandardCharsets.UTF_8));
            }
            c.waitForConfirmsOrDie(10_000);
        }
        CountDownLatch consumedLatch = new CountDownLatch(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> consumedLatch.countDown())
        );

        client.subscribe(1, stream, OffsetSpecification.first(), 10);
        assertThat(consumedLatch.await(10, SECONDS)).isTrue();
    }

}

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

import com.rabbitmq.stream.impl.Client;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultEnvironmentTest {

    static EventLoopGroup eventLoopGroup;

    @BeforeAll
    static void initAll() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void afterAll() throws Exception {
        eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }

    @Test
    void defaultEnvironmentCanBeInstantiatedAndCanConnect() throws Exception {
        String stream = UUID.randomUUID().toString();
        try (Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup))) {
            Client.Response response = client.create(stream);
            assertThat(response.isOk()).isTrue();
            try (Environment environment = Environment.builder().eventLoopGroup(eventLoopGroup).build()) {
                environment.producerBuilder().stream(stream);
            } finally {
                response = client.delete(stream);
                assertThat(response.isOk()).isTrue();
            }
        }
    }

}

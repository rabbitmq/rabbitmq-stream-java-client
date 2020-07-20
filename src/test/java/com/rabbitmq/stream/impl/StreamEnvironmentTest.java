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

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamEnvironmentTest {

    static EventLoopGroup eventLoopGroup;

    EnvironmentBuilder environmentBuilder;

    @BeforeAll
    static void initAll() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void afterAll() throws Exception {
        eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }

    @BeforeEach
    void init() {
        environmentBuilder = Environment.builder();
        environmentBuilder.eventLoopGroup(eventLoopGroup);
    }

    @Test
    void environmentCreationShouldFailWithIncorrectCredentialsInUri() {
        assertThatThrownBy(() -> environmentBuilder
                .uri("rabbitmq-stream://bad:credentials@localhost:5555")
                .build()).isInstanceOf(AuthenticationFailureException.class);
    }

    @Test
    void environmentCreationShouldFailWithIncorrectVirtualHostInUri() {
        assertThatThrownBy(() -> environmentBuilder
                .uri("rabbitmq-stream://guest:guest@localhost:5555/dummy")
                .build()).isInstanceOf(StreamException.class)
                .hasMessageContaining(String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE));
    }

    @Test
    void environmentCreationShouldSucceedWithUrlContainingAllCorrectInformation() throws Exception {
        environmentBuilder
                .uri("rabbitmq-stream://guest:guest@localhost:5555/%2f")
                .build()
                .close();
    }

    @Test
    void createDelete() throws Exception {
        try (Environment environment = environmentBuilder.build();
             Client client = new Client()) {

            String s = UUID.randomUUID().toString();
            environment.streamCreator().stream(s).create();
            Client.StreamMetadata metadata = client.metadata(s).get(s);
            assertThat(metadata.isResponseOk()).isTrue();
            environment.deleteStream(s);
            metadata = client.metadata(s).get(s);
            assertThat(metadata.isResponseOk()).isFalse();
            assertThat(metadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        }
    }

}

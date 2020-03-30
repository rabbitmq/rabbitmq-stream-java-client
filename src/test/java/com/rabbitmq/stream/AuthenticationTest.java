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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class AuthenticationTest {

    static EventLoopGroup eventLoopGroup;

    @BeforeAll
    static void initSuite() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void tearDownSuite() throws Exception {
        eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }

    Client client(Client.ClientParameters parameters) {
        Client client = new Client(parameters.eventLoopGroup(eventLoopGroup));
        return client;
    }

    @Test
    void authenticateShouldPassWithValidCredentials() {
        client(new Client.ClientParameters());
    }

    @Test
    void authenticateWithJdkSaslConfiguration() {
        client(new Client.ClientParameters().saslConfiguration(new JdkSaslConfiguration(
                new DefaultUsernamePasswordCredentialsProvider("guest", "guest"), () -> "localhost"
        )));
    }

    @Test
    void authenticateShouldFailWhenUsingBadCredentials() {
        try {
            client(new Client.ClientParameters().username("bad").password("bad"));
        } catch (AuthenticationFailureException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE)));
        }
    }

    @Test
    void authenticateShouldFailWhenUsingUnsupportedSaslMechanism() {
        try {
            client(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
                @Override
                public String getName() {
                    return "FANCY-SASL";
                }

                @Override
                public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
                    return new byte[0];
                }
            }));
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED)));
        }
    }

    @Test
    void authenticateShouldFailWhenSendingGarbageToSaslChallenge() {
        try {
            client(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
                @Override
                public String getName() {
                    return PlainSaslMechanism.INSTANCE.getName();
                }

                @Override
                public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
                    return "blabla".getBytes(StandardCharsets.UTF_8);
                }
            }));
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_SASL_ERROR)));
        }
    }

    @Test
    void accessToNonExistingVirtualHostShouldFail() {
        try {
            client(new Client.ClientParameters().virtualHost(UUID.randomUUID().toString()));
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE)));
        }
    }


}

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

import com.rabbitmq.stream.sasl.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class AuthenticationTest {

    TestUtils.ClientFactory cf;

    @Test
    void authenticateShouldPassWithValidCredentials() {
        cf.get(new Client.ClientParameters());
    }

    @Test
    void authenticateWithJdkSaslConfiguration() {
        cf.get(new Client.ClientParameters().saslConfiguration(new JdkSaslConfiguration(
                new DefaultUsernamePasswordCredentialsProvider("guest", "guest"), () -> "localhost"
        )));
    }

    @Test
    void authenticateShouldFailWhenUsingBadCredentials() {
        try {
            cf.get(new Client.ClientParameters().username("bad").password("bad"));
        } catch (AuthenticationFailureException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE)));
        }
    }

    @Test
    void authenticateShouldFailWhenUsingUnsupportedSaslMechanism() {
        try {
            cf.get(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
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
            cf.get(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
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
            cf.get(new Client.ClientParameters().virtualHost(UUID.randomUUID().toString()));
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE)));
        }
    }


}

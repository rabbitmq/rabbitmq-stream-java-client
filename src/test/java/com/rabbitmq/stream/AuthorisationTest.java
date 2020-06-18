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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@TestUtils.DisabledIfRabbitMqCtlNotSet
public class AuthorisationTest {

    private static final String VH = "test_stream";
    private static final String USERNAME = "stream";
    private static final String PASSWORD = "stream";
    TestUtils.ClientFactory cf;

    @BeforeAll
    static void init() throws Exception {
        Host.rabbitmqctl("add_vhost " + VH);
        Host.rabbitmqctl("add_user " + USERNAME + " " + PASSWORD);
        Host.rabbitmqctl("set_permissions --vhost " + VH + " " + USERNAME + " '^stream.*$' '^stream.*$' '^stream.*$'");
        Host.rabbitmqctl("set_permissions --vhost " + VH + " guest '.*' '.*' '.*'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        Host.rabbitmqctl("delete_user stream");
        Host.rabbitmqctl("delete_vhost test_stream");
    }

    @Test
    void createStreamWithAuthorisedNameShouldSucceed() throws Exception {
        Client deletionClient = cf.get(new Client.ClientParameters().virtualHost(VH));
        Client client = client();
        IntStream.range(0, 30).forEach(i -> {
            String stream = "stream-authorized" + i;
            Client.Response response = client.create(stream);
            assertThat(response.isOk()).isTrue();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

            response = deletionClient.delete(stream);
            assertThat(response.isOk()).isTrue();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
        });
    }

    @Test
    void createStreamWithUnauthorisedNameShouldFail() {
        Client client = client();
        IntStream.range(0, 30).forEach(i -> {
            Client.Response response = client.create("not-authorized" + i);
            assertThat(response.isOk()).isFalse();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);
        });
    }

    @Test
    void deleteStreamWithAuthorisedNameShouldSucceed() {
        Client creationClient = cf.get(new Client.ClientParameters().virtualHost(VH));
        Client client = client();
        IntStream.range(0, 30).forEach(i -> {
            String stream = "stream-authorized" + i;
            Client.Response response = creationClient.create(stream);
            assertThat(response.isOk()).isTrue();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

            response = client.delete(stream);
            assertThat(response.isOk()).isTrue();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
        });
    }

    @Test
    void deleteStreamWithUnauthorisedNameShouldFail() {
        Client creationClient = cf.get(new Client.ClientParameters().virtualHost(VH));
        Client client = client();
        IntStream.range(0, 30).forEach(i -> {
            String stream = "not-authorized" + i;
            Client.Response response = creationClient.create(stream);
            assertThat(response.isOk()).isTrue();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

            response = client.delete(stream);
            assertThat(response.isOk()).isFalse();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);
        });
    }

    Client client() {
        return cf.get(new Client.ClientParameters().virtualHost(VH).username(USERNAME).password(PASSWORD));
    }

}

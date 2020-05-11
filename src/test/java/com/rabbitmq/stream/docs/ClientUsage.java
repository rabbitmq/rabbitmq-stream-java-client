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

package com.rabbitmq.stream.docs;

import com.rabbitmq.stream.Client;

public class ClientUsage {

    void connect() {
        // tag::client-creation[]
        Client client = new Client(); // <1>
        // end::client-creation[]
    }

    void connectWithClientParameters() {
        // tag::client-creation-with-client-parameters[]
        Client client = new Client(new Client.ClientParameters() // <1>
                .host("my-rabbitmq-stream")
                .port(1234)
                .username("stream-user")
                .password("stream-password")
        );
        // end::client-creation-with-client-parameters[]
    }

    void createStream() {
        Client client = new Client();
        // tag::stream-creation[]
        Client.Response response = client.create("my-stream"); // <1>
        if (!response.isOk()) { // <2>
            response.getResponseCode(); // <3>
        }
        // end::stream-creation[]
    }

    void deleteStream() {
        Client client = new Client();
        // tag::stream-deletion[]
        Client.Response response = client.delete("my-stream"); // <1>
        if (!response.isOk()) { // <2>
            response.getResponseCode(); // <3>
        }
        // end::stream-deletion[]
    }

}

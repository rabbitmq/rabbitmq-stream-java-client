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

package com.rabbitmq.stream.docs;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Client;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
                .virtualHost("my-virtual-host")
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

    void createStreamWithRetention() {
        Client client = new Client();
        // tag::stream-creation-retention[]
        client.create("my-stream", new Client.StreamParametersBuilder()
                .maxLengthGb(10) // <1>
                .maxSegmentSizeMb(500) // <2>
                .build()
        );
        // end::stream-creation-retention[]
    }

    void createStreamWithRetentionAlternative() {
        Client client = new Client();
        // tag::stream-creation-retention-alternative[]
        client.create("my-stream", new Client.StreamParametersBuilder()
                .maxLengthBytes(ByteCapacity.GB(10)) // <1>
                .maxSegmentSizeBytes(ByteCapacity.MB(500)) // <2>
                .build()
        );
        // end::stream-creation-retention-alternative[]
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

    void publishSimple() {
        Client client = new Client();
        // tag::publish-simple[]
        byte[] messagePayload = "hello".getBytes(StandardCharsets.UTF_8);  // <1>
        List<Long> publishingIds = client.publish(
                "my-stream",  // <2>
                Collections.singletonList(client.messageBuilder().addData(messagePayload).build())  // <3>
        );
        // end::publish-simple[]
    }

    void publishMultiple() {
        Client client = new Client();
        // tag::publish-multiple[]
        List<Message> messages = IntStream.range(0, 9)
                .mapToObj(i -> ("hello" + i).getBytes(StandardCharsets.UTF_8))
                .map(body -> client.messageBuilder().addData(body).build())
                .collect(Collectors.toList());  // <1>

        List<Long> publishingIds = client.publish(
                "my-stream",  // <2>
                messages  // <3>
        );
        // end::publish-multiple[]
    }

    void publishConfirmCallback() {
        // tag::publish-confirm-callback[]
        Client client = new Client(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> {  // <1>
                    // map publishing ID with initial outbound message
                })
        );
        // end::publish-confirm-callback[]
    }

    void publishErrorCallback() {
        // tag::publish-error-callback[]
        Client client = new Client(new Client.ClientParameters()
                .publishErrorListener((publishingId, errorCode) -> {  // <1>
                    // map publishing ID with initial outbound message
                })
        );
        // end::publish-error-callback[]
    }

    void subscribe() {
        Client client = new Client();
        // tag::subscribe[]
        Client.Response response = client.subscribe(
                1,  // <1>
                "my-stream",  // <2>
                OffsetSpecification.first(),  // <3>
                10  // <4>
        );
        if (!response.isOk()) {  // <5>
            response.getResponseCode();  // <6>
        }
        // end::subscribe[]
    }

    void consume() {
        // tag::consume[]
        Client consumer = new Client(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, messageCount, dataSize) -> {  // <1>
                    client.credit(subscriptionId, 1);  // <2>
                })
                .messageListener((subscriptionId, offset, message) -> {  // <3>
                    message.getBodyAsBinary();  // <4>
                }));
        // end::consume[]
    }

    void messageCreation() {
        Client client = new Client();
        // tag::message-creation[]
        Message message = client.messageBuilder()  // <1>
                .properties()  // <2>
                    .messageId(UUID.randomUUID())
                    .correlationId(UUID.randomUUID())
                    .contentType("text/plain")
                .messageBuilder()  // <3>
                    .addData("hello".getBytes(StandardCharsets.UTF_8))  // <4>
                .build();  // <5>
        List<Long> publishingId = client.publish("my-stream", Collections.singletonList(message));  // <6>
        // end::message-creation[]
    }

}

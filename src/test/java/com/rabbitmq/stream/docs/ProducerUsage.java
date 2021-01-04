// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ProducerUsage {

    void producerCreation() throws Exception {
        Environment environment = Environment.builder().build();
        // tag::producer-creation[]
        Producer producer = environment.producerBuilder()  // <1>
                .stream("my-stream")  // <2>
                .build();  // <3>
        // ...
        producer.close();  // <4>
        // end::producer-creation[]
    }

    void producerPublish() {
        Environment environment = Environment.builder().build();
        Producer producer = environment.producerBuilder()
                .stream("my-stream")
                .build();
        // tag::producer-publish[]
        byte[] messagePayload = "hello".getBytes(StandardCharsets.UTF_8);  // <1>
        producer.send(
                producer.messageBuilder().addData(messagePayload).build(),  // <2>
                confirmationStatus -> {  // <3>
                    if (confirmationStatus.isConfirmed()) {
                        // the message made it to the broker
                    } else {
                        // the message did not make it to the broker
                    }
                });
        // end::producer-publish[]
    }

    void producerComplexMessage() {
        Environment environment = Environment.builder().build();
        Producer producer = environment.producerBuilder()
                .stream("my-stream")
                .build();
        // tag::producer-publish-complex-message[]
        Message message = producer.messageBuilder()  // <1>
                .properties()  // <2>
                    .messageId(UUID.randomUUID())
                    .correlationId(UUID.randomUUID())
                    .contentType("text/plain")
                .messageBuilder()  // <3>
                    .addData("hello".getBytes(StandardCharsets.UTF_8))  // <4>
                .build();  // <5>
        producer.send(message, confirmationStatus -> { }); // <6>
        // end::producer-publish-complex-message[]
    }


}

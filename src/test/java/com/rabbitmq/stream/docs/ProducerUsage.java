// Copyright (c) 2020-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.compression.Compression;

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

    void producerWithName() {
        Environment environment = Environment.builder().build();
        // tag::producer-with-name[]
        Producer producer = environment.producerBuilder()
            .name("my-app-producer")  // <1>
            .confirmTimeout(Duration.ZERO)  // <2>
            .stream("my-stream")
            .build();
        // end::producer-with-name[]
        // tag::message-with-publishing-id[]
        Message message = producer.messageBuilder()
            .publishingId(1)  // <1>
            .addData("hello".getBytes(StandardCharsets.UTF_8))
            .build();
        producer.send(message, confirmationStatus -> { });
        // end::message-with-publishing-id[]
    }

    void producerWithNameQueryLastPublishingId() {
        Environment environment = Environment.builder().build();
        // tag::producer-queries-last-publishing-id[]
        Producer producer = environment.producerBuilder()
            .name("my-app-producer")  // <1>
            .confirmTimeout(Duration.ZERO)  // <2>
            .stream("my-stream")
            .build();
        long nextPublishingId = producer.getLastPublishingId() + 1;  // <3>
        while (moreContent(nextPublishingId)) {
            byte[] content = getContent(nextPublishingId); // <4>
            Message message = producer.messageBuilder()
                .publishingId(nextPublishingId) // <5>
                .addData(content)
                .build();
            producer.send(message, confirmationStatus -> {});
            nextPublishingId++;
        }
        // end::producer-queries-last-publishing-id[]
    }

    void producerSubEntryBatching() {
        Environment environment = Environment.builder().build();
        // tag::producer-sub-entry-batching[]
        Producer producer = environment.producerBuilder()
            .stream("my-stream")
            .batchSize(100) // <1>
            .subEntrySize(10) // <2>
            .build();
        // end::producer-sub-entry-batching[]
    }

    void producerSubEntryBatchingCompression() {
        Environment environment = Environment.builder().build();
        // tag::producer-sub-entry-batching-and-compression[]
        Producer producer = environment.producerBuilder()
            .stream("my-stream")
            .batchSize(100) // <1>
            .subEntrySize(10) // <2>
            .compression(Compression.ZSTD) // <3>
            .build();
        // end::producer-sub-entry-batching-and-compression[]
    }

    boolean moreContent(long publishingId) {
        return true;
    }

    byte [] getContent(long publishingId) {
        return null;
    }


}

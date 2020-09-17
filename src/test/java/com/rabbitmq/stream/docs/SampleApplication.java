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

// tag::sample-imports[]
import com.rabbitmq.stream.*;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
// end::sample-imports[]

public class SampleApplication {

    public static void main(String[] args) throws Exception {
        // tag::sample-environment[]
        System.out.println("Connecting...");
        Environment environment = Environment.builder().build();  // <1>
        String stream = UUID.randomUUID().toString();
        environment.streamCreator().stream(stream).create();  // <2>
        // end::sample-environment[]
        // tag::sample-publisher[]
        System.out.println("Starting publishing...");
        int messageCount = 10000;
        CountDownLatch publishConfirmLatch = new CountDownLatch(messageCount);
        Producer producer = environment.producerBuilder()  // <1>
                .stream(stream)
                .build();
        IntStream.range(0, messageCount)
                .forEach(i -> producer.send(  // <2>
                        producer.messageBuilder()                    // <3>
                            .addData(String.valueOf(i).getBytes())   // <3>
                            .build(),                                // <3>
                        confirmationStatus -> publishConfirmLatch.countDown()  // <4>
                ));
        publishConfirmLatch.await(10, TimeUnit.SECONDS);  // <5>
        producer.close();  // <6>
        System.out.printf("Published %,d messages%n", messageCount);
        // end::sample-publisher[]

        // tag::sample-consumer[]
        System.out.println("Starting consuming...");
        AtomicLong sum = new AtomicLong(0);
        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Consumer consumer = environment.consumerBuilder()  // <1>
                .stream(stream)
                .messageHandler((offset, message) -> {  // <2>
                    sum.addAndGet(Long.parseLong(new String(message.getBodyAsBinary())));  // <3>
                    consumeLatch.countDown();  // <4>
                })
                .build();

        consumeLatch.await(10, TimeUnit.SECONDS);  // <5>

        System.out.println("Sum: " + sum.get());  // <6>

        consumer.close();  // <7>
        // end::sample-consumer[]

        // tag::sample-environment-close[]
        environment.deleteStream(stream);  // <1>
        environment.close();  // <2>
        // end::sample-environment-close[]
    }


}

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
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.OffsetSpecification;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
// end::sample-imports[]

public class SampleApplication {

    public static void main(String[] args) throws Exception {
        // tag::sample-publisher[]
        System.out.println("Starting publishing...");
        int messageCount = 10000;
        String stream = UUID.randomUUID().toString();
        CountDownLatch publishConfirmLatch = new CountDownLatch(messageCount);
        Client publisher = new Client(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> publishConfirmLatch.countDown()));  // <1>
        publisher.create(stream);  // <2>
        IntStream.range(0, messageCount)
                .forEach(i -> publisher.publish(stream,
                        Collections.singletonList(publisher.messageBuilder().addData(String.valueOf(i).getBytes()).build())));  // <3>
        publishConfirmLatch.await(10, TimeUnit.SECONDS);  // <4>
        publisher.close();  // <5>
        System.out.printf("Published %,d messages%n", messageCount);
        // end::sample-publisher[]

        // tag::sample-consumer[]
        System.out.println("Starting consuming...");
        AtomicLong sum = new AtomicLong(0);
        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Client consumer = new Client(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, messagesInChunk, dataSize) ->
                        client.credit(subscriptionId, 1))  // <1>
                .messageListener((subscriptionId, offset, message) -> {
                    sum.addAndGet(Long.parseLong(new String(message.getBodyAsBinary())));  // <2>
                    consumeLatch.countDown();  // <3>
                }));

        consumer.subscribe(1, stream, OffsetSpecification.first(), 10);  // <4>
        consumeLatch.await(10, TimeUnit.SECONDS);  // <5>

        System.out.println("Sum: " + sum.get());  // <6>

        consumer.delete(stream);  // <7>
        consumer.close();  // <8>
        // end::sample-consumer[]
    }


}

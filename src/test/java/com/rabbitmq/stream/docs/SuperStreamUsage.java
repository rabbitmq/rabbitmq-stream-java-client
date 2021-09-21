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

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;

public class SuperStreamUsage {

    void producerSimple() {
        Environment environment = Environment.builder().build();
        // tag::producer-simple[]
        Producer producer = environment.producerBuilder()
                .stream("invoices")  // <1>
                .routing(message -> message.getProperties().getMessageIdAsString()) // <2>
                .producerBuilder()
                .build();  // <3>
        // ...
        producer.close();  // <4>
        // end::producer-simple[]
    }

    void producerCustomHashFunction() {
        Environment environment = Environment.builder().build();
        // tag::producer-custom-hash-function[]
        Producer producer = environment.producerBuilder()
            .stream("invoices")
            .routing(message -> message.getProperties().getMessageIdAsString())
            .hash(rk -> rk.hashCode())  // <1>
            .producerBuilder()
            .build();
        // end::producer-custom-hash-function[]
    }

    void producerKeyRoutingStrategy() {
        Environment environment = Environment.builder().build();
        // tag::producer-key-routing-strategy[]
        Producer producer = environment.producerBuilder()
            .stream("invoices")
            .routing(msg -> msg.getApplicationProperties().get("region").toString())  // <1>
            .key()  // <2>
            .producerBuilder()
            .build();
        // end::producer-key-routing-strategy[]
    }
}

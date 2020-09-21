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

import com.rabbitmq.stream.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConsumerUsage {

    void consumerCreation() throws Exception {
        Environment environment = Environment.builder().build();
        // tag::producer-creation[]
        Consumer consumer = environment.consumerBuilder()  // <1>
                .stream("my-stream")  // <2>
                .offset(OffsetSpecification.first())  // <3>
                .messageHandler((offset, message) -> {
                    message.getBodyAsBinary(); // <4>
                })
                .build();  // <5>
        // ...
        consumer.close();  // <6>
        // end::producer-creation[]
    }

    void offsetTracking() {
      Environment environment = Environment.builder().build();
      // tag::offset-tracking[]
      int commitEvery = 100;   // <1>
      AtomicInteger consumedMessageCount = new AtomicInteger();

      Consumer consumer =
          environment.consumerBuilder()
              .stream("my-stream")
              .name("application-1")   // <2>
              .messageHandler(
                  (context, message) -> {
                    // business code
                    // ...

                    consumedMessageCount.incrementAndGet();   // <3>
                    if (consumedMessageCount.get() % commitEvery == 0) {
                      context.commit();   // <4>
                    }
                  })
              .build();

      // end::offset-tracking[]
    }

}

// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.RoutingStrategy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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

   void producerCustomRoutingStrategy() {
       Environment environment = Environment.builder().build();
       // tag::producer-custom-routing-strategy[]
       AtomicLong messageCount = new AtomicLong(0);
       RoutingStrategy routingStrategy = (message, metadata) -> {
           List<String> partitions = metadata.partitions();
           String stream = partitions.get(
               (int) messageCount.getAndIncrement() % partitions.size()
           );
           return Collections.singletonList(stream);
       };
       Producer producer = environment.producerBuilder()
           .stream("invoices")
           .routing(null)  // <1>
           .strategy(routingStrategy)  // <2>
           .producerBuilder()
           .build();
       // end::producer-custom-routing-strategy[]
   }

   void consumerSimple() {
       Environment environment = Environment.builder().build();
       // tag::consumer-simple[]
       Consumer consumer = environment.consumerBuilder()
           .superStream("invoices")  // <1>
           .messageHandler((context, message) -> {
               // message processing
           })
           .build();
       // ...
       consumer.close();  // <2>
       // end::consumer-simple[]
   }

    void sacSimple() {
        Environment environment = Environment.builder().build();
        // tag::sac-simple[]
        Consumer consumer = environment.consumerBuilder()
            .superStream("invoices")  // <1>
            .name("application-1")  // <2>
            .singleActiveConsumer()  // <3>
            .messageHandler((context, message) -> {
                // message processing
            })
            .build();
        // ...
        // end::sac-simple[]
    }

    void sacManualOffsetTracking() {
        Environment environment = Environment.builder().build();
        long lastProcessedOffsetForThisStream = 0;
        // tag::sac-manual-offset-tracking[]
        Consumer consumer =
            environment.consumerBuilder()
                .superStream("invoices")  // <1>
                .name("application-1")  // <2>
                .singleActiveConsumer()  // <3>
                .manualTrackingStrategy()  // <4>
                .builder()
                .consumerUpdateListener(context -> {  // <5>
                   if(context.isActive()) {  // <6>
                       try {
                           return OffsetSpecification.offset(
                               context.consumer().storedOffset() + 1
                           );
                       } catch (NoOffsetException e) {
                           return OffsetSpecification.next();
                       }
                   } else {
                       context.consumer().store(lastProcessedOffsetForThisStream);  // <7>
                       return null;
                   }
                })
                .messageHandler((context, message) -> {
                    // message handling code...

                    if (conditionToStore()) {
                        context.storeOffset();  // <8>
                    }
                })
                .build();
        // ...
        // end::sac-manual-offset-tracking[]
    }

    boolean conditionToStore() {
        return true;
    }

  void sacExternalOffsetTracking() {
    Environment environment = Environment.builder().build();
    // tag::sac-external-offset-tracking[]
    Consumer consumer = environment.consumerBuilder()
        .superStream("invoices")  // <1>
        .name("application-1")  // <2>
        .singleActiveConsumer()  // <3>
        .manualTrackingStrategy()  // <4>
        .builder()
        .consumerUpdateListener(context -> {  // <5>
          if (context.isActive()) {  // <6>
            long offset = getOffsetFromExternalStore();
            return OffsetSpecification.offset(offset + 1);
          }
          return null;  // <7>
        })
        .messageHandler((context, message) -> {
          // message handling code...

          storeOffsetInExternalStore(context.stream(), context.offset());  // <8>
        })
        .build();
    // end::sac-external-offset-tracking[]
  }

  void storeOffsetInExternalStore(String stream, long offset) {

  }

  long getOffsetFromExternalStore() {
    return 0L;
  }
}

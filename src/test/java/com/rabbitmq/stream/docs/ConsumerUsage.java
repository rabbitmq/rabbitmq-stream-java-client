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

import com.rabbitmq.stream.*;
import java.time.Duration;
import org.assertj.core.data.Offset;

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

    void defaultAutoTracking() {
      Environment environment = Environment.builder().build();
      // tag::auto-tracking-defaults[]
      Consumer consumer =
          environment.consumerBuilder()
              .stream("my-stream")
              .name("application-1")   // <1>
              .autoTrackingStrategy()   // <2>
              .builder()
              .messageHandler((context, message) -> {
                // message handling code...
              })
              .build();
      // end::auto-tracking-defaults[]
    }

  void autoTrackingWithSettings() {
    Environment environment = Environment.builder().build();
    // tag::auto-tracking-with-settings[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .autoTrackingStrategy()   // <2>
                .messageCountBeforeStorage(50_000)   // <3>
                .flushInterval(Duration.ofSeconds(10))   // <4>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...
            })
            .build();
    // end::auto-tracking-with-settings[]
  }

  void autoTrackingOnlyWithName() {
    Environment environment = Environment.builder().build();
    // tag::auto-tracking-only-with-name[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .messageHandler((context, message) -> {
              // message handling code...
            })
            .build();
    // end::auto-tracking-only-with-name[]
  }

  void manualTrackingDefaults() {
    Environment environment = Environment.builder().build();
    // tag::manual-tracking-defaults[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .manualTrackingStrategy()   // <2>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...

              if (conditionToStore()) {
                context.storeOffset();   // <3>
              }
            })
            .build();
    // end::manual-tracking-defaults[]
  }

  boolean conditionToStore() {
      return true;
  }

  void manualTrackingWithSettings() {
    Environment environment = Environment.builder().build();
    // tag::manual-tracking-with-settings[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .manualTrackingStrategy()   // <2>
                .checkInterval(Duration.ofSeconds(10))   // <3>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...

              if (conditionToStore()) {
                context.storeOffset();   // <4>
              }
            })
            .build();
    // end::manual-tracking-with-settings[]
  }

  void subscriptionListener() {
    Environment environment = Environment.builder().build();
    // tag::subscription-listener[]
    Consumer consumer = environment.consumerBuilder()
        .stream("my-stream")
        .subscriptionListener(subscriptionContext -> {  // <1>
            long offset = getOffsetFromExternalStore();  // <2>
            subscriptionContext.offsetSpecification(OffsetSpecification.offset(offset + 1));  // <3>
        })
        .messageHandler((context, message) -> {
            // message handling code...

            storeOffsetInExternalStore(context.offset());  // <4>
        })
        .build();
    // end::subscription-listener[]
  }

  void storeOffsetInExternalStore(long offset) {

  }

  long getOffsetFromExternalStore() {
      return 0L;
  }

  void enablingSingleActiveConsumer() {
    Environment environment = Environment.builder().build();
    // tag::enabling-single-active-consumer[]
    Consumer consumer = environment.consumerBuilder()
        .stream("my-stream")
        .name("application-1")  // <1>
        .singleActiveConsumer()  // <2>
        .messageHandler((context, message) -> {
          // message handling code...
        })
        .build();
    // end::enabling-single-active-consumer[]
  }

  void sacConsumerUpdateListener() {
    Environment environment = Environment.builder().build();
    // tag::sac-consumer-update-listener[]
    Consumer consumer = environment.consumerBuilder()
        .stream("my-stream")
        .name("application-1")  // <1>
        .singleActiveConsumer()  // <2>
        .consumerUpdateListener(context -> {  // <3>
          long offset = getOffsetFromExternalStore();  // <4>
          return OffsetSpecification.offset(offset + 1); // <5>
        })
        .messageHandler((context, message) -> {
          // message handling code...

          storeOffsetInExternalStore(context.offset());
        })
        .build();
    // end::sac-consumer-update-listener[]
  }
}

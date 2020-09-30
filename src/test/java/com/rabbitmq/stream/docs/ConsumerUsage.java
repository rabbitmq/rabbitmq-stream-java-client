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
import java.time.Duration;
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

    void defaultAutoCommit() {
      Environment environment = Environment.builder().build();
      // tag::auto-commit-defaults[]
      Consumer consumer =
          environment.consumerBuilder()
              .stream("my-stream")
              .name("application-1")   // <1>
              .autoCommitStrategy()   // <2>
              .builder()
              .messageHandler((context, message) -> {
                // message handling code...
              })
              .build();
      // end::auto-commit-defaults[]
    }

  void autoCommitWithSettings() {
    Environment environment = Environment.builder().build();
    // tag::auto-commit-with-settings[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .autoCommitStrategy()   // <2>
                .messageCountBeforeCommit(50_000)   // <3>
                .flushInterval(Duration.ofSeconds(10))   // <4>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...
            })
            .build();
    // end::auto-commit-with-settings[]
  }

  void autoCommitOnlyWithName() {
    Environment environment = Environment.builder().build();
    // tag::auto-commit-only-with-name[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .messageHandler((context, message) -> {
              // message handling code...
            })
            .build();
    // end::auto-commit-only-with-name[]
  }

  void manualCommitDefaults() {
    Environment environment = Environment.builder().build();
    // tag::manual-commit-defaults[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .manualCommitStrategy()   // <2>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...

              if (conditionToCommit()) {
                context.commit();   // <3>
              }
            })
            .build();
    // end::manual-commit-defaults[]
  }

  boolean conditionToCommit() {
      return true;
  }

  void manualCommitWithSettings() {
    Environment environment = Environment.builder().build();
    // tag::manual-commit-with-settings[]
    Consumer consumer =
        environment.consumerBuilder()
            .stream("my-stream")
            .name("application-1")   // <1>
            .manualCommitStrategy()   // <2>
                .checkInterval(Duration.ofSeconds(10))   // <3>
            .builder()
            .messageHandler((context, message) -> {
              // message handling code...

              if (conditionToCommit()) {
                context.commit();   // <4>
              }
            })
            .build();
    // end::manual-commit-with-settings[]
  }

}

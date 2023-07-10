// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
import com.rabbitmq.stream.Producer;

public class FilteringUsage {

  void producerSimple() {
    Environment environment = Environment.builder().build();
    // tag::producer-simple[]
    Producer producer = environment.producerBuilder()
      .stream("invoices")
      .filterValue(msg ->
        msg.getApplicationProperties().get("state").toString())  // <1>
      .build();
    // end::producer-simple[]
  }

  void consumerSimple() {
    Environment environment = Environment.builder().build();
    // tag::consumer-simple[]
    String filterValue = "california";
    Consumer consumer = environment.consumerBuilder()
      .stream("invoices")
      .filter()
        .values(filterValue)  // <1>
        .postFilter(msg ->
          filterValue.equals(msg.getApplicationProperties().get("state")))  // <2>
      .builder()
      .messageHandler((ctx, msg) -> { })
      .build();
    // end::consumer-simple[]
  }

  void consumerMatchUnfiltered() {
    Environment environment = Environment.builder().build();
    // tag::consumer-match-unfiltered[]
    String filterValue = "california";
    Consumer consumer = environment.consumerBuilder()
      .stream("invoices")
      .filter()
        .values(filterValue)  // <1>
        .matchUnfiltered()  // <2>
        .postFilter(msg ->
            filterValue.equals(msg.getApplicationProperties().get("state"))
            || !msg.getApplicationProperties().containsKey("state")  // <3>
        )
      .builder()
      .messageHandler((ctx, msg) -> { })
      .build();
    // end::consumer-match-unfiltered[]
  }

}

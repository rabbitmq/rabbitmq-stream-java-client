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

package com.rabbitmq.stream;

import java.time.Duration;
import java.util.function.Function;

public interface ProducerBuilder {

  ProducerBuilder name(String name);

  ProducerBuilder stream(String stream);

  ProducerBuilder subEntrySize(int subEntrySize);

  ProducerBuilder batchSize(int batchSize);

  ProducerBuilder batchPublishingDelay(Duration batchPublishingDelay);

  ProducerBuilder maxUnconfirmedMessages(int maxUnconfirmedMessages);

  ProducerBuilder confirmTimeout(Duration timeout);

  ProducerBuilder enqueueTimeout(Duration timeout);

  ProducerBuilder routing(Function<Message, String> routingKeyExtractor, RoutingType routingType);

  Producer build();

  enum RoutingType {
    HASH,
    KEY
  }
}

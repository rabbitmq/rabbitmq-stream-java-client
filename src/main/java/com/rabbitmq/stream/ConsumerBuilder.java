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

public interface ConsumerBuilder {

  ConsumerBuilder stream(String stream);

  ConsumerBuilder offset(OffsetSpecification offsetSpecification);

  ConsumerBuilder messageHandler(MessageHandler messageHandler);

  ConsumerBuilder name(String name);

  ManualCommitStrategy manualCommitStrategy();

  AutoCommitStrategy autoCommitStrategy();

  Consumer build();

  interface ManualCommitStrategy {

    ManualCommitStrategy checkInterval(Duration checkInterval);

    ConsumerBuilder builder();
  }

  interface AutoCommitStrategy {

    AutoCommitStrategy messageCountBeforeCommit(int messageCountBeforeCommit);

    AutoCommitStrategy flushInterval(Duration flushInterval);

    ConsumerBuilder builder();
  }
}

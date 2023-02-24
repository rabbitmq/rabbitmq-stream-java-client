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
package com.rabbitmq.stream;

import java.util.List;
import java.util.function.Function;

/**
 * Strategy to route outbound messages to appropriate streams.
 *
 * <p>This is an experimental API, subject to change.
 *
 * <p>Used for super streams (partitioned stream).
 *
 * @see ProducerBuilder#routing(Function)
 */
public interface RoutingStrategy {

  /**
   * Where to route a message.
   *
   * @param message
   * @param metadata
   * @return the list of streams to route messages to
   */
  List<String> route(Message message, Metadata metadata);

  /** Metadata on the super stream. */
  interface Metadata {

    List<String> partitions();

    List<String> route(String routingKey);
  }
}

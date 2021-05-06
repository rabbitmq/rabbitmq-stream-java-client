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

/**
 * Callback API for inbound messages.
 *
 * @see ConsumerBuilder#messageHandler(MessageHandler)
 * @see Consumer
 */
public interface MessageHandler {

  /**
   * Callback for an inbound message.
   *
   * @param context context on the message
   * @param message the message
   */
  void handle(Context context, Message message);

  /** Information about the message. */
  interface Context {

    /**
     * The offset of the message in the stream.
     *
     * @return
     */
    long offset();

    /**
     * Shortcut to send a commit for the message offset.
     *
     * @see Consumer#commit(long)
     */
    void commit();

    /**
     * The consumer that receives the message.
     *
     * @return
     * @see Consumer#commit(long)
     */
    Consumer consumer();
  }
}

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
     * @return the offset of the current message
     */
    long offset();

    /**
     * Shortcut to send a store order for the message offset.
     *
     * @see Consumer#store(long)
     */
    void storeOffset();

    /**
     * The timestamp of the message chunk.
     *
     * @return the timestamp of the message chunk
     */
    long timestamp();

    /**
     * The ID (offset) of the committed chunk (block of messages) in the stream.
     *
     * <p>It is the offset of the first message in the last chunk confirmed by a quorum of the
     * stream cluster members (leader and replicas).
     *
     * <p>The committed chunk ID is a good indication of what the last offset of a stream can be at
     * a given time. The value can be stale as soon as the application reads it though, as the
     * committed chunk ID for a stream that is published to changes all the time.
     *
     * <p>This requires RabbitMQ 3.11 or more. The method always returns 0 otherwise.
     *
     * @return committed chunk ID in this stream
     * @see StreamStats#committedChunkId()
     */
    long committedChunkId();

    /**
     * The stream the message comes from.
     *
     * @return the stream the message comes from
     */
    String stream();

    /**
     * The consumer that receives the message.
     *
     * @return the consumer instance
     * @see Consumer#store(long)
     */
    Consumer consumer();
  }
}

// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
 * Marker interface for {@link com.rabbitmq.stream.Resource}-like classes.
 *
 * <p>Instances of these classes have different states during their lifecycle: open, recovering,
 * closed, etc. Application can be interested in taking some actions for a given state (e.g.
 * stopping publishing when a {@link com.rabbitmq.stream.Producer} is recovering after a connection
 * problem and resuming publishing when it is open again).
 *
 * @see com.rabbitmq.stream.Producer
 * @see com.rabbitmq.stream.Consumer
 */
public interface Resource {

  /**
   * Application listener for a {@link com.rabbitmq.stream.Resource}.
   *
   * <p>They are registered at creation time.
   *
   * @see
   *     com.rabbitmq.stream.ProducerBuilder#listeners(com.rabbitmq.stream.Resource.StateListener...)
   * @see
   *     com.rabbitmq.stream.ConsumerBuilder#listeners(com.rabbitmq.stream.Resource.StateListener...)
   */
  @FunctionalInterface
  interface StateListener {

    /**
     * Handle state change.
     *
     * @param context state change context
     */
    void handle(Context context);
  }

  /** Context of a resource state change. */
  interface Context {

    /**
     * The resource instance.
     *
     * @return resource instance
     */
    Resource resource();

    /**
     * The previous state of the resource.
     *
     * @return previous state
     */
    State previousState();

    /**
     * The current (new) state of the resource.
     *
     * @return current state
     */
    State currentState();
  }

  /** Resource state. */
  enum State {
    /** The resource is currently opening. */
    OPENING,
    /** The resource is open and functional. */
    OPEN,
    /** The resource is recovering. */
    RECOVERING,
    /** The resource is closing. */
    CLOSING,
    /** The resource is closed. */
    CLOSED
  }
}

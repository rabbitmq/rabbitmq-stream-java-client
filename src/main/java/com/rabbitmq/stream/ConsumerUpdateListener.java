// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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
 * An interface for reacting to status changes of single active consumers.
 *
 * <p>An application uses the {@link #update(Context)} callback to compute the offset to start
 * consuming from when the consumer becomes active.
 *
 * <p>The application can also use the {@link #update(Context)} callback to store the offset of the
 * last message it processed when the consumer goes from active to passive.
 *
 * <p>This is especially useful when using manual server-side offset tracking or offset tracking
 * from an external datastore.
 *
 * @see ConsumerBuilder#singleActiveConsumer()
 * @see ConsumerBuilder#manualTrackingStrategy()
 * @see ConsumerBuilder#noTrackingStrategy()
 */
public interface ConsumerUpdateListener {

  /**
   * Callback when the consumer status change.
   *
   * <p>The method <em>must</em> return the offset specification to resume consumption from when the
   * consumer becomes active. The broker will use the value to initialize the dispatching of
   * messages from the appropriate location.
   *
   * <p>The broker ignores the returned value when the consumer becomes inactive.
   *
   * @param context information on the status change
   * @return the offset specification to consume from if the status is active
   */
  OffsetSpecification update(Context context);

  /** Information on the status change. */
  interface Context {

    /**
     * The consumer instance.
     *
     * @return the consumer instance
     */
    Consumer consumer();

    /**
     * The stream (partition in a super stream) involved.
     *
     * @return the stream
     */
    String stream();

    /**
     * Whether the consumer is active or not.
     *
     * @return active flag
     */
    boolean isActive();
  }
}

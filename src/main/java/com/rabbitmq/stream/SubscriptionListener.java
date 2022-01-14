// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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
 * Callback interface to customize a subscription.
 *
 * <p>It is possible to change the computed {@link OffsetSpecification} in {@link
 * #preSubscribe(SubscriptionContext)} by using a custom offset tracking strategy.
 *
 * <p>This is an experimental API, subject to change.
 */
public interface SubscriptionListener {

  /**
   * Callback called before the subscription is created.
   *
   * <p>The method is called when a {@link Consumer} is created and it registers to broker, and also
   * when the subscription must be re-created (after a disconnection or when the subscription must
   * moved because the stream member it was connection becomes unavailable).
   *
   * <p>Application code can set the {@link OffsetSpecification} that will be used with the {@link
   * SubscriptionContext#offsetSpecification(OffsetSpecification)} method.
   *
   * @param subscriptionContext
   */
  void preSubscribe(SubscriptionContext subscriptionContext);

  /** Context object for the subscription. */
  interface SubscriptionContext {

    /**
     * The offset specification computed by the library.
     *
     * <p>If the consumer has no name, the value is the value set with {@link
     * ConsumerBuilder#offset(OffsetSpecification)} on the first subscription and the offset of the
     * last dispatched message on subsequent calls (e.g. when the client re-subscribes after a
     * disconnection).
     *
     * <p>If the consumer has a name, the value is the last stored if any.
     *
     * @see ConsumerBuilder#name(String)
     * @return the computed offset specification
     */
    OffsetSpecification offsetSpecification();

    /**
     * Set the offset specification to use for the subscription.
     *
     * <p>It overrides the value computed by the client.
     *
     * @param offsetSpecification the offset specification to use
     */
    void offsetSpecification(OffsetSpecification offsetSpecification);
  }
}

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
package com.rabbitmq.stream;

/**
 * API to instrument operations in the stream client. The supported operations are publishing, and
 * asynchronous delivery.
 *
 * <p>Implementations can gather information and send it to tracing backends. This allows e.g.
 * following the processing steps of a given message through different systems.
 *
 * <p>This is considered an SPI and is susceptible to change at any time.
 *
 * @since 0.12.0
 * @see EnvironmentBuilder#observationCollector(ObservationCollector)
 * @see com.rabbitmq.stream.observation.micrometer.MicrometerObservationCollectorBuilder
 */
public interface ObservationCollector<T> {

  ObservationCollector<Void> NO_OP =
      new ObservationCollector<Void>() {
        @Override
        public Void prePublish(String stream, Message message) {
          return null;
        }

        @Override
        public void published(Void context, Message message) {}

        @Override
        public MessageHandler subscribe(MessageHandler handler) {
          return handler;
        }
      };

  /**
   * Start observation.
   *
   * <p>Implementations are expecting to return an observation context that will be passed in to the
   * {@link #published(Object, Message)} callback.
   *
   * @param stream the stream to publish to
   * @param message the message to publish
   * @return observation context
   */
  T prePublish(String stream, Message message);

  /**
   * Callback when the message is about to be published.
   *
   * @param context the observation context
   * @param message the message to publish
   */
  void published(T context, Message message);

  /**
   * Decorate consumer registration.
   *
   * @param handler the original handler
   * @return a decorated handler
   */
  MessageHandler subscribe(MessageHandler handler);

  /**
   * Says whether the implementation does nothing or not.
   *
   * @return true if the implementation is a no-op
   */
  default boolean isNoop() {
    return this == NO_OP;
  }
}

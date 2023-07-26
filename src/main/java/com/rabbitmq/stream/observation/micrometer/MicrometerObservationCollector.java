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
package com.rabbitmq.stream.observation.micrometer;

import com.rabbitmq.stream.*;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micrometer's {@link ObservationCollector}.
 *
 * @since 0.12.0
 */
class MicrometerObservationCollector implements ObservationCollector<Observation> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MicrometerObservationCollector.class);

  private final ObservationRegistry registry;
  private final PublishObservationConvention customPublishConvention, defaultPublishConvention;
  private final ProcessObservationConvention customProcessConvention, defaultProcessConvention;

  MicrometerObservationCollector(
      ObservationRegistry registry,
      PublishObservationConvention customPublishConvention,
      PublishObservationConvention defaultPublishConvention,
      ProcessObservationConvention customProcessConvention,
      ProcessObservationConvention defaultProcessConvention) {
    this.registry = registry;
    this.customPublishConvention = customPublishConvention;
    this.defaultPublishConvention = defaultPublishConvention;
    this.customProcessConvention = customProcessConvention;
    this.defaultProcessConvention = defaultProcessConvention;
  }

  @Override
  public void published(Observation observation, Message message) {
    try {
      observation.stop();
    } catch (Exception e) {
      LOGGER.warn("Error while stopping Micrometer observation: {}", e.getMessage());
    }
  }

  @Override
  public Observation prePublish(String stream, Message message) {
    PublishContext context = new PublishContext(stream, message);
    Observation observation =
        StreamObservationDocumentation.PUBLISH_OBSERVATION.observation(
            customPublishConvention, defaultPublishConvention, () -> context, registry);
    observation.start();
    return observation;
  }

  @Override
  public MessageHandler subscribe(MessageHandler handler) {
    return new ObservationMessageHandler(
        handler, registry, customProcessConvention, defaultProcessConvention);
  }

  private static class ObservationMessageHandler implements MessageHandler {

    private final MessageHandler delegate;
    private final ObservationRegistry registry;
    private final ProcessObservationConvention customProcessConvention, defaultProcessConvention;

    private ObservationMessageHandler(
        MessageHandler delegate,
        ObservationRegistry registry,
        ProcessObservationConvention customProcessConvention,
        ProcessObservationConvention defaultProcessConvention) {
      this.delegate = delegate;
      this.registry = registry;
      this.customProcessConvention = customProcessConvention;
      this.defaultProcessConvention = defaultProcessConvention;
    }

    @Override
    public void handle(Context context, Message message) {
      ProcessContext processContext = new ProcessContext(context.stream(), message);
      Observation observation =
          StreamObservationDocumentation.PROCESS_OBSERVATION.observation(
              this.customProcessConvention,
              this.defaultProcessConvention,
              () -> processContext,
              this.registry);
      observation.observeChecked(() -> delegate.handle(context, message));
    }
  }
}

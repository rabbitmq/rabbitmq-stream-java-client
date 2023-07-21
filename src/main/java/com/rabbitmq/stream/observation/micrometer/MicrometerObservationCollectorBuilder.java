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

import com.rabbitmq.stream.ObservationCollector;
import io.micrometer.observation.ObservationRegistry;

public class MicrometerObservationCollectorBuilder {

  private ObservationRegistry registry = ObservationRegistry.NOOP;
  private PublishObservationConvention customPublishConvention;
  private PublishObservationConvention defaultPublishConvention =
      new DefaultPublishObservationConvention();
  private ProcessObservationConvention customProcessConvention;
  private ProcessObservationConvention defaultProcessConvention =
      new DefaultProcessObservationConvention();

  public MicrometerObservationCollectorBuilder registry(ObservationRegistry registry) {
    this.registry = registry;
    return this;
  }

  public MicrometerObservationCollectorBuilder customPublishConvention(
      PublishObservationConvention customPublishConvention) {
    this.customPublishConvention = customPublishConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder defaultPublishConvention(
      PublishObservationConvention defaultPublishConvention) {
    this.defaultPublishConvention = defaultPublishConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder customProcessConvention(
      ProcessObservationConvention customProcessConvention) {
    this.customProcessConvention = customProcessConvention;
    return this;
  }

  public MicrometerObservationCollectorBuilder defaultProcessConvention(
      ProcessObservationConvention defaultProcessConvention) {
    this.defaultProcessConvention = defaultProcessConvention;
    return this;
  }

  public ObservationCollector build() {
    return new MicrometerObservationCollector(
        this.registry,
        this.customPublishConvention,
        this.defaultPublishConvention,
        this.customProcessConvention,
        this.defaultProcessConvention);
  }
}

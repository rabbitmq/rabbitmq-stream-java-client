// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.observation.micrometer;

import com.rabbitmq.stream.ObservationCollector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import java.util.function.Supplier;

/**
 * Builder to configure and create <a href="https://micrometer.io/docs/observation">Micrometer
 * Observation</a> implementation of {@link ObservationCollector}.
 *
 * @since 0.12.0
 */
public class MicrometerObservationCollectorBuilder {

  private ObservationRegistry registry = ObservationRegistry.NOOP;
  private PublishObservationConvention customPublishObservationConvention;
  private PublishObservationConvention defaultPublishObservationConvention =
      new DefaultPublishObservationConvention();
  private ProcessObservationConvention customProcessObservationConvention;
  private ProcessObservationConvention defaultProcessObservationConvention =
      new DefaultProcessObservationConvention();

  /**
   * Set the {@link ObservationRegistry} to use.
   *
   * <p>Default is {@link ObservationRegistry#NOOP}.
   *
   * @param registry the registry
   * @return this builder instance
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public MicrometerObservationCollectorBuilder registry(ObservationRegistry registry) {
    this.registry = registry;
    return this;
  }

  /**
   * Custom convention for publishing.
   *
   * <p>If not null, it will override any pre-configured conventions.
   *
   * <p>Default is <code>null</code>.
   *
   * @param customPublishObservationConvention the convention
   * @return this builder instance
   * @see io.micrometer.observation.docs.ObservationDocumentation#observation(ObservationConvention,
   *     ObservationConvention, Supplier, ObservationRegistry)
   */
  public MicrometerObservationCollectorBuilder customPublishObservationConvention(
      PublishObservationConvention customPublishObservationConvention) {
    this.customPublishObservationConvention = customPublishObservationConvention;
    return this;
  }

  /**
   * Default convention for publishing.
   *
   * <p>It will be picked if there was neither custom convention nor a pre-configured one via {@link
   * ObservationRegistry}.
   *
   * <p>Default is {@link DefaultPublishObservationConvention}.
   *
   * @param defaultPublishObservationConvention the convention
   * @return this builder instance
   * @see io.micrometer.observation.docs.ObservationDocumentation#observation(ObservationConvention,
   *     ObservationConvention, Supplier, ObservationRegistry)
   */
  public MicrometerObservationCollectorBuilder defaultPublishObservationConvention(
      PublishObservationConvention defaultPublishObservationConvention) {
    this.defaultPublishObservationConvention = defaultPublishObservationConvention;
    return this;
  }

  /**
   * Custom convention for consuming.
   *
   * <p>If not null, it will override any pre-configured conventions.
   *
   * <p>Default is <code>null</code>.
   *
   * @param customProcessObservationConvention the convention
   * @return this builder instance
   * @see io.micrometer.observation.docs.ObservationDocumentation#observation(ObservationConvention,
   *     ObservationConvention, Supplier, ObservationRegistry)
   */
  public MicrometerObservationCollectorBuilder customProcessObservationConvention(
      ProcessObservationConvention customProcessObservationConvention) {
    this.customProcessObservationConvention = customProcessObservationConvention;
    return this;
  }

  /**
   * Default convention for consuming.
   *
   * <p>It will be picked if there was neither custom convention nor a pre-configured one via {@link
   * ObservationRegistry}.
   *
   * <p>Default is {@link DefaultProcessObservationConvention}.
   *
   * @param defaultProcessObservationConvention the convention
   * @return this builder instance
   * @see io.micrometer.observation.docs.ObservationDocumentation#observation(ObservationConvention,
   *     ObservationConvention, Supplier, ObservationRegistry)
   * @since 0.12.0
   */
  public MicrometerObservationCollectorBuilder defaultProcessObservationConvention(
      ProcessObservationConvention defaultProcessObservationConvention) {
    this.defaultProcessObservationConvention = defaultProcessObservationConvention;
    return this;
  }

  /**
   * Create the Micrometer {@link ObservationCollector}.
   *
   * @return the Micrometer observation collector
   */
  public ObservationCollector<Observation> build() {
    return new MicrometerObservationCollector(
        this.registry,
        this.customPublishObservationConvention,
        this.defaultPublishObservationConvention,
        this.customProcessObservationConvention,
        this.defaultProcessObservationConvention);
  }
}

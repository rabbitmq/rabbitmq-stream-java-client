// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

public class MetricsCollectorsTest {

  @Test
  void micrometer() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsCollector collector = new MicrometerMetricsCollector(registry);

    assertThat(registry.get("rabbitmq.stream.connections").gauge().value()).isZero();
    collector.openConnection();
    assertThat(registry.get("rabbitmq.stream.connections").gauge().value()).isEqualTo(1);
    collector.openConnection();
    assertThat(registry.get("rabbitmq.stream.connections").gauge().value()).isEqualTo(2);
    collector.closeConnection();
    assertThat(registry.get("rabbitmq.stream.connections").gauge().value()).isEqualTo(1);

    collector.publish(10);
    assertThat(registry.get("rabbitmq.stream.published").counter().count()).isEqualTo(10.0);
    assertThat(registry.get("rabbitmq.stream.outstanding_publish_confirm").gauge().value())
        .isEqualTo(10.0);
    collector.publishConfirm(5);
    assertThat(registry.get("rabbitmq.stream.confirmed").counter().count()).isEqualTo(5.0);
    assertThat(registry.get("rabbitmq.stream.outstanding_publish_confirm").gauge().value())
        .isEqualTo(5.0);
    collector.publishError(5);
    assertThat(registry.get("rabbitmq.stream.errored").counter().count()).isEqualTo(5.0);
    assertThat(registry.get("rabbitmq.stream.outstanding_publish_confirm").gauge().value())
        .isEqualTo(0);

    collector.chunk(100);
    assertThat(registry.get("rabbitmq.stream.chunk").counter().count()).isEqualTo(1);
    assertThat(registry.get("rabbitmq.stream.chunk_size").summary().totalAmount()).isEqualTo(100.0);
  }

  @Test
  void dropwizard() {
    MetricRegistry registry = new MetricRegistry();
    MetricsCollector collector = new DropwizardMetricsCollector(registry);

    assertThat(registry.counter("rabbitmq.stream.connections").getCount()).isZero();
    collector.openConnection();
    assertThat(registry.counter("rabbitmq.stream.connections").getCount()).isEqualTo(1);
    collector.openConnection();
    assertThat(registry.counter("rabbitmq.stream.connections").getCount()).isEqualTo(2);
    collector.closeConnection();
    assertThat(registry.counter("rabbitmq.stream.connections").getCount()).isEqualTo(1);

    collector.publish(10);
    assertThat(registry.meter("rabbitmq.stream.published").getCount()).isEqualTo(10);
    assertThat(registry.counter("rabbitmq.stream.outstanding_publish_confirm").getCount())
        .isEqualTo(10);
    collector.publishConfirm(5);
    assertThat(registry.meter("rabbitmq.stream.confirmed").getCount()).isEqualTo(5);
    assertThat(registry.counter("rabbitmq.stream.outstanding_publish_confirm").getCount())
        .isEqualTo(5);
    collector.publishError(5);
    assertThat(registry.meter("rabbitmq.stream.errored").getCount()).isEqualTo(5);
    assertThat(registry.counter("rabbitmq.stream.outstanding_publish_confirm").getCount())
        .isEqualTo(0);

    collector.chunk(100);
    assertThat(registry.meter("rabbitmq.stream.chunk").getCount()).isEqualTo(1);
    assertThat(registry.histogram("rabbitmq.stream.chunk_size").getSnapshot().getMax())
        .isEqualTo(100);
  }
}

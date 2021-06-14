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

package com.rabbitmq.stream.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class DropwizardMetricsCollector implements MetricsCollector {

  private final Counter connections;

  private final Meter publish;
  private final Meter publishConfirm;
  private final Meter publishError;
  private final Meter chunk;
  private final Meter consume;
  private final Meter writtenBytes;
  private final Meter readBytes;

  private final Counter outstandingPublishConfirm;
  private final Histogram chunkSize;

  public DropwizardMetricsCollector(MetricRegistry registry, String metricsPrefix) {
    this.connections = registry.counter(metricsPrefix + ".connections");
    this.publish = registry.meter(metricsPrefix + ".published");
    this.publishConfirm = registry.meter(metricsPrefix + ".confirmed");
    this.publishError = registry.meter(metricsPrefix + ".errored");
    this.chunk = registry.meter(metricsPrefix + ".chunk");
    this.chunkSize = registry.histogram(metricsPrefix + ".chunk_size");
    this.consume = registry.meter(metricsPrefix + ".consumed");
    this.writtenBytes = registry.meter(metricsPrefix + ".written_bytes");
    this.readBytes = registry.meter(metricsPrefix + ".read_bytes");
    this.outstandingPublishConfirm =
        registry.counter(metricsPrefix + ".outstanding_publish_confirm");
  }

  public DropwizardMetricsCollector() {
    this(new MetricRegistry());
  }

  public DropwizardMetricsCollector(MetricRegistry metricRegistry) {
    this(metricRegistry, "rabbitmq.stream");
  }

  @Override
  public void openConnection() {
    this.connections.inc();
  }

  @Override
  public void closeConnection() {
    this.connections.dec();
  }

  @Override
  public void publish(int count) {
    publish.mark(count);
    outstandingPublishConfirm.inc(count);
  }

  @Override
  public void publishConfirm(int count) {
    publishConfirm.mark(count);
    outstandingPublishConfirm.dec(count);
  }

  @Override
  public void publishError(int count) {
    publishError.mark(count);
    outstandingPublishConfirm.dec(count);
  }

  @Override
  public void chunk(int entriesCount) {
    chunk.mark();
    chunkSize.update(entriesCount);
  }

  @Override
  public void consume(long count) {
    consume.mark(count);
  }

  @Override
  public void writtenBytes(int writtenBytes) {
    this.writtenBytes.mark(writtenBytes);
  }

  @Override
  public void readBytes(int readBytes) {
    this.readBytes.mark(readBytes);
  }
}

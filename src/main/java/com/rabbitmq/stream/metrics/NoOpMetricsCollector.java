// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

public final class NoOpMetricsCollector implements MetricsCollector {

  public static final MetricsCollector SINGLETON = new NoOpMetricsCollector();

  private NoOpMetricsCollector() {}

  @Override
  public void openConnection() {}

  @Override
  public void closeConnection() {}

  @Override
  public void publish(int count) {}

  @Override
  public void publishConfirm(int count) {}

  @Override
  public void publishError(int count) {}

  @Override
  public void chunk(int entriesCount) {}

  @Override
  public void consume(long count) {}

  @Override
  public void writtenBytes(int writtenBytes) {}

  @Override
  public void readBytes(int readBytes) {}
}

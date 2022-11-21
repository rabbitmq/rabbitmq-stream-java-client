// Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.perf;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import picocli.CommandLine.Option;

class PrometheusEndpointMonitoring implements Monitoring {

  @Option(
      names = {"--prometheus"},
      description = "Enable HTTP Prometheus metrics endpoint",
      defaultValue = "false")
  private boolean enabled;

  private volatile PrometheusMeterRegistry registry;

  @Override
  public void configure(MonitoringContext context) {
    if (enabled) {
      registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      context.meterRegistry().add(registry);
      context.addHttpEndpoint(
          "metrics",
          "Prometheus metrics",
          exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            byte[] content = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(content);
            }
          });
    }
  }
}

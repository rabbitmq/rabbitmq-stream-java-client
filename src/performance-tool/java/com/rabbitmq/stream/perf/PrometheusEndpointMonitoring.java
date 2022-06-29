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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
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
          new AbstractHandler() {
            @Override
            public void handle(
                String target,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {
              String scraped = registry.scrape();

              response.setStatus(HttpServletResponse.SC_OK);
              response.setContentLength(scraped.length());
              response.setContentType("text/plain");

              response.getWriter().print(scraped);

              baseRequest.setHandled(true);
            }
          });
    }
  }
}

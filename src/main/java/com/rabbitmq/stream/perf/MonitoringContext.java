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

import com.rabbitmq.stream.Environment;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MonitoringContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringContext.class);

  private final int monitoringPort;
  private final CompositeMeterRegistry meterRegistry;
  private final Environment environment;

  private final Collection<Endpoint> endpoints = Collections.synchronizedList(new ArrayList<>());

  private volatile HttpServer server;

  MonitoringContext(
      int monitoringPort, CompositeMeterRegistry meterRegistry, Environment environment) {
    this.monitoringPort = monitoringPort;
    this.meterRegistry = meterRegistry;
    this.environment = environment;
  }

  void addHttpEndpoint(String path, String description, HttpHandler handler) {
    this.endpoints.add(new Endpoint(path, description, handler));
  }

  void start() throws Exception {
    if (!endpoints.isEmpty()) {
      server = HttpServer.create(new InetSocketAddress(this.monitoringPort), 0);

      for (Endpoint handler : endpoints) {
        server.createContext(handler.path, handler.handler);
      }

      String hostname = hostname();

      StringBuilder builder =
          new StringBuilder(
              "{ \"name\" : \"StreamPerfTest Monitoring Endpoints\", \"endpoints\" : [");
      String endpointJson =
          endpoints.stream()
              .map(
                  endpoint ->
                      "{\"name\" : \""
                          + endpoint.description
                          + "\", \"href\" : \""
                          + url(hostname, endpoint.path)
                          + "\"}")
              .collect(Collectors.joining(","));
      builder.append(endpointJson);
      builder.append("]}");
      server.createContext(
          "/",
          exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            byte[] content = builder.toString().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(content);
            }
          });

      server.start();
      System.out.println("Monitoring endpoints started on http://localhost:" + this.monitoringPort);
    }
  }

  private String url(String hostname, String endpoint) {
    return String.format("http://%s:%d%s", hostname, this.monitoringPort, endpoint);
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "localhost";
    }
  }

  void close() {
    if (server != null) {
      LOGGER.debug("Closing HTTP server");
      long start = System.currentTimeMillis();
      server.stop(0);
      LOGGER.debug("Closed HTTP server in {} ms", (System.currentTimeMillis() - start));
    }
  }

  CompositeMeterRegistry meterRegistry() {
    return this.meterRegistry;
  }

  Environment environment() {
    return this.environment;
  }

  private static class Endpoint {

    private final String path, description;
    private final HttpHandler handler;

    private Endpoint(String path, String description, HttpHandler handler) {
      this.path = path.startsWith("/") ? path : "/" + path;
      this.description = description;
      this.handler = handler;
    }
  }
}

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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MonitoringContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringContext.class);

  private final int monitoringPort;
  private final CompositeMeterRegistry meterRegistry;

  private final Map<String, HttpHandler> handlers = new LinkedHashMap<>();

  private volatile HttpServer server;

  MonitoringContext(int monitoringPort, CompositeMeterRegistry meterRegistry) {
    this.monitoringPort = monitoringPort;
    this.meterRegistry = meterRegistry;
  }

  void addHttpEndpoint(String path, HttpHandler handler) {
    this.handlers.put(path, handler);
  }

  void start() throws Exception {
    if (!handlers.isEmpty()) {
      server = HttpServer.create(new InetSocketAddress(this.monitoringPort), 0);

      for (Entry<String, HttpHandler> entry : handlers.entrySet()) {
        String path = entry.getKey().startsWith("/") ? entry.getKey() : "/" + entry.getKey();
        HttpHandler handler = entry.getValue();
        server.createContext(path, handler);
      }

      server.start();
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
    return meterRegistry;
  }
}

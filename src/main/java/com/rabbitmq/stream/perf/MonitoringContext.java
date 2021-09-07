// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

class MonitoringContext {

  private final int monitoringPort;

  private final Map<String, Handler> handlers = new LinkedHashMap<>();

  private volatile Server server;

  MonitoringContext(int monitoringPort) {
    this.monitoringPort = monitoringPort;
  }

  void addHttpEndpoint(String path, Handler handler) {
    this.handlers.put(path, handler);
  }

  void start() throws Exception {
    if (!handlers.isEmpty()) {
      QueuedThreadPool threadPool = new QueuedThreadPool();
      // difference between those 2 should be high enough to avoid a warning
      threadPool.setMinThreads(2);
      threadPool.setMaxThreads(12);
      server = new Server(threadPool);
      ServerConnector connector = new ServerConnector(server);
      connector.setPort(this.monitoringPort);
      server.setConnectors(new Connector[] {connector});

      List<ContextHandler> contextHandlers = new ArrayList<>(handlers.size());
      for (Entry<String, Handler> entry : handlers.entrySet()) {
        String path = entry.getKey().startsWith("/") ? entry.getKey() : "/" + entry.getKey();
        Handler handler = entry.getValue();
        ContextHandler contextHandler = new ContextHandler();
        contextHandler.setContextPath(path);
        contextHandler.setHandler(handler);
        contextHandlers.add(contextHandler);
      }

      ContextHandlerCollection contextHandler =
          new ContextHandlerCollection(contextHandlers.toArray(new ContextHandler[0]));
      server.setHandler(contextHandler);

      server.setStopTimeout(1000);
      server.start();
    }
  }

  void close() throws Exception {
    if (server != null) {
      server.stop();
    }
  }
}

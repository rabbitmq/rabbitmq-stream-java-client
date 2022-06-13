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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;

class LogUtils {

  static void configureLog() throws IOException {
    String loggers =
        System.getProperty("rabbitmq.streamperftest.loggers") == null
            ? System.getenv("RABBITMQ_STREAM_PERF_TEST_LOGGERS")
            : System.getProperty("rabbitmq.streamperftest.loggers");
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    InputStream configurationFile = StreamPerfTest.class.getResourceAsStream("/logback.xml");
    try {
      String configuration =
          processConfigurationFile(configurationFile, convertKeyValuePairs(loggers));
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);
      context.reset();
      configurator.doConfigure(
          new ByteArrayInputStream(configuration.getBytes(StandardCharsets.UTF_8)));
    } catch (JoranException je) {
      // StatusPrinter will handle this
    } finally {
      configurationFile.close();
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
  }

  private static Map<String, Object> convertKeyValuePairs(String arg) {
    if (arg == null || arg.trim().isEmpty()) {
      return null;
    }
    Map<String, Object> properties = new HashMap<>();
    for (String entry : arg.split(",")) {
      String[] keyValue = entry.split("=");
      try {
        properties.put(keyValue[0], Long.parseLong(keyValue[1]));
      } catch (NumberFormatException e) {
        properties.put(keyValue[0], keyValue[1]);
      }
    }
    return properties;
  }

  static String processConfigurationFile(InputStream configurationFile, Map<String, Object> loggers)
      throws IOException {
    StringBuilder loggersConfiguration = new StringBuilder();
    if (loggers != null) {
      for (Map.Entry<String, Object> logger : loggers.entrySet()) {
        loggersConfiguration.append(
            String.format(
                "\t<logger name=\"%s\" level=\"%s\" />%s",
                logger.getKey(),
                logger.getValue().toString(),
                System.getProperty("line.separator")));
      }
    }

    BufferedReader in = new BufferedReader(new InputStreamReader(configurationFile));
    final int bufferSize = 1024;
    final char[] buffer = new char[bufferSize];
    StringBuilder builder = new StringBuilder();
    int charsRead;
    while ((charsRead = in.read(buffer, 0, buffer.length)) > 0) {
      builder.append(buffer, 0, charsRead);
    }

    return builder.toString().replace("${loggers}", loggersConfiguration);
  }
}

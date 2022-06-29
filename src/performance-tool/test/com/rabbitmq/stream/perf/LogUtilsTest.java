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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

public class LogUtilsTest {

  static final String XML =
      "<configuration>\n"
          + "    <appender name=\"STDOUT\" class=\"ch.qos.logback.core.ConsoleAppender\">\n"
          + "        <encoder>\n"
          + "            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>\n"
          + "        </encoder>\n"
          + "    </appender>\n"
          + "\n"
          + "${loggers}\n"
          + "\n"
          + "    <root level=\"warn\">\n"
          + "        <appender-ref ref=\"STDOUT\" />\n"
          + "    </root>\n"
          + "</configuration>";

  static InputStream xml() {
    return new ByteArrayInputStream(XML.getBytes(StandardCharsets.UTF_8));
  }

  private static Condition<String> validXml() {
    return new Condition<>(
        xml -> {
          try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            return true;
          } catch (Exception e) {
            return false;
          }
        },
        "Not a valid XML document");
  }

  @Test
  void processConfigurationFileNoLoggers() throws Exception {
    assertThat(LogUtils.processConfigurationFile(xml(), null))
        .isEqualTo(XML.replace("${loggers}", ""))
        .is(validXml());
    assertThat(LogUtils.processConfigurationFile(xml(), new HashMap<>()))
        .isEqualTo(XML.replace("${loggers}", ""))
        .is(validXml());
  }

  @Test
  void processConfigurationFileOneLogger() throws IOException {
    assertThat(
            LogUtils.processConfigurationFile(
                xml(), Collections.singletonMap("com.rabbitmq.stream", "info")))
        .contains("<logger name=\"com.rabbitmq.stream\" level=\"info\"")
        .is(validXml());
  }

  @Test
  void processConfigurationFileSeveralLoggers() throws IOException {
    Map<String, Object> loggers = new HashMap<>();
    loggers.put("com.rabbitmq.stream", "debug");
    loggers.put("com.rabbitmq.stream.perf", "info");
    assertThat(LogUtils.processConfigurationFile(xml(), loggers))
        .contains("<logger name=\"com.rabbitmq.stream\" level=\"debug\"")
        .contains("<logger name=\"com.rabbitmq.stream.perf\" level=\"info\"")
        .is(validXml());
  }
}

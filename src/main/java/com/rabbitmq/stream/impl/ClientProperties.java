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
package com.rabbitmq.stream.impl;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClientProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientProperties.class);

  // We store the version property in an unusual way because relocating the package can rewrite the
  // key in the property
  // file, which results in spurious warnings being emitted at start-up.
  // see https://github.com/rabbitmq/rabbitmq-java-client/issues/436
  private static final char[] VERSION_PROPERTY =
      new char[] {
        'c', 'o', 'm', '.', 'r', 'a', 'b', 'b', 'i', 't', 'm', 'q', '.', 's', 't', 'r', 'e', 'a',
        'm', '.', 'c', 'l', 'i', 'e', 'n', 't', '.', 'v', 'e', 'r', 's', 'i', 'o', 'n'
      };

  public static final String VERSION = getVersion();

  public static final Map<String, String> DEFAULT_CLIENT_PROPERTIES =
      Collections.unmodifiableMap(
          new HashMap<String, String>() {
            {
              put("product", "RabbitMQ Stream");
              put("version", ClientProperties.VERSION);
              put("platform", "Java");
              put("copyright", "Copyright (c) 2020-2022 VMware, Inc. or its affiliates.");
              put("information", "Licensed under the MPL 2.0. See https://www.rabbitmq.com/");
            }
          });

  private static String getVersion() {
    String version;
    try {
      version = getVersionFromPropertyFile();
    } catch (Exception e1) {
      LOGGER.warn("Couldn't get version from property file", e1);
      try {
        version = getVersionFromPackage();
      } catch (Exception e2) {
        LOGGER.warn("Couldn't get version with Package#getImplementationVersion", e1);
        version = getDefaultVersion();
      }
    }
    return version;
  }

  private static String getVersionFromPropertyFile() throws Exception {
    InputStream inputStream =
        ClientProperties.class
            .getClassLoader()
            .getResourceAsStream("rabbitmq-stream-client.properties");
    java.util.Properties version = new Properties();
    try {
      version.load(inputStream);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    String propertyName = new String(VERSION_PROPERTY);
    String versionProperty = version.getProperty(propertyName);
    if (versionProperty == null) {
      throw new IllegalStateException("Couldn't find version property in property file");
    }
    return versionProperty;
  }

  private static String getVersionFromPackage() {
    if (ClientProperties.class.getPackage().getImplementationVersion() == null) {
      throw new IllegalStateException("Couldn't get version with Package#getImplementationVersion");
    }
    return ClientProperties.class.getPackage().getImplementationVersion();
  }

  private static String getDefaultVersion() {
    return "0.0.0";
  }

  private ClientProperties() {}
}

// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConcurrencyUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyUtils.class);

  private static final ThreadFactory THREAD_FACTORY;

  static {
    ThreadFactory tf;
    if (isJava21OrMore()) {
      LOGGER.debug("Running Java 21 or more, using virtual threads");
      try {
        Class<?> builderClass =
            Arrays.stream(Thread.class.getDeclaredClasses())
                .filter(c -> "Builder".equals(c.getSimpleName()))
                .findFirst()
                .get();
        // Reflection code is the same as:
        // Thread.ofVirtual().factory();
        Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
        tf = (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
      } catch (IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException
          | RuntimeException e) {
        LOGGER.debug("Error when creating virtual thread factory on Java 21+: {}", e.getMessage());
        LOGGER.debug("Falling back to default thread factory");
        tf = Executors.defaultThreadFactory();
      }
    } else {
      tf = Executors.defaultThreadFactory();
    }
    THREAD_FACTORY = tf;
  }

  private ConcurrencyUtils() {}

  static ThreadFactory defaultThreadFactory() {
    return THREAD_FACTORY;
  }

  private static boolean isJava21OrMore() {
    String version = System.getProperty("java.version").replace("-beta", "");
    return Utils.versionCompare(version, "21.0") >= 0;
  }
}

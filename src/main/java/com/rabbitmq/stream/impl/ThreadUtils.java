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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ThreadUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);

  static final boolean VIRTUAL_THREADS_ON =
      Boolean.parseBoolean(System.getProperty("rabbitmq.stream.threads.virtual.enabled", "false"));

  private static final ThreadFactory THREAD_FACTORY;
  private static final Predicate<Thread> IS_VIRTUAL;

  static {
    ThreadFactory tf;
    LOGGER.debug("Virtual threads enabled: {}", VIRTUAL_THREADS_ON);
    LOGGER.debug("Java 21 or more: {}", isJava21OrMore());
    if (VIRTUAL_THREADS_ON && isJava21OrMore()) {
      LOGGER.debug("Using virtual threads");
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
      IS_VIRTUAL =
          thread -> {
            Method method = null;
            try {
              method = Thread.class.getDeclaredMethod("isVirtual");
              return (boolean) method.invoke(thread);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
              LOGGER.info("Error while checking if a thread is virtual: {}", e.getMessage());
              return false;
            }
          };
    } else {
      tf = Executors.defaultThreadFactory();
      IS_VIRTUAL = ignored -> false;
    }
    THREAD_FACTORY = tf;
  }

  private ThreadUtils() {}

  /**
   * Create a thread factory that prefixes thread names. Based on {@link
   * java.util.concurrent.Executors#defaultThreadFactory()}, so always creates platform threads.
   *
   * @param prefix used to prefix thread names
   * @return thread factory
   */
  static ThreadFactory threadFactory(String prefix) {
    if (prefix == null) {
      return Executors.defaultThreadFactory();
    } else {
      return new NamedThreadFactory(prefix);
    }
  }

  /**
   * Returns a thread factory that creates virtual threads if available.
   *
   * @param prefix
   * @return
   */
  static ThreadFactory internalThreadFactory(String prefix) {
    if (prefix == null) {
      return THREAD_FACTORY;
    } else {
      return new NamedThreadFactory(THREAD_FACTORY, prefix);
    }
  }

  /**
   * Creates a virtual thread if available.
   *
   * @param name
   * @param task
   * @return
   */
  static Thread newInternalThread(String name, Runnable task) {
    Thread t = THREAD_FACTORY.newThread(task);
    t.setName(name);
    return t;
  }

  static boolean isVirtual(Thread thread) {
    return IS_VIRTUAL.test(thread);
  }

  private static boolean isJava21OrMore() {
    return Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0;
  }

  private static class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory backingThreadFactory;

    private final String prefix;

    private final AtomicLong count = new AtomicLong(0);

    private NamedThreadFactory(String prefix) {
      this(Executors.defaultThreadFactory(), prefix);
    }

    private NamedThreadFactory(ThreadFactory backingThreadFactory, String prefix) {
      this.backingThreadFactory = backingThreadFactory;
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = this.backingThreadFactory.newThread(r);
      thread.setName(prefix + count.getAndIncrement());
      return thread;
    }
  }
}

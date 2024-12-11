// Copyright (c) 2024 Broadcom. All Rights Reserved.
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ThreadUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);

  private static final ThreadFactory THREAD_FACTORY;
  private static final Function<String, ExecutorService> EXECUTOR_SERVICE_FACTORY;
  private static final Predicate<Thread> IS_VIRTUAL;

  static {
    if (isJava21OrMore()) {
      LOGGER.debug("Running Java 21 or more, using virtual threads");
      Class<?> builderClass =
          Arrays.stream(Thread.class.getDeclaredClasses())
              .filter(c -> "Builder".equals(c.getSimpleName()))
              .findFirst()
              .get();
      // Reflection code is the same as:
      // Thread.ofVirtual().factory();
      try {
        Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
        THREAD_FACTORY = (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
      EXECUTOR_SERVICE_FACTORY =
          prefix -> {
            try {
              // Reflection code is the same as the 2 following lines:
              // ThreadFactory factory = Thread.ofVirtual().name(prefix, 0).factory();
              // Executors.newThreadPerTaskExecutor(factory);
              Object builder = Thread.class.getDeclaredMethod("ofVirtual").invoke(null);
              if (prefix != null) {
                builder =
                    builderClass
                        .getDeclaredMethod("name", String.class, Long.TYPE)
                        .invoke(builder, prefix, 0L);
              }
              ThreadFactory factory =
                  (ThreadFactory) builderClass.getDeclaredMethod("factory").invoke(builder);
              return (ExecutorService)
                  Executors.class
                      .getDeclaredMethod("newThreadPerTaskExecutor", ThreadFactory.class)
                      .invoke(null, factory);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          };
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
      THREAD_FACTORY = Executors.defaultThreadFactory();
      EXECUTOR_SERVICE_FACTORY = prefix -> Executors.newCachedThreadPool(threadFactory(prefix));
      IS_VIRTUAL = ignored -> false;
    }
  }

  private ThreadUtils() {}

  static ThreadFactory threadFactory(String prefix) {
    if (prefix == null) {
      return Executors.defaultThreadFactory();
    } else {
      return new NamedThreadFactory(prefix);
    }
  }

  static ThreadFactory internalThreadFactory(String prefix) {
    return new NamedThreadFactory(THREAD_FACTORY, prefix);
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

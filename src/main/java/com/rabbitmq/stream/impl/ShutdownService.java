// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to register callbacks and call them in reverse order. Registered callbacks are made
 * automatically idempotent.
 *
 * <p>This class can be used to register closing callbacks, call them individually, and/or call all
 * of them (in LIFO order) with the {@link #close()} method.
 *
 * <p>From
 * https://github.com/rabbitmq/rabbitmq-perf-test/blob/main/src/main/java/com/rabbitmq/perf/ShutdownService.java.
 */
final class ShutdownService implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownService.class);

  private final List<AutoCloseable> closeables = Collections.synchronizedList(new ArrayList<>());

  /**
   * Wrap and register the callback into an idempotent {@link AutoCloseable}.
   *
   * @param closeCallback
   * @return the callback as an idempotent {@link AutoCloseable}
   */
  AutoCloseable wrap(CloseCallback closeCallback) {
    AtomicBoolean closingOrAlreadyClosed = new AtomicBoolean(false);
    AutoCloseable idempotentCloseCallback =
        new AutoCloseable() {
          @Override
          public void close() throws Exception {
            if (closingOrAlreadyClosed.compareAndSet(false, true)) {
              closeCallback.run();
            }
          }

          @Override
          public String toString() {
            return closeCallback.toString();
          }
        };
    closeables.add(idempotentCloseCallback);
    return idempotentCloseCallback;
  }

  /** Close all the registered callbacks, in the reverse order of registration. */
  @Override
  public synchronized void close() {
    if (!closeables.isEmpty()) {
      for (int i = closeables.size() - 1; i >= 0; i--) {
        try {
          closeables.get(i).close();
        } catch (Exception e) {
          LOGGER.warn("Could not properly execute closing step '{}'", closeables.get(i), e);
        }
      }
    }
  }

  @FunctionalInterface
  interface CloseCallback {

    void run() throws Exception;
  }
}

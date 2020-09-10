// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class Utils {

  private Utils() {}

  static Runnable makeIdempotent(Runnable action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return () -> {
      if (executed.compareAndSet(false, true)) {
        action.run();
      }
    };
  }

  static <T> Consumer<T> makeIdempotent(Consumer<T> action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return t -> {
      if (executed.compareAndSet(false, true)) {
        action.accept(t);
      }
    };
  }
}

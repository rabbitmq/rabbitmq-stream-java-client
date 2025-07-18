// Copyright (c) 2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

import java.util.ArrayList;
import java.util.Collection;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamClientTestExtension implements BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamClientTestExtension.class);
  private static final ExtensionContext.Namespace NS =
      ExtensionContext.Namespace.create(StreamClientTestExtension.class);

  @Override
  public void beforeEach(ExtensionContext ctx) {
    ExtensionContext.Store store = store(ctx);
    store.put("threads", threads());
  }

  @Override
  public void afterEach(ExtensionContext ctx) {
    @SuppressWarnings("unchecked")
    Collection<Thread> initialThreads = (Collection<Thread>) store(ctx).remove("threads");
    if (initialThreads != null) {
      Collection<Thread> threads = threads();
      if (threads.size() > initialThreads.size()) {
        Collection<Thread> diff = new ArrayList<>(threads);
        diff.removeAll(initialThreads);
        LOGGER.warn(
            "[{}] There should be no new threads, initial {}, current {} (diff: {})",
            ctx.getTestMethod().get().getName(),
            initialThreads.size(),
            threads.size(),
            diff);
      }
    } else {
      LOGGER.warn("No threads in test context");
    }
  }

  private static ExtensionContext.Store store(ExtensionContext ctx) {
    return ctx.getStore(NS);
  }

  private Collection<Thread> threads() {
    return Thread.getAllStackTraces().keySet();
  }
}

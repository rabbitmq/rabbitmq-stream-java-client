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
package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import java.nio.channels.ClosedChannelException;
import java.util.function.Predicate;

final class CoordinatorUtils {

  private static final Predicate<Throwable> REFRESH_CANDIDATES =
      e ->
          e instanceof ConnectionStreamException
              || e instanceof ClientClosedException
              || e instanceof StreamNotAvailableException
              || e instanceof ClosedChannelException;

  private CoordinatorUtils() {}

  static boolean shouldRefreshCandidates(Throwable e) {
    return REFRESH_CANDIDATES.test(e) || REFRESH_CANDIDATES.test(e.getCause());
  }

  static class ClientClosedException extends StreamException {

    public ClientClosedException() {
      super("Client already closed");
    }
  }
}

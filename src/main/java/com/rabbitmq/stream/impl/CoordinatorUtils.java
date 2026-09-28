// Copyright (c) 2025-2026 Broadcom. All Rights Reserved.
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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.impl.AgentStateMachine.State;
import io.netty.util.concurrent.EventExecutorGroup;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CoordinatorUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorUtils.class);

  // insurance against an agent stuck in RECOVERING because of a bug not yet found: every known way
  // to get stuck is already fixed by the epoch-supersede mechanism the watchdog itself uses, so
  // the threshold is generous, not tuned to any known failure timing
  static final long WATCHDOG_TICK_INTERVAL_MS = SECONDS.toMillis(30);
  static final long WATCHDOG_STUCK_THRESHOLD_NANOS = SECONDS.toNanos(120);

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

  /**
   * The back-off delay for an attempt, in nanoseconds, or 0 if the policy has given up.
   *
   * <p>{@link BackOffDelayPolicy#TIMEOUT} is {@code Duration.ofMillis(Long.MAX_VALUE)}, so it has
   * to be excluded before converting: {@code toNanos()} would overflow on it.
   */
  static long backOffNanos(BackOffDelayPolicy delayPolicy, int attempts) {
    Duration delay = delayPolicy.delay(attempts);
    return BackOffDelayPolicy.TIMEOUT.equals(delay) ? 0 : delay.toNanos();
  }

  /**
   * Whether the watchdog should start a fresh attempt for an agent in this state.
   *
   * <p>Measured against when the current attempt is <b>due</b>, not when it was created: an agent
   * waiting out its back-off delay is waiting by design, not stuck, so comparing against the
   * creation time would let the watchdog cut short any configured delay longer than the stuck
   * threshold.
   */
  static boolean watchdogShouldReDispatch(State state, long nextAttemptAt, long now) {
    // subtraction, not a direct comparison, so this stays correct across a nanoTime() wraparound
    return state == State.RECOVERING && now - nextAttemptAt > WATCHDOG_STUCK_THRESHOLD_NANOS;
  }

  static void closeEventExecutorGroup(EventExecutorGroup group) {
    try {
      if (!group.isShuttingDown()) {
        // no quiet period: the loop is a control plane, there is no in-flight batch to drain
        group.shutdownGracefully(0, 10, SECONDS).get(10, SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.info("Error while closing coordinator event executor group: {}", e.getMessage());
    }
  }

  static class ClientClosedException extends StreamException {

    public ClientClosedException() {
      super("Client already closed");
    }
  }
}

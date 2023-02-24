// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import java.time.Duration;

/**
 * Contract to determine a delay between attempts of some task.
 *
 * <p>The task is typically the creation of a connection.
 *
 * @see EnvironmentBuilder#recoveryBackOffDelayPolicy(BackOffDelayPolicy)
 * @see EnvironmentBuilder#topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy)
 */
public interface BackOffDelayPolicy {

  Duration TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

  /**
   * A policy with a constant delay.
   *
   * @param delay
   * @return the constant delay policy
   */
  static BackOffDelayPolicy fixed(Duration delay) {
    return new FixedWithInitialDelayBackOffPolicy(delay, delay);
  }

  /**
   * A policy with a first delay and then a constant delay.
   *
   * @param initialDelay
   * @param delay
   * @return the policy with an initial delay
   */
  static BackOffDelayPolicy fixedWithInitialDelay(Duration initialDelay, Duration delay) {
    return new FixedWithInitialDelayBackOffPolicy(initialDelay, delay);
  }

  /**
   * A policy with a first delay, then a constant delay until a timeout is reached.
   *
   * @param initialDelay
   * @param delay
   * @param timeout
   * @return the policy with an initial delay
   */
  static BackOffDelayPolicy fixedWithInitialDelay(
      Duration initialDelay, Duration delay, Duration timeout) {
    return new FixedWithInitialDelayAndTimeoutBackOffPolicy(initialDelay, delay, timeout);
  }

  /**
   * Returns the delay to use for a given attempt.
   *
   * <p>The policy can return the TIMEOUT constant to indicate that the task has reached a timeout.
   *
   * @param recoveryAttempt
   * @return the delay, TIMEOUT if the task should stop being retried
   */
  Duration delay(int recoveryAttempt);

  class FixedWithInitialDelayBackOffPolicy implements BackOffDelayPolicy {

    private final Duration initialDelay;
    private final Duration delay;

    private FixedWithInitialDelayBackOffPolicy(Duration initialDelay, Duration delay) {
      this.initialDelay = initialDelay;
      this.delay = delay;
    }

    @Override
    public Duration delay(int recoveryAttempt) {
      return recoveryAttempt == 0 ? initialDelay : delay;
    }

    @Override
    public String toString() {
      return "FixedWithInitialDelayBackOffPolicy{"
          + "initialDelay="
          + initialDelay
          + ", delay="
          + delay
          + '}';
    }
  }

  class FixedWithInitialDelayAndTimeoutBackOffPolicy implements BackOffDelayPolicy {

    private final int attemptLimitBeforeTimeout;
    private final BackOffDelayPolicy delegate;

    private FixedWithInitialDelayAndTimeoutBackOffPolicy(
        Duration initialDelay, Duration delay, Duration timeout) {
      this(fixedWithInitialDelay(initialDelay, delay), timeout);
    }

    private FixedWithInitialDelayAndTimeoutBackOffPolicy(
        BackOffDelayPolicy policy, Duration timeout) {
      if (timeout.toMillis() < policy.delay(0).toMillis()) {
        throw new IllegalArgumentException("Timeout must be longer than initial delay");
      }
      this.delegate = policy;
      // best effort, assume FixedWithInitialDelay-ish policy
      Duration initialDelay = policy.delay(0);
      Duration delay = policy.delay(1);
      long timeoutWithInitialDelay = timeout.toMillis() - initialDelay.toMillis();
      this.attemptLimitBeforeTimeout = (int) (timeoutWithInitialDelay / delay.toMillis()) + 1;
    }

    @Override
    public Duration delay(int recoveryAttempt) {
      if (recoveryAttempt >= attemptLimitBeforeTimeout) {
        return TIMEOUT;
      } else {
        return delegate.delay(recoveryAttempt);
      }
    }

    @Override
    public String toString() {
      return "FixedWithInitialDelayAndTimeoutBackOffPolicy{"
          + "attemptLimitBeforeTimeout="
          + attemptLimitBeforeTimeout
          + ", delegate="
          + delegate
          + '}';
    }
  }
}

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

import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import org.assertj.core.api.AbstractObjectAssert;

final class Assertions {

  private Assertions() {}

  static SyncAssert assertThat(TestUtils.Sync sync) {
    return new SyncAssert(sync);
  }

  static class SyncAssert extends AbstractObjectAssert<SyncAssert, TestUtils.Sync> {

    private SyncAssert(TestUtils.Sync sync) {
      super(sync, SyncAssert.class);
    }

    SyncAssert completes() {
      return this.completes(TestUtils.DEFAULT_CONDITION_TIMEOUT);
    }

    SyncAssert completes(Duration timeout) {
      boolean completed = actual.await(timeout);
      if (!completed) {
        fail(
            "Sync timed out after %d ms (current count is %d)",
            timeout.toMillis(), actual.currentCount());
      }
      return this;
    }

    SyncAssert hasCompleted() {
      if (!this.actual.hasCompleted()) {
        fail("Sync should have completed but has not");
      }
      return this;
    }

    SyncAssert hasNotCompleted() {
      if (this.actual.hasCompleted()) {
        fail("Sync should have not completed");
      }
      return this;
    }
  }
}

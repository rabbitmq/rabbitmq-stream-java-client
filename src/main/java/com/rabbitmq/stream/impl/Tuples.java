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

final class Tuples {

  private Tuples() {}

  static <A, B> Pair<A, B> pair(A v1, B v2) {
    return new Pair<>(v1, v2);
  }

  static class Pair<A, B> {

    private final A v1;
    private final B v2;

    private Pair(A v1, B v2) {
      this.v1 = v1;
      this.v2 = v2;
    }

    A v1() {
      return this.v1;
    }

    B v2() {
      return this.v2;
    }
  }
}

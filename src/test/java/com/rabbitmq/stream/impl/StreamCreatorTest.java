// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.ByteCapacity;
import org.junit.jupiter.api.Test;

public class StreamCreatorTest {

  @Test
  void maxSegmentSizeBytesOK() {
    new StreamStreamCreator(null)
        .maxSegmentSizeBytes(ByteCapacity.MB(500))
        .maxSegmentSizeBytes(ByteCapacity.MB(2999))
        .maxSegmentSizeBytes(ByteCapacity.GB(3));
  }

  @Test
  void maxSegmentSizeBytesKO() {
    assertThatThrownBy(
            () -> {
              new StreamStreamCreator(null).maxSegmentSizeBytes(ByteCapacity.MB(3001));
            })
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> {
              new StreamStreamCreator(null).maxSegmentSizeBytes(ByteCapacity.GB(4));
            })
        .isInstanceOf(IllegalArgumentException.class);
  }
}

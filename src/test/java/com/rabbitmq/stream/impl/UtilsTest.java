// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.Constants.CODE_MESSAGE_ENQUEUEING_FAILED;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_OK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class UtilsTest {

  @Test
  void formatConstantOk() {
    assertThat(formatConstant(RESPONSE_CODE_OK)).isEqualTo("0 (OK)");
    assertThat(formatConstant(RESPONSE_CODE_STREAM_DOES_NOT_EXIST))
        .isEqualTo("1 (STREAM_DOES_NOT_EXIST)");
    assertThat(formatConstant(CODE_MESSAGE_ENQUEUEING_FAILED))
        .isEqualTo("10000 (MESSAGE_ENQUEUEING_FAILED)");
  }
}

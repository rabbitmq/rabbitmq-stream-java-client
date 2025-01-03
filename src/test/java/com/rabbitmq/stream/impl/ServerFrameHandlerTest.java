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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.impl.ServerFrameHandler.FrameHandlerInfo;
import org.junit.jupiter.api.Test;

public class ServerFrameHandlerTest {

  @Test
  void commandVersionsHasDeliver() {
    FrameHandlerInfo deliverInfo =
        ServerFrameHandler.commandVersions().stream()
            .filter(info -> info.getKey() == Constants.COMMAND_DELIVER)
            .findFirst()
            .get();

    assertThat(deliverInfo.getKey()).isEqualTo(Constants.COMMAND_DELIVER);
    assertThat(deliverInfo.getMinVersion()).isEqualTo(Constants.VERSION_1);
    assertThat(deliverInfo.getMaxVersion()).isGreaterThanOrEqualTo(Constants.VERSION_2);
  }
}

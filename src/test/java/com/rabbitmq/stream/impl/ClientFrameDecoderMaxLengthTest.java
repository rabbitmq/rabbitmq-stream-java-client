// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

public class ClientFrameDecoderMaxLengthTest {

  @Test
  void frameDecoderMaxLengthAccountsForLengthFieldItself() {
    assertThat(Client.frameDecoderMaxLength(1_048_576)).isEqualTo(1_048_576 + 4);
  }

  @Test
  void frameDecoderMaxLengthIsUnboundedWhenRequestedSizeIsZeroOrLess() {
    assertThat(Client.frameDecoderMaxLength(0)).isEqualTo(Integer.MAX_VALUE);
    assertThat(Client.frameDecoderMaxLength(-1)).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void decoderAcceptsFrameAtOrUnderTheCap() {
    EmbeddedChannel channel = channel(128);
    assertThat(channel.writeInbound(frame(128))).isTrue();
    ByteBuf decoded = channel.readInbound();
    assertThat(decoded.readableBytes()).isEqualTo(128);
    decoded.release();
  }

  @Test
  void decoderRejectsFrameDeclaringMoreThanTheCap() {
    EmbeddedChannel channel = channel(128);
    assertThatThrownBy(() -> channel.writeInbound(frame(129)))
        .isInstanceOf(TooLongFrameException.class);
  }

  private static EmbeddedChannel channel(int requestedMaxFrameSize) {
    return new EmbeddedChannel(Client.frameDecoder(requestedMaxFrameSize));
  }

  private static ByteBuf frame(int bodySize) {
    ByteBuf bb = Unpooled.buffer(4 + bodySize);
    bb.writeInt(bodySize);
    bb.writeZero(bodySize);
    return bb;
  }
}

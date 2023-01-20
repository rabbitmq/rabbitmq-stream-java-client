// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.EnvironmentBuilder.NettyConfiguration;
import io.netty.channel.Channel;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An extension point to customize Netty's {@link io.netty.channel.Channel}s used for connection.
 *
 * @deprecated use {@link NettyConfiguration#channelCustomizer(Consumer)} from {@link
 *     EnvironmentBuilder#netty()} instead
 * @see NettyConfiguration#netty()
 */
public interface ChannelCustomizer {

  void customize(Channel channel);

  default ChannelCustomizer andThen(ChannelCustomizer after) {
    Objects.requireNonNull(after);
    return ch -> {
      customize(ch);
      after.customize(ch);
    };
  }
}

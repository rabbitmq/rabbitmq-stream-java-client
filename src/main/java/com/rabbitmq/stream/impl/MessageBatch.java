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

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.compression.Compression;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;

public final class MessageBatch {

  final Compression compression;
  final List<Message> messages;

  public MessageBatch(Compression compression) {
    this(compression, new ArrayList<>());
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public MessageBatch(Compression compression, List<Message> messages) {
    this.compression = compression;
    this.messages = messages;
  }

  public MessageBatch add(Message message) {
    this.messages.add(message);
    return this;
  }
}

// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

/**
 * Codec to encode and decode messages.
 *
 * <p>The codec is expected to use the AMQP 1.0 message format.
 *
 * <p>This is considered a SPI and is susceptible to change at any time.
 */
public interface Codec {

  EncodedMessage encode(Message message);

  Message decode(byte[] data);

  MessageBuilder messageBuilder();

  class EncodedMessage {

    private final int size;
    private final byte[] data;

    public EncodedMessage(int size, byte[] data) {
      this.size = size;
      this.data = data;
    }

    public byte[] getData() {
      return data;
    }

    public int getSize() {
      return size;
    }
  }
}

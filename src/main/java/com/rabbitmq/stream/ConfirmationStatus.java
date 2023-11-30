// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
// Inc. and/or its subsidiaries.
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
 * The status to confirm or fail a message.
 *
 * @see ConfirmationHandler
 * @see Producer#send(Message, ConfirmationHandler)
 */
public class ConfirmationStatus {

  private final Message message;

  private final boolean confirmed;

  private final short code;

  public ConfirmationStatus(Message message, boolean confirmed, short code) {
    this.message = message;
    this.confirmed = confirmed;
    this.code = code;
  }

  public Message getMessage() {
    return message;
  }

  /**
   * Whether the message is confirmed or not.
   *
   * @return true if the message is confirmed by the broker, false otherwise
   */
  public boolean isConfirmed() {
    return confirmed;
  }

  /**
   * The status code.
   *
   * @return status code
   * @see Constants
   */
  public short getCode() {
    return code;
  }
}

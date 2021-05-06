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

package com.rabbitmq.stream;

/**
 * Generic stream exception.
 *
 * @see Constants
 */
public class StreamException extends RuntimeException {

  private static final long serialVersionUID = 1712097491684870324L;

  private final short code;

  public StreamException(String message) {
    super(message);
    this.code = -1;
  }

  public StreamException(String message, short code) {
    super(message);
    this.code = code;
  }

  public StreamException(Throwable cause) {
    super(null, cause);
    this.code = -1;
  }

  public StreamException(String message, Throwable cause) {
    super(message, cause);
    this.code = -1;
  }

  public short getCode() {
    return code;
  }
}

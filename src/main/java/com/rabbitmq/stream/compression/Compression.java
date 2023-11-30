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
package com.rabbitmq.stream.compression;

/** Enum to define types of compression codecs. */
public enum Compression {
  NONE((byte) 0),
  GZIP((byte) 1),
  SNAPPY((byte) 2),
  LZ4((byte) 3),
  ZSTD((byte) 4);

  private static final Compression[] COMPRESSIONS =
      new Compression[] {NONE, GZIP, SNAPPY, LZ4, ZSTD};
  byte code;

  Compression(byte code) {
    this.code = code;
  }

  public static Compression get(byte code) {
    return COMPRESSIONS[code];
  }

  public byte code() {
    return this.code;
  }
}

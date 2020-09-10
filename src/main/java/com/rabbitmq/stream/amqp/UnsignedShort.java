// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.amqp;

/** This code is based on QPid Proton's {@link org.apache.qpid.proton.amqp.UnsignedShort} class. */
public final class UnsignedShort extends Number implements Comparable<UnsignedShort> {
  public static final UnsignedShort MAX_VALUE = new UnsignedShort((short) -1);
  private static final UnsignedShort[] cachedValues = new UnsignedShort[256];

  static {
    for (short i = 0; i < 256; i++) {
      cachedValues[i] = new UnsignedShort(i);
    }
  }

  private final short _underlying;

  public UnsignedShort(short underlying) {
    _underlying = underlying;
  }

  public static UnsignedShort valueOf(short underlying) {
    if ((underlying & 0xFF00) == 0) {
      return cachedValues[underlying];
    } else {
      return new UnsignedShort(underlying);
    }
  }

  public static UnsignedShort valueOf(final String value) {
    int intVal = Integer.parseInt(value);
    if (intVal < 0 || intVal >= (1 << 16)) {
      throw new NumberFormatException(
          "Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 16) + ").");
    }
    return valueOf((short) intVal);
  }

  public short shortValue() {
    return _underlying;
  }

  @Override
  public int intValue() {
    return _underlying & 0xFFFF;
  }

  @Override
  public long longValue() {
    return ((long) _underlying) & 0xFFFFl;
  }

  @Override
  public float floatValue() {
    return (float) intValue();
  }

  @Override
  public double doubleValue() {
    return intValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UnsignedShort that = (UnsignedShort) o;

    return _underlying == that._underlying;
  }

  public int compareTo(UnsignedShort o) {
    return Integer.signum(intValue() - o.intValue());
  }

  @Override
  public int hashCode() {
    return _underlying;
  }

  @Override
  public String toString() {
    return String.valueOf(longValue());
  }
}

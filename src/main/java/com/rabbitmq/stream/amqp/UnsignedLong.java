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
package com.rabbitmq.stream.amqp;

import java.math.BigInteger;

/** This code is based on QPid Proton's {@link org.apache.qpid.proton.amqp.UnsignedLong} class. */
public final class UnsignedLong extends Number implements Comparable<UnsignedLong> {
  private static final BigInteger TWO_TO_THE_SIXTY_FOUR =
      new BigInteger(
          new byte[] {
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0
          });
  private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

  private static final UnsignedLong[] cachedValues = new UnsignedLong[256];
  public static final UnsignedLong ZERO = cachedValues[0];

  static {
    for (int i = 0; i < 256; i++) {
      cachedValues[i] = new UnsignedLong(i);
    }
  }

  private final long _underlying;

  public UnsignedLong(long underlying) {
    _underlying = underlying;
  }

  public static UnsignedLong valueOf(long underlying) {
    if ((underlying & 0xFFL) == underlying) {
      return cachedValues[(int) underlying];
    } else {
      return new UnsignedLong(underlying);
    }
  }

  public static UnsignedLong valueOf(final String value) {
    BigInteger bigInt = new BigInteger(value);

    return valueOf(bigInt);
  }

  public static UnsignedLong valueOf(BigInteger bigInt) {
    if (bigInt.signum() == -1 || bigInt.bitLength() > 64) {
      throw new NumberFormatException(
          "Value \"" + bigInt + "\" lies outside the range [0 - 2^64).");
    } else if (bigInt.compareTo(LONG_MAX_VALUE) >= 0) {
      return UnsignedLong.valueOf(bigInt.longValue());
    } else {
      return UnsignedLong.valueOf(TWO_TO_THE_SIXTY_FOUR.subtract(bigInt).negate().longValue());
    }
  }

  @Override
  public int intValue() {
    return (int) _underlying;
  }

  @Override
  public long longValue() {
    return _underlying;
  }

  public BigInteger bigIntegerValue() {
    if (_underlying >= 0L) {
      return BigInteger.valueOf(_underlying);
    } else {
      return TWO_TO_THE_SIXTY_FOUR.add(BigInteger.valueOf(_underlying));
    }
  }

  @Override
  public float floatValue() {
    return (float) longValue();
  }

  @Override
  public double doubleValue() {
    return (double) longValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UnsignedLong that = (UnsignedLong) o;

    return _underlying == that._underlying;
  }

  public int compareTo(UnsignedLong o) {
    return bigIntegerValue().compareTo(o.bigIntegerValue());
  }

  @Override
  public int hashCode() {
    return (int) (_underlying ^ (_underlying >>> 32));
  }

  @Override
  public String toString() {
    return String.valueOf(bigIntegerValue());
  }
}

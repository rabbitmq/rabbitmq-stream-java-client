// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

/**
 * This code is based on QPid Proton's {@link org.apache.qpid.proton.amqp.UnsignedInteger} class.
 */
public final class UnsignedInteger extends Number implements Comparable<UnsignedInteger> {
    public static final UnsignedInteger MAX_VALUE = new UnsignedInteger(0xffffffff);
    private static final UnsignedInteger[] cachedValues = new UnsignedInteger[256];
    public static final UnsignedInteger ZERO = cachedValues[0];
    public static final UnsignedInteger ONE = cachedValues[1];

    static {
        for (int i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedInteger(i);
        }
    }

    private final int _underlying;


    public UnsignedInteger(int underlying) {
        _underlying = underlying;
    }

    public static UnsignedInteger valueOf(int underlying) {
        if ((underlying & 0xFFFFFF00) == 0) {
            return cachedValues[underlying];
        } else {
            return new UnsignedInteger(underlying);
        }
    }

    public static UnsignedInteger valueOf(final String value) {
        long longVal = Long.parseLong(value);
        return valueOf(longVal);
    }

    public static UnsignedInteger valueOf(final long longVal) {
        if (longVal < 0L || longVal >= (1L << 32)) {
            throw new NumberFormatException("Value \"" + longVal + "\" lies outside the range [" + 0L + "-" + (1L << 32) + ").");
        }
        return valueOf((int) longVal);
    }

    @Override
    public int intValue() {
        return _underlying;
    }

    @Override
    public long longValue() {
        return ((long) _underlying) & 0xFFFFFFFFl;
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

        UnsignedInteger that = (UnsignedInteger) o;

        return _underlying == that._underlying;
    }

    public int compareTo(UnsignedInteger o) {
        return Long.signum(longValue() - o.longValue());
    }

    @Override
    public int hashCode() {
        return _underlying;
    }

    @Override
    public String toString() {
        return String.valueOf(longValue());
    }

    public UnsignedInteger add(final UnsignedInteger i) {
        int val = _underlying + i._underlying;
        return UnsignedInteger.valueOf(val);
    }

    public UnsignedInteger subtract(final UnsignedInteger i) {
        int val = _underlying - i._underlying;
        return UnsignedInteger.valueOf(val);
    }

}

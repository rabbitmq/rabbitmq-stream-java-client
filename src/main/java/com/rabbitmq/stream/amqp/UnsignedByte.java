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

/**
 * This code is based on QPid Proton's {@link org.apache.qpid.proton.amqp.UnsignedByte} class.
 */
public final class UnsignedByte extends Number implements Comparable<org.apache.qpid.proton.amqp.UnsignedByte> {
    private static final UnsignedByte[] cachedValues = new UnsignedByte[256];

    static {
        for (int i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedByte((byte) i);
        }
    }

    private final byte _underlying;

    public UnsignedByte(byte underlying) {
        _underlying = underlying;
    }

    public static UnsignedByte valueOf(byte underlying) {
        final int index = ((int) underlying) & 0xFF;
        return cachedValues[index];
    }

    public static UnsignedByte valueOf(final String value)
            throws NumberFormatException {
        int intVal = Integer.parseInt(value);
        if (intVal < 0 || intVal >= (1 << 8)) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 8) + ").");
        }
        return valueOf((byte) intVal);
    }

    @Override
    public byte byteValue() {
        return _underlying;
    }

    @Override
    public short shortValue() {
        return (short) intValue();
    }

    @Override
    public int intValue() {
        return ((int) _underlying) & 0xFF;
    }

    @Override
    public long longValue() {
        return ((long) _underlying) & 0xFFl;
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

        UnsignedByte that = (UnsignedByte) o;

        return _underlying == that._underlying;
    }

    public int compareTo(org.apache.qpid.proton.amqp.UnsignedByte o) {
        return Integer.signum(intValue() - o.intValue());
    }

    @Override
    public int hashCode() {
        return _underlying;
    }

    @Override
    public String toString() {
        return String.valueOf(intValue());
    }
}

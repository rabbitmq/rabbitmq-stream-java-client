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

import java.math.BigDecimal;

/**
 * This code is based on QPid Proton's {@link org.apache.qpid.proton.amqp.Decimal32} class.
 */
public final class Decimal32 extends Number {
    private final BigDecimal _underlying;
    private final int _bits;

    public Decimal32(BigDecimal underlying) {
        _underlying = underlying;
        _bits = calculateBits(underlying);
    }

    public Decimal32(final int bits) {
        _bits = bits;
        _underlying = calculateBigDecimal(bits);
    }

    static int calculateBits(final BigDecimal underlying) {
        return 0;  //TODO.
    }

    static BigDecimal calculateBigDecimal(int bits) {
        return BigDecimal.ZERO; // TODO
    }


    @Override
    public int intValue() {
        return _underlying.intValue();
    }

    @Override
    public long longValue() {
        return _underlying.longValue();
    }

    @Override
    public float floatValue() {
        return _underlying.floatValue();
    }

    @Override
    public double doubleValue() {
        return _underlying.doubleValue();
    }

    public int getBits() {
        return _bits;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Decimal32 decimal32 = (Decimal32) o;

        return _bits == decimal32._bits;
    }

    @Override
    public int hashCode() {
        return _bits;
    }
}

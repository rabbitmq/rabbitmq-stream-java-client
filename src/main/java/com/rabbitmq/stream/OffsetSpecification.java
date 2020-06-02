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

package com.rabbitmq.stream;

public class OffsetSpecification {

    private static final short FIRST = 0;
    private static final short LAST = 1;
    private static final short NEXT = 2;
    private static final short OFFSET = 3;
    private static final short TIMESTAMP = 4;

    private static final long UNUSED_OFFSET = -1;

    private final short type;
    private final long offset;

    private OffsetSpecification(short type, long offset) {
        this.type = type;
        this.offset = offset;
    }

    public static OffsetSpecification first() {
        return new OffsetSpecification(FIRST, UNUSED_OFFSET);
    }

    public static OffsetSpecification last() {
        return new OffsetSpecification(LAST, UNUSED_OFFSET);
    }

    public static OffsetSpecification next() {
        return new OffsetSpecification(NEXT, UNUSED_OFFSET);
    }

    public static OffsetSpecification offset(long offset) {
        return new OffsetSpecification(OFFSET, offset);
    }

    public static OffsetSpecification timestamp(long timestamp) {
        return new OffsetSpecification(TIMESTAMP, timestamp);
    }

    boolean isOffset() {
        return this.type == OFFSET;
    }

    boolean isTimestamp() {
        return this.type == TIMESTAMP;
    }

    short getType() {
        return type;
    }

    long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "OffsetSpecification{" +
                "type=" + type +
                ", offset=" + offset +
                '}';
    }
}

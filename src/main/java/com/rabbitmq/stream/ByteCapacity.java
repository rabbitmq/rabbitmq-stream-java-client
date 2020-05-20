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

public class ByteCapacity {

    private static final int KILOBYTES_MULTIPLIER = 1000;
    private static final int MEGABYTES_MULTIPLIER = 1000 * 1000;
    private static final int GIGABYTES_MULTIPLIER = 1000 * 1000 * 1000;
    private static final int TERABYTES_MULTIPLIER = 1000 * 1000 * 1000;

    private final long bytes;

    private ByteCapacity(long bytes) {
        this.bytes = bytes;
    }

    public static ByteCapacity B(long bytes) {
        return new ByteCapacity(bytes);
    }

    public static ByteCapacity kB(long kilobytes) {
        return new ByteCapacity(kilobytes * KILOBYTES_MULTIPLIER);
    }

    public static ByteCapacity MB(long megabytes) {
        return new ByteCapacity(megabytes * MEGABYTES_MULTIPLIER);
    }

    public static ByteCapacity GB(long gigabytes) {
        return new ByteCapacity(gigabytes * GIGABYTES_MULTIPLIER);
    }

    public static ByteCapacity TB(long terabytes) {
        return new ByteCapacity(terabytes * TERABYTES_MULTIPLIER);
    }

    public long toBytes() {
        return bytes;
    }

}

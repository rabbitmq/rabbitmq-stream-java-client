// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.impl;

import java.nio.charset.StandardCharsets;
import java.util.function.ToIntFunction;

final class HashUtils {

  static final ToIntFunction<String> MURMUR3 = new Murmur3();

  private HashUtils() {}

  // from
  // https://github.com/apache/commons-codec/blob/rel/commons-codec-1.15/src/main/java/org/apache/commons/codec/digest/MurmurHash3.java
  static class Murmur3 implements ToIntFunction<String> {

    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    private static int getLittleEndianInt(final byte[] data, final int index) {
      return ((data[index] & 0xff))
          | ((data[index + 1] & 0xff) << 8)
          | ((data[index + 2] & 0xff) << 16)
          | ((data[index + 3] & 0xff) << 24);
    }

    private static int mix32(int k, int hash) {
      k *= C1_32;
      k = Integer.rotateLeft(k, R1_32);
      k *= C2_32;
      hash ^= k;
      return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
    }

    private static int fmix32(int hash) {
      hash ^= (hash >>> 16);
      hash *= 0x85ebca6b;
      hash ^= (hash >>> 13);
      hash *= 0xc2b2ae35;
      hash ^= (hash >>> 16);
      return hash;
    }

    @Override
    public int applyAsInt(String value) {
      byte[] data = value.getBytes(StandardCharsets.UTF_8);
      final int offset = 0;
      final int length = data.length;
      final int seed = 104729;
      int hash = seed;
      final int nblocks = length >> 2;

      // body
      for (int i = 0; i < nblocks; i++) {
        final int index = offset + (i << 2);
        final int k = getLittleEndianInt(data, index);
        hash = mix32(k, hash);
      }

      // tail
      final int index = offset + (nblocks << 2);
      int k1 = 0;
      switch (offset + length - index) {
        case 3:
          k1 ^= (data[index + 2] & 0xff) << 16;
        case 2:
          k1 ^= (data[index + 1] & 0xff) << 8;
        case 1:
          k1 ^= (data[index] & 0xff);

          // mix functions
          k1 *= C1_32;
          k1 = Integer.rotateLeft(k1, R1_32);
          k1 *= C2_32;
          hash ^= k1;
      }

      hash ^= length;
      return fmix32(hash);
    }
  }
}

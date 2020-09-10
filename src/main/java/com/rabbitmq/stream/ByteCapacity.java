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

package com.rabbitmq.stream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteCapacity {

  private static final String UNIT_MB = "mb";
  private static final int KILOBYTES_MULTIPLIER = 1000;
  private static final int MEGABYTES_MULTIPLIER = 1000 * 1000;
  private static final int GIGABYTES_MULTIPLIER = 1000 * 1000 * 1000;
  private static final long TERABYTES_MULTIPLIER = 1000L * 1000L * 1000L * 1000L;

  private static final Pattern PATTERN =
      Pattern.compile("^(?<size>\\d*)((?<unit>kb|mb|gb|tb))?$", Pattern.CASE_INSENSITIVE);
  private static final String GROUP_SIZE = "size";
  private static final String GROUP_UNIT = "unit";
  private static final String UNIT_KB = "kb";
  private static final String UNIT_GB = "gb";
  private static final String UNIT_TB = "tb";

  private static final Map<String, Function<Long, ByteCapacity>> CONSTRUCTORS =
      Collections.unmodifiableMap(
          new HashMap<String, Function<Long, ByteCapacity>>() {
            {
              put(UNIT_KB, size -> ByteCapacity.kB(size));
              put(UNIT_MB, size -> ByteCapacity.MB(size));
              put(UNIT_GB, size -> ByteCapacity.GB(size));
              put(UNIT_TB, size -> ByteCapacity.TB(size));
            }
          });

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

  public static ByteCapacity from(String value) {
    Matcher matcher = PATTERN.matcher(value);
    if (matcher.matches()) {
      long size = Long.valueOf(matcher.group(GROUP_SIZE));
      String unit = matcher.group(GROUP_UNIT);
      ByteCapacity result;
      if (unit == null) {
        result = ByteCapacity.B(size);
      } else {
        return CONSTRUCTORS
            .getOrDefault(
                unit.toLowerCase(),
                s -> {
                  throw new IllegalArgumentException("Unknown capacity unit: " + unit);
                })
            .apply(size);
      }
      return result;
    } else {
      throw new IllegalArgumentException("Cannot parse value for byte capacity: " + value);
    }
  }
}

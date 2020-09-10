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

import java.util.Objects;

public class OffsetSpecification {

  private static final short TYPE_FIRST = 0;
  private static final short TYPE_LAST = 1;
  private static final short TYPE_NEXT = 2;
  private static final short TYPE_OFFSET = 3;
  private static final short TYPE_TIMESTAMP = 4;

  private static final long UNUSED_OFFSET = -1;

  private static final OffsetSpecification FIRST =
      new OffsetSpecification(TYPE_FIRST, UNUSED_OFFSET);
  private static final OffsetSpecification LAST = new OffsetSpecification(TYPE_LAST, UNUSED_OFFSET);
  private static final OffsetSpecification NEXT = new OffsetSpecification(TYPE_NEXT, UNUSED_OFFSET);

  private final short type;
  private final long offset;

  private OffsetSpecification(short type, long offset) {
    this.type = type;
    this.offset = offset;
  }

  public static OffsetSpecification first() {
    return FIRST;
  }

  public static OffsetSpecification last() {
    return LAST;
  }

  public static OffsetSpecification next() {
    return NEXT;
  }

  public static OffsetSpecification offset(long offset) {
    return new OffsetSpecification(TYPE_OFFSET, offset);
  }

  public static OffsetSpecification timestamp(long timestamp) {
    return new OffsetSpecification(TYPE_TIMESTAMP, timestamp);
  }

  public boolean isOffset() {
    return this.type == TYPE_OFFSET;
  }

  public boolean isTimestamp() {
    return this.type == TYPE_TIMESTAMP;
  }

  public short getType() {
    return type;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "OffsetSpecification{" + "type=" + type + ", offset=" + offset + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OffsetSpecification that = (OffsetSpecification) o;
    return type == that.type && offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, offset);
  }
}

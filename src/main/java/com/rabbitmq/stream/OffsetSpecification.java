// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

/** API to specify an offset in a stream. */
public class OffsetSpecification {

  private static final short TYPE_NONE = 0;
  private static final short TYPE_FIRST = 1;
  private static final short TYPE_LAST = 2;
  private static final short TYPE_NEXT = 3;
  private static final short TYPE_OFFSET = 4;
  private static final short TYPE_TIMESTAMP = 5;

  private static final long UNUSED_OFFSET = -1;

  private static final OffsetSpecification NONE = new OffsetSpecification(TYPE_NONE, UNUSED_OFFSET);
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

  /**
   * When the offset specification is not relevant.
   *
   * @return none offset specification
   */
  public static OffsetSpecification none() {
    return NONE;
  }

  /**
   * The first available offset in the stream.
   *
   * <p>If the stream has not been truncated, this means the beginning of the stream (offset 0).
   *
   * @return first offset in a stream
   */
  public static OffsetSpecification first() {
    return FIRST;
  }

  /**
   * The last chunk of messages in the stream.
   *
   * @return the offset of the last chunk of messages
   */
  public static OffsetSpecification last() {
    return LAST;
  }

  /**
   * The next offset to be written, that is the end of the stream.
   *
   * @return The next offset to be written
   */
  public static OffsetSpecification next() {
    return NEXT;
  }

  /**
   * A specific offset in the stream.
   *
   * <p>The first offset of a non-truncated stream is 0.
   *
   * @param offset
   * @return the absolute offset
   */
  public static OffsetSpecification offset(long offset) {
    return new OffsetSpecification(TYPE_OFFSET, offset);
  }

  /**
   * Offset of messages stored after the specified timestamp.
   *
   * @param timestamp
   * @return messages stored after the specified timestamp
   */
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

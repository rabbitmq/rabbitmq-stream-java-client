// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import com.rabbitmq.stream.MessageHandler.Context;

/**
 * Statistics on a stream.
 *
 * @see Environment#queryStreamStats(String)
 */
public interface StreamStats {

  /**
   * The first offset in the stream.
   *
   * @return first offset in the stream
   * @throws NoOffsetException if there is no first offset yet
   */
  long firstOffset();

  /**
   * The ID (offset) of the committed chunk (block of messages) in the stream.
   *
   * <p>It is the offset of the first message in the last chunk confirmed by a quorum of the stream
   * cluster members (leader and replicas).
   *
   * <p>The committed chunk ID is a good indication of what the last offset of a stream can be at a
   * given time. The value can be stale as soon as the application reads it though, as the committed
   * chunk ID for a stream that is published to changes all the time.
   *
   * @return committed offset in this stream
   * @see Context#committedChunkId()
   * @throws NoOffsetException if there is no committed chunk yet
   */
  long committedChunkId();

  long committedOffset();
}

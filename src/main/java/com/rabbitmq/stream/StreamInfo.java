// Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
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
 * Information on a stream.
 *
 * @see Environment#queryStreamInfo(String)
 */
public interface StreamInfo {

  /**
   * The first offset in the stream.
   *
   * @return first offset in the stream
   * @throws NoOffsetException if there is no first offset yet
   */
  long firstOffset();

  /**
   * The committed offset in the stream.
   *
   * <p>It is the offset of the last message confirmed by a quorum of the stream cluster members
   * (leader and replicas).
   *
   * <p>The committed offset is a good indication of what the last offset of a stream is at a given
   * time. The value can be stale as soon as the application reads it though, as the committed
   * offset for a stream that is published to changes all the time.
   *
   * @return committed offset in this stream
   * @see Context#committedOffset()
   * @throws NoOffsetException if there is no committed offset yet
   */
  long committedOffset();
}

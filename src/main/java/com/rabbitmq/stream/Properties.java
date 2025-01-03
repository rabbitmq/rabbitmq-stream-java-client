// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import java.util.UUID;

/**
 * Message properties.
 *
 * <p>Please see section 3.2 "message format" of the AMQP 1.0 specification to find out about the
 * exact meaning of the message sections.
 */
public interface Properties {

  Object getMessageId();

  String getMessageIdAsString();

  long getMessageIdAsLong();

  byte[] getMessageIdAsBinary();

  UUID getMessageIdAsUuid();

  byte[] getUserId();

  String getTo();

  String getSubject();

  String getReplyTo();

  Object getCorrelationId();

  String getCorrelationIdAsString();

  long getCorrelationIdAsLong();

  byte[] getCorrelationIdAsBinary();

  UUID getCorrelationIdAsUuid();

  String getContentType();

  String getContentEncoding();

  long getAbsoluteExpiryTime();

  long getCreationTime();

  String getGroupId();

  long getGroupSequence();

  String getReplyToGroupId();
}

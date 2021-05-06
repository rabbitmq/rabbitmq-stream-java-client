// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

/**
 * Various constants (response codes, errors, etc)
 */
public final class Constants {

  public static final short RESPONSE_CODE_OK = 1;
  public static final short RESPONSE_CODE_STREAM_DOES_NOT_EXIST = 2;
  public static final short RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS = 3;
  public static final short RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST = 4;
  public static final short RESPONSE_CODE_STREAM_ALREADY_EXISTS = 5;
  public static final short RESPONSE_CODE_STREAM_NOT_AVAILABLE = 6;
  public static final short RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED = 7;
  public static final short RESPONSE_CODE_AUTHENTICATION_FAILURE = 8;
  public static final short RESPONSE_CODE_SASL_ERROR = 9;
  public static final short RESPONSE_CODE_SASL_CHALLENGE = 10;
  public static final short RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK = 11;
  public static final short RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE = 12;
  public static final short RESPONSE_CODE_UNKNOWN_FRAME = 13;
  public static final short RESPONSE_CODE_FRAME_TOO_LARGE = 14;
  public static final short RESPONSE_CODE_INTERNAL_ERROR = 15;
  public static final short RESPONSE_CODE_ACCESS_REFUSED = 16;
  public static final short RESPONSE_CODE_PRECONDITION_FAILED = 17;
  public static final short RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST = 18;

  public static final short CODE_MESSAGE_ENQUEUEING_FAILED = 10_001;
  public static final short CODE_PRODUCER_NOT_AVAILABLE = 10_002;
  public static final short CODE_PRODUCER_CLOSED = 10_003;
  public static final short CODE_PUBLISH_CONFIRM_TIMEOUT = 10_004;

  public static final short COMMAND_DECLARE_PUBLISHER = 1;
  public static final short COMMAND_PUBLISH = 2;
  public static final short COMMAND_PUBLISH_CONFIRM = 3;
  public static final short COMMAND_PUBLISH_ERROR = 4;
  public static final short COMMAND_QUERY_PUBLISHER_SEQUENCE = 5;
  public static final short COMMAND_DELETE_PUBLISHER = 6;
  public static final short COMMAND_SUBSCRIBE = 7;
  public static final short COMMAND_DELIVER = 8;
  public static final short COMMAND_CREDIT = 9;
  public static final short COMMAND_COMMIT_OFFSET = 10;
  public static final short COMMAND_QUERY_OFFSET = 11;
  public static final short COMMAND_UNSUBSCRIBE = 12;
  public static final short COMMAND_CREATE_STREAM = 13;
  public static final short COMMAND_DELETE_STREAM = 14;
  public static final short COMMAND_METADATA = 15;
  public static final short COMMAND_METADATA_UPDATE = 16;
  public static final short COMMAND_PEER_PROPERTIES = 17;
  public static final short COMMAND_SASL_HANDSHAKE = 18;
  public static final short COMMAND_SASL_AUTHENTICATE = 19;
  public static final short COMMAND_TUNE = 20;
  public static final short COMMAND_OPEN = 21;
  public static final short COMMAND_CLOSE = 22;
  public static final short COMMAND_HEARTBEAT = 23;
  public static final short COMMAND_ROUTE = 24;
  public static final short COMMAND_PARTITIONS = 25;

  public static final short VERSION_1 = 1;

  private Constants() {}
}

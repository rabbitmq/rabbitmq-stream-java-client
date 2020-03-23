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

public final class Constants {

    public static final short RESPONSE_CODE_OK = 0;
    public static final short RESPONSE_CODE_TARGET_DOES_NOT_EXIST = 1;
    public static final short RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS = 2;
    public static final short RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST = 3;
    public static final short RESPONSE_CODE_TARGET_ALREADY_EXISTS = 4;
    public static final short RESPONSE_CODE_TARGET_DELETED = 5;
    public static final short COMMAND_PUBLISH = 0;
    public static final short COMMAND_PUBLISH_CONFIRM = 1;
    public static final short COMMAND_SUBSCRIBE = 2;
    public static final short COMMAND_DELIVER = 3;
    public static final short COMMAND_CREDIT = 4;
    public static final short COMMAND_UNSUBSCRIBE = 5;
    public static final short COMMAND_PUBLISH_ERROR = 6;
    public static final short COMMAND_METADATA_UPDATE = 7;
    public static final short COMMAND_METADATA = 8;
    public static final short COMMAND_SASL_HANDSHAKE = 9;
    public static final short COMMAND_CREATE_TARGET = 998;
    public static final short COMMAND_DELETE_TARGET = 999;
    public static final short VERSION_0 = 0;

    private Constants() {

    }

}

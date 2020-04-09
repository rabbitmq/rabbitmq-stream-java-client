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

import java.util.UUID;

public interface MessageBuilder {

    Message build();

    PropertiesBuilder properties();

    ApplicationPropertiesBuilder applicationProperties();

    MessageBuilder addData(byte[] data);

    interface ApplicationPropertiesBuilder {

        ApplicationPropertiesBuilder entry(String key, byte value);

        ApplicationPropertiesBuilder entry(String key, short value);

        ApplicationPropertiesBuilder entry(String key, int value);

        ApplicationPropertiesBuilder entry(String key, long value);

        ApplicationPropertiesBuilder entryUnsigned(String key, byte value);

        ApplicationPropertiesBuilder entryUnsigned(String key, short value);

        ApplicationPropertiesBuilder entryUnsigned(String key, int value);

        ApplicationPropertiesBuilder entryUnsigned(String key, long value);

        ApplicationPropertiesBuilder entry(String key, float value);

        ApplicationPropertiesBuilder entry(String key, double value);

        MessageBuilder messageBuilder();

    }

    interface PropertiesBuilder {

        PropertiesBuilder messageId(String id);

        PropertiesBuilder messageId(long id);

        PropertiesBuilder messageId(byte[] id);

        PropertiesBuilder messageId(UUID id);

        PropertiesBuilder userId(byte[] userId);

        PropertiesBuilder to(String address);

        PropertiesBuilder subject(String subject);

        PropertiesBuilder replyTo(String replyTo);

        PropertiesBuilder correlationId(String correlationId);

        PropertiesBuilder correlationId(long correlationId);

        PropertiesBuilder correlationId(byte[] correlationId);

        PropertiesBuilder correlationId(UUID correlationId);

        PropertiesBuilder contentType(String contentType);

        PropertiesBuilder contentEncoding(String contentEncoding);

        PropertiesBuilder absoluteExpiryTime(long absoluteExpiryTime);

        PropertiesBuilder creationTime(long creationTime);

        PropertiesBuilder groupId(String groupId);

        PropertiesBuilder groupSequence(long groupSequence);

        PropertiesBuilder replyToGroupId(String replyToGroupId);

        MessageBuilder messageBuilder();

    }

}

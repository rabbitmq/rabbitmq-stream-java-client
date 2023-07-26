// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.observation.micrometer;

import static com.rabbitmq.stream.observation.micrometer.StreamObservationDocumentation.HighCardinalityTags.*;

import com.rabbitmq.stream.observation.micrometer.StreamObservationDocumentation.LowCardinalityTags;
import io.micrometer.common.KeyValues;

/**
 * Default {@link PublishObservationConvention}.
 *
 * @since 0.12.0
 */
public class DefaultPublishObservationConvention implements PublishObservationConvention {

  private static final String OPERATION = "publish";
  private static final String OPERATION_SUFFIX = " " + OPERATION;

  @Override
  public String getName() {
    return "rabbitmq.stream.publish";
  }

  @Override
  public String getContextualName(PublishContext context) {
    return context.stream() + OPERATION_SUFFIX;
  }

  @Override
  public KeyValues getLowCardinalityKeyValues(PublishContext context) {
    return KeyValues.of(
        LowCardinalityTags.MESSAGING_OPERATION.withValue(OPERATION),
        LowCardinalityTags.MESSAGING_SYSTEM.withValue("rabbitmq"),
        LowCardinalityTags.NET_PROTOCOL_NAME.withValue("rabbitmq-stream"),
        LowCardinalityTags.NET_PROTOCOL_VERSION.withValue("1.0"));
  }

  @Override
  public KeyValues getHighCardinalityKeyValues(PublishContext context) {
    return KeyValues.of(
        MESSAGING_DESTINATION_NAME.withValue(context.stream()),
        MESSAGING_SOURCE_NAME.withValue(context.stream()),
        MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES.withValue(
            String.valueOf(context.getPayloadSizeBytes())));
  }
}

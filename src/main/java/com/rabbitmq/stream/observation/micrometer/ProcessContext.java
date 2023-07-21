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

import com.rabbitmq.stream.Message;
import io.micrometer.observation.transport.ReceiverContext;
import java.util.Map;

public class ProcessContext extends ReceiverContext<Message> {

  private final String stream;
  private final int payloadSizeBytes;

  ProcessContext(String stream, Message message) {
    super(
        (carrier, key) -> {
          Map<String, Object> map = carrier.getMessageAnnotations();
          Object result = map == null ? null : map.get(key);
          if (result == null) {
            map = carrier.getApplicationProperties();
            result = map == null ? null : map.get(key);
          }
          if (result == null) {
            return null;
          }
          return String.valueOf(result);
        });
    this.stream = stream;
    int payloadSize;
    try {
      byte[] body = message.getBodyAsBinary();
      payloadSize = body == null ? 0 : body.length;
    } catch (Exception e) {
      payloadSize = 0;
    }
    this.payloadSizeBytes = payloadSize;
    setCarrier(message);
  }

  public String getStream() {
    return stream;
  }

  public int getPayloadSizeBytes() {
    return payloadSizeBytes;
  }
}

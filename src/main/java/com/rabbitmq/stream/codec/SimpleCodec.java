// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.codec;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Properties;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleCodec implements Codec {

  @Override
  public EncodedMessage encode(Message message) {
    return new EncodedMessage(message.getBodyAsBinary().length, message.getBodyAsBinary());
  }

  @Override
  public Message decode(byte[] data) {
    return new SimpleMessage(false, 0, data);
  }

  @Override
  public MessageBuilder messageBuilder() {
    return new SimpleMessageBuilder();
  }

  private static class SimpleMessage implements Message {

    private final boolean hasPublishingId;
    private final long publishingId;

    private final byte[] body;

    private SimpleMessage(boolean hasPublishingId, long publishingId, byte[] body) {
      this.hasPublishingId = hasPublishingId;
      this.publishingId = publishingId;
      this.body = body;
    }

    @Override
    public boolean hasPublishingId() {
      return hasPublishingId;
    }

    @Override
    public long getPublishingId() {
      return publishingId;
    }

    @Override
    public byte[] getBodyAsBinary() {
      return body;
    }

    @Override
    public Object getBody() {
      return body;
    }

    @Override
    public Properties getProperties() {
      return null;
    }

    @Override
    public Map<String, Object> getApplicationProperties() {
      return null;
    }

    @Override
    public Map<String, Object> getMessageAnnotations() {
      return null;
    }
  }

  private static class SimpleMessageBuilder implements MessageBuilder {
    private boolean hasPublishingId = false;

    private long publishingId = 0;
    private byte[] body;
    private final AtomicBoolean built = new AtomicBoolean(false);

    @Override
    public Message build() {
      if (built.compareAndSet(false, true)) {
        return new SimpleMessage(hasPublishingId, publishingId, body);
      } else {
        throw new IllegalStateException("A message builder can build only one message");
      }
    }

    @Override
    public MessageBuilder publishingId(long publishingId) {
      this.publishingId = publishingId;
      this.hasPublishingId = true;
      return this;
    }

    @Override
    public PropertiesBuilder properties() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationPropertiesBuilder applicationProperties() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageAnnotationsBuilder messageAnnotations() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageBuilder addData(byte[] data) {
      this.body = data;
      return this;
    }
  }
}

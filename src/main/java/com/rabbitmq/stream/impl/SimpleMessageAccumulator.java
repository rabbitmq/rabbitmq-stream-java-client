// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.StreamException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class SimpleMessageAccumulator implements MessageAccumulator {

  protected final BlockingQueue<AccumulatedEntity> messages;
  private final int capacity;
  private final Codec codec;
  private final int maxFrameSize;

  SimpleMessageAccumulator(int capacity, Codec codec, int maxFrameSize) {
    this.capacity = capacity;
    this.messages = new LinkedBlockingQueue<>(capacity);
    this.codec = codec;
    this.maxFrameSize = maxFrameSize;
  }

  public boolean add(Message message, ConfirmationHandler confirmationHandler) {
    Codec.EncodedMessage encodedMessage = this.codec.encode(message);
    Client.checkMessageFitsInFrame(this.maxFrameSize, encodedMessage);

    try {
      boolean offered =
          messages.offer(
              new SimpleAccumulatedEntity(
                  encodedMessage, new SimpleConfirmationCallback(message, confirmationHandler)),
              60,
              TimeUnit.SECONDS);
      if (!offered) {
        throw new StreamException("Could not accumulate outbound message");
      }
    } catch (InterruptedException e) {
      throw new StreamException("Error while accumulating outbound message", e);
    }
    return this.messages.size() == this.capacity;
  }

  @Override
  public AccumulatedEntity get() {
    return this.messages.poll();
  }

  @Override
  public boolean isEmpty() {
    return messages.isEmpty();
  }

  @Override
  public int size() {
    return messages.size();
  }

  private static final class SimpleAccumulatedEntity implements AccumulatedEntity {

    private final Codec.EncodedMessage encodedMessage;
    private final StreamProducer.ConfirmationCallback confirmationCallback;

    private SimpleAccumulatedEntity(
        Codec.EncodedMessage encodedMessage,
        StreamProducer.ConfirmationCallback confirmationCallback) {
      this.encodedMessage = encodedMessage;
      this.confirmationCallback = confirmationCallback;
    }

    @Override
    public Object encodedEntity() {
      return encodedMessage;
    }

    @Override
    public StreamProducer.ConfirmationCallback confirmationCallback() {
      return confirmationCallback;
    }
  }

  private static final class SimpleConfirmationCallback
      implements StreamProducer.ConfirmationCallback {

    private final Message message;
    private final ConfirmationHandler confirmationHandler;

    private SimpleConfirmationCallback(Message message, ConfirmationHandler confirmationHandler) {
      this.message = message;
      this.confirmationHandler = confirmationHandler;
    }

    @Override
    public int handle(boolean confirmed, short code) {
      confirmationHandler.handle(new ConfirmationStatus(message, confirmed, code));
      return 1;
    }
  }
}

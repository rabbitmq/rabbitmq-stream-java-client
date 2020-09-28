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

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.CommitConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironment.CommittingConsumerRegistration;
import java.util.concurrent.atomic.AtomicBoolean;

class StreamConsumer implements Consumer {

  private final Runnable closingCallback;

  private final Runnable closingCommitCallback;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final String name;

  private final String stream;

  private final StreamEnvironment environment;

  private volatile Client commitClient;

  private volatile Status status;

  StreamConsumer(
      String stream,
      OffsetSpecification offsetSpecification,
      MessageHandler messageHandler,
      String name,
      StreamEnvironment environment,
      CommitConfiguration commitConfiguration) {

    try {
      this.name = name;
      this.stream = stream;
      this.environment = environment;

      MessageHandler messageHandlerWithOrWithoutCommit;
      if (commitConfiguration.enabled()) {
        CommittingConsumerRegistration committingConsumerRegistration =
            environment.registerCommittingConsumer(this, commitConfiguration);

        this.closingCommitCallback = committingConsumerRegistration.closingCallback();

        java.util.function.Consumer<Context> postMessageProcessingCallback =
            committingConsumerRegistration.postMessageProcessingCallback();
        if (postMessageProcessingCallback == null) {
          // no callback, no need to decorate
          messageHandlerWithOrWithoutCommit = messageHandler;
        } else {
          messageHandlerWithOrWithoutCommit =
              (context, message) -> {
                messageHandler.handle(context, message);
                postMessageProcessingCallback.accept(context);
              };
        }

      } else {
        this.closingCommitCallback = () -> {};
        messageHandlerWithOrWithoutCommit = messageHandler;
      }

      this.closingCallback =
          environment.registerConsumer(
              this, stream, offsetSpecification, this.name, messageHandlerWithOrWithoutCommit);

      this.status = Status.RUNNING;
    } catch (RuntimeException e) {
      this.closed.set(true);
      throw e;
    }
  }

  @Override
  public void commit(long offset) {
    // FIXME appropriate behavior if commit is not possible
    if (canCommit()) {
      this.commitClient.commitOffset(this.name, this.stream, offset);
    }
  }

  private boolean canCommit() {
    return this.status == Status.RUNNING;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      this.environment.removeConsumer(this);
      closeFromEnvironment();
    }
  }

  void closeFromEnvironment() {
    this.closingCallback.run();
    this.closingCommitCallback.run();
    closed.set(true);
    this.status = Status.CLOSED;
  }

  void closeAfterStreamDeletion() {
    if (closed.compareAndSet(false, true)) {
      this.environment.removeConsumer(this);
      this.status = Status.CLOSED;
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  synchronized void setClient(Client client) {
    this.commitClient = client;
  }

  void unavailable() {
    this.status = Status.NOT_AVAILABLE;
  }

  void running() {
    this.status = Status.RUNNING;
  }

  private Status status() {
    return this.status;
  }

  String name() {
    return this.name;
  }

  String stream() {
    return this.stream;
  }

  enum Status {
    RUNNING,
    NOT_AVAILABLE,
    CLOSED
  }
}

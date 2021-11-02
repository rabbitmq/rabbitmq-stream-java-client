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
package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironment.TrackingConsumerRegistration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamConsumer implements Consumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumer.class);

  private volatile Runnable closingCallback;

  private final Runnable closingTrackingCallback;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final String name;

  private final String stream;

  private final StreamEnvironment environment;

  private volatile Client trackingClient;

  private volatile Status status;

  private volatile long lastRequestedStoredOffset = 0;

  private final LongConsumer trackingCallback;

  private final Runnable initCallback;

  StreamConsumer(
      String stream,
      OffsetSpecification offsetSpecification,
      MessageHandler messageHandler,
      String name,
      StreamEnvironment environment,
      TrackingConfiguration trackingConfiguration,
      boolean lazyInit,
      SubscriptionListener subscriptionListener) {

    try {
      this.name = name;
      this.stream = stream;
      this.environment = environment;

      MessageHandler messageHandlerWithOrWithoutTracking;
      if (trackingConfiguration.enabled()) {
        TrackingConsumerRegistration trackingConsumerRegistration =
            environment.registerTrackingConsumer(this, trackingConfiguration);

        this.closingTrackingCallback = trackingConsumerRegistration.closingCallback();

        java.util.function.Consumer<Context> postMessageProcessingCallback =
            trackingConsumerRegistration.postMessageProcessingCallback();
        if (postMessageProcessingCallback == null) {
          // no callback, no need to decorate
          messageHandlerWithOrWithoutTracking = messageHandler;
        } else {
          messageHandlerWithOrWithoutTracking =
              (context, message) -> {
                messageHandler.handle(context, message);
                postMessageProcessingCallback.accept(context);
              };
        }

        this.trackingCallback = trackingConsumerRegistration.trackingCallback();

      } else {
        this.closingTrackingCallback = () -> {};
        this.trackingCallback = Utils.NO_OP_LONG_CONSUMER;
        messageHandlerWithOrWithoutTracking = messageHandler;
      }

      Runnable init =
          () -> {
            this.closingCallback =
                environment.registerConsumer(
                    this,
                    stream,
                    offsetSpecification,
                    this.name,
                    subscriptionListener,
                    messageHandlerWithOrWithoutTracking);

            this.status = Status.RUNNING;
          };
      if (lazyInit) {
        this.initCallback = init;
      } else {
        this.initCallback = () -> {};
        init.run();
      }
    } catch (RuntimeException e) {
      this.closed.set(true);
      throw e;
    }
  }

  void start() {
    try {
      this.initCallback.run();
    } catch (RuntimeException e) {
      this.closed.set(true);
      throw e;
    }
  }

  @Override
  public void store(long offset) {
    trackingCallback.accept(offset);
    if (canTrack()) {
      if (Long.compareUnsigned(this.lastRequestedStoredOffset, offset) < 0) {
        try {
          this.trackingClient.storeOffset(this.name, this.stream, offset);
          this.lastRequestedStoredOffset = offset;
        } catch (Exception e) {
          LOGGER.debug("Error while trying to store offset: {}", e.getMessage());
        }
      }
    }
    // nothing special to do if tracking is not possible or errors, e.g. because of a network
    // failure
    // the tracking strategy will stack the storage request and apply it as soon as it can
  }

  private boolean canTrack() {
    return this.status == Status.RUNNING && this.name != null;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      this.environment.removeConsumer(this);
      closeFromEnvironment();
      LOGGER.debug("Closed consumer successfully");
    }
  }

  void closeFromEnvironment() {
    LOGGER.debug("Calling consumer closing callback");
    this.closingCallback.run();
    LOGGER.debug("Calling tracking consumer closing callback (may be no-op)");
    this.closingTrackingCallback.run();
    closed.set(true);
    this.status = Status.CLOSED;
    LOGGER.debug("Closed consumer successfully");
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
    this.trackingClient = client;
  }

  synchronized void unavailable() {
    this.status = Status.NOT_AVAILABLE;
    this.trackingClient = null;
  }

  void running() {
    this.status = Status.RUNNING;
  }

  long lastStoredOffset() {
    if (canTrack()) {
      try {
        // the client can be null by now, but we catch the exception and return 0
        // callers should know how to deal with a stored offset of 0
        return this.trackingClient.queryOffset(this.name, this.stream);
      } catch (Exception e) {
        return 0;
      }
    } else {
      return 0;
    }
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

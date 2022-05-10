// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.Utils.offsetBefore;
import static com.rabbitmq.stream.impl.Utils.isSac;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerUpdateListener;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironment.TrackingConsumerRegistration;
import com.rabbitmq.stream.impl.Utils.CompositeConsumerUpdateListener;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamConsumer implements Consumer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumer.class);
  private final long id;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final String name;
  private final String stream;
  private final StreamEnvironment environment;
  private final LongConsumer trackingCallback;
  private final Runnable initCallback;
  private final ConsumerUpdateListener consumerUpdateListener;
  private volatile Runnable closingCallback;
  private volatile Client trackingClient;
  private volatile Client subscriptionClient;
  private volatile Status status;
  private volatile long lastRequestedStoredOffset = 0;
  private final AtomicBoolean nothingStoredYet = new AtomicBoolean(true);
  private volatile boolean sacActive;
  private final boolean sac;
  private final OffsetSpecification initialOffsetSpecification;

  StreamConsumer(
      String stream,
      OffsetSpecification offsetSpecification,
      MessageHandler messageHandler,
      String name,
      StreamEnvironment environment,
      TrackingConfiguration trackingConfiguration,
      boolean lazyInit,
      SubscriptionListener subscriptionListener,
      Map<String, String> subscriptionProperties,
      ConsumerUpdateListener consumerUpdateListener) {

    this.id = ID_SEQUENCE.getAndIncrement();
    Runnable trackingClosingCallback;
    try {
      this.name = name;
      this.stream = stream;
      this.environment = environment;
      this.initialOffsetSpecification =
          offsetSpecification == null
              ? ConsumersCoordinator.DEFAULT_OFFSET_SPECIFICATION
              : offsetSpecification;

      AtomicReference<MessageHandler> decoratedMessageHandler = new AtomicReference<>();
      LongSupplier trackingFlushCallback;
      if (trackingConfiguration.enabled()) {
        TrackingConsumerRegistration trackingConsumerRegistration =
            environment.registerTrackingConsumer(this, trackingConfiguration);

        trackingClosingCallback = trackingConsumerRegistration.closingCallback();

        java.util.function.Consumer<Context> postMessageProcessingCallback =
            trackingConsumerRegistration.postMessageProcessingCallback();
        if (postMessageProcessingCallback == null) {
          // no callback, no need to decorate
          decoratedMessageHandler.set(messageHandler);
        } else {
          decoratedMessageHandler.set(
              (context, message) -> {
                messageHandler.handle(context, message);
                postMessageProcessingCallback.accept(context);
              });
        }

        this.trackingCallback = trackingConsumerRegistration.trackingCallback();
        trackingFlushCallback = trackingConsumerRegistration::flush;
      } else {
        trackingClosingCallback = () -> {};
        this.trackingCallback = Utils.NO_OP_LONG_CONSUMER;
        trackingFlushCallback = Utils.NO_OP_LONG_SUPPLIER;
        decoratedMessageHandler.set(messageHandler);
      }

      this.sacActive = false;
      if (Utils.isSac(subscriptionProperties)) {
        this.sac = true;
        MessageHandler existingMessageHandler = decoratedMessageHandler.get();
        AtomicBoolean receivedSomething = new AtomicBoolean(false);
        MessageHandler messageHandlerWithSac;
        if (trackingConfiguration.auto()) {
          messageHandlerWithSac =
              (context, message) -> {
                if (this.sacActive) {
                  receivedSomething.set(true);
                  existingMessageHandler.handle(context, message);
                }
              };
        } else {
          messageHandlerWithSac =
              (context, message) -> {
                if (this.sacActive) {
                  existingMessageHandler.handle(context, message);
                }
              };
        }

        decoratedMessageHandler.set(messageHandlerWithSac);

        if (consumerUpdateListener == null
            || consumerUpdateListener instanceof CompositeConsumerUpdateListener) {
          if (trackingConfiguration.auto()) {
            LOGGER.debug("Setting default consumer update listener for auto tracking strategy");
            ConsumerUpdateListener defaultListener =
                context -> {
                  OffsetSpecification result = null;
                  if (context.isActive()) {
                    LOGGER.debug("Looking up offset (stream {})", this.stream);
                    Consumer consumer = context.consumer();
                    try {
                      long offset = consumer.storedOffset();
                      LOGGER.debug(
                          "Stored offset is {}, returning the value + 1 to the server", offset);
                      result = OffsetSpecification.offset(offset + 1);
                    } catch (NoOffsetException e) {
                      LOGGER.debug(
                          "No stored offset, using initial offset specification: {}",
                          this.initialOffsetSpecification);
                      result = initialOffsetSpecification;
                    }
                    return result;
                  } else {
                    if (receivedSomething.get()) {
                      LOGGER.debug(
                          "Storing offset (consumer {}, stream {}) because going from active to passive",
                          this.id,
                          this.stream);
                      long offset = trackingFlushCallback.getAsLong();
                      LOGGER.debug(
                          "Making sure offset {} has been stored (consumer {}, stream {})",
                          offset,
                          this.id,
                          this.stream);
                      waitForOffsetToBeStored(offset);
                    }
                    result = OffsetSpecification.none();
                  }
                  return result;
                };
            // just a trick for testing
            if (consumerUpdateListener instanceof CompositeConsumerUpdateListener) {
              ((CompositeConsumerUpdateListener) consumerUpdateListener).add(defaultListener);
              this.consumerUpdateListener = consumerUpdateListener;
            } else {
              this.consumerUpdateListener = defaultListener;
            }
          } else if (trackingConfiguration.manual()) {
            LOGGER.debug("Setting default consumer update listener for manual tracking strategy");
            ConsumerUpdateListener defaultListener =
                context -> {
                  OffsetSpecification result = null;
                  // we are not supposed to store offsets with manual tracking strategy
                  // so we just look up the last stored offset when we go from passive to active
                  if (context.isActive()) {
                    LOGGER.debug("Going from passive to active, looking up offset");
                    Consumer consumer = context.consumer();
                    try {
                      long offset = consumer.storedOffset();
                      LOGGER.debug(
                          "Stored offset is {}, returning the value + 1 to the server", offset);
                      result = OffsetSpecification.offset(offset + 1);
                    } catch (NoOffsetException e) {
                      LOGGER.debug(
                          "No stored offset, using initial offset specification: {}",
                          this.initialOffsetSpecification);
                      result = initialOffsetSpecification;
                    }
                  }
                  return result;
                };
            this.consumerUpdateListener = defaultListener;
          } else {
            // no consumer update listener to look up the offset, this is not what we want
            this.consumerUpdateListener = context -> null;
          }
        } else {
          this.consumerUpdateListener = consumerUpdateListener;
        }

      } else {
        this.consumerUpdateListener = null;
        this.sac = false;
      }

      MessageHandler computedMessageHandler = decoratedMessageHandler.get();
      MessageHandler closedAwareMessageHandler =
          (context, message) -> {
            if (!closed.get()) {
              computedMessageHandler.handle(context, message);
            }
          };

      Runnable init =
          () -> {
            this.status = Status.INITIALIZING;
            this.closingCallback =
                environment.registerConsumer(
                    this,
                    stream,
                    offsetSpecification,
                    this.name,
                    subscriptionListener,
                    trackingClosingCallback,
                    closedAwareMessageHandler,
                    Collections.unmodifiableMap(subscriptionProperties));

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

  void waitForOffsetToBeStored(long expectedStoredOffset) {
    CompletableFuture<Boolean> storedTask =
        AsyncRetry.asyncRetry(
                () -> {
                  try {
                    long lastStoredOffset = storedOffset();
                    boolean stored = lastStoredOffset == expectedStoredOffset;
                    LOGGER.debug(
                        "Last stored offset from consumer {} on {} is {}, expecting {}",
                        this.id,
                        this.stream,
                        lastStoredOffset,
                        expectedStoredOffset);
                    if (!stored) {
                      throw new IllegalStateException();
                    } else {
                      return true;
                    }
                  } catch (StreamException e) {
                    if (e.getCode() == Constants.RESPONSE_CODE_NO_OFFSET) {
                      throw new IllegalStateException();
                    } else {
                      throw e;
                    }
                  }
                })
            .description(
                "Last stored offset for consumer %s on stream %s must be %d",
                this.name, this.stream, expectedStoredOffset)
            .delayPolicy(
                BackOffDelayPolicy.fixedWithInitialDelay(
                    Duration.ofMillis(200), Duration.ofMillis(200)))
            .retry(exception -> exception instanceof IllegalStateException)
            .scheduler(environment.scheduledExecutorService())
            .build();

    try {
      storedTask.get(10, TimeUnit.SECONDS);
      LOGGER.debug(
          "Offset {} stored (consumer {}, stream {})", expectedStoredOffset, this.id, this.stream);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException e) {
      LOGGER.warn("Error while checking offset has been stored", e);
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
      if (offsetBefore(this.lastRequestedStoredOffset, offset)
          || nothingStoredYet.compareAndSet(true, false)) {
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

  OffsetSpecification consumerUpdate(boolean active) {
    LOGGER.debug(
        "Consumer {} from stream {} with name {} received consumer update notification, active = {}",
        this.id,
        this.stream,
        this.name,
        active);

    if (this.sacActive == active) {
      LOGGER.warn(
          "Previous and new status are the same ({}), there should be no consumer update in this case.",
          active);
    }

    this.sacActive = active;

    ConsumerUpdateListener.Context context = new DefaultConsumerUpdateContext(this, active);

    LOGGER.debug("Calling consumer update listener");
    OffsetSpecification result = null;
    try {
      result = this.consumerUpdateListener.update(context);
      LOGGER.debug("Consumer update listener returned {}", result);
    } catch (Exception e) {
      LOGGER.warn("Error in consumer update listener", e);
    }

    // TODO pause/unpause offset tracking strategy
    // they are not supposed to do anything if nothing changes, so no doing anything should be fine
    // pausing/unpausing them would save some resources, but makes the code more complicated

    return result;
  }

  private static class DefaultConsumerUpdateContext implements ConsumerUpdateListener.Context {

    private final StreamConsumer consumer;
    private final boolean active;

    private DefaultConsumerUpdateContext(StreamConsumer consumer, boolean active) {
      this.consumer = consumer;
      this.active = active;
    }

    @Override
    public Consumer consumer() {
      return this.consumer;
    }

    @Override
    public String stream() {
      return this.consumer.stream;
    }

    @Override
    public boolean isActive() {
      return this.active;
    }

    @Override
    public String toString() {
      return "DefaultConsumerUpdateContext{" + "consumer=" + consumer + ", active=" + active + '}';
    }
  }

  boolean isSac() {
    return this.sac;
  }

  boolean sacActive() {
    return this.sacActive;
  }

  private boolean canTrack() {
    return (this.status == Status.INITIALIZING || this.status == Status.RUNNING)
        && this.name != null;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      this.environment.removeConsumer(this);
      closeFromEnvironment();
    }
  }

  void closeFromEnvironment() {
    LOGGER.debug("Calling consumer {} closing callback (stream {})", this.id, this.stream);
    this.closingCallback.run();
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

  synchronized void setTrackingClient(Client client) {
    this.trackingClient = client;
  }

  void setSubscriptionClient(Client client) {
    this.subscriptionClient = client;
    if (client == null && this.isSac()) {
      // we lost the connection
      this.sacActive = false;
    }
  }

  synchronized void unavailable() {
    this.status = Status.NOT_AVAILABLE;
    this.trackingClient = null;
  }

  void running() {
    this.status = Status.RUNNING;
  }

  @Override
  public long storedOffset() {
    if (canTrack()) {
      // the client can be null by now, so we catch any exception
      QueryOffsetResponse response;
      try {
        response = this.trackingClient.queryOffset(this.name, this.stream);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "Not possible to query offset for consumer %s on stream %s for now",
                this.name, this.stream),
            e);
      }
      if (response.isOk()) {
        return response.getOffset();
      } else if (response.getResponseCode() == Constants.RESPONSE_CODE_NO_OFFSET) {
        throw new NoOffsetException(
            String.format(
                "No offset stored for consumer %s on stream %s (%s)",
                this.name, this.stream, Utils.formatConstant(response.getResponseCode())));
      } else {
        throw new StreamException(
            String.format(
                "QueryOffset for consumer %s on stream %s returned an error (%s)",
                this.name, this.stream, Utils.formatConstant(response.getResponseCode())),
            response.getResponseCode());
      }

    } else if (this.name == null) {
      throw new UnsupportedOperationException(
          "Not possible to query stored offset for a consumer without a name");
    } else {
      throw new IllegalStateException(
          String.format(
              "Not possible to query offset for consumer %s on stream %s for now",
              this.name, this.stream));
    }
  }

  String stream() {
    return this.stream;
  }

  enum Status {
    INITIALIZING,
    RUNNING,
    NOT_AVAILABLE,
    CLOSED
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamConsumer that = (StreamConsumer) o;
    return id == that.id && stream.equals(that.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, stream);
  }

  @Override
  public String toString() {
    Client subscriptionClient = this.subscriptionClient;
    Client trackingClient = this.trackingClient;
    return "{ "
        + "\"id\" : "
        + id
        + ","
        + "\"stream\" : \""
        + stream
        + "\","
        + "\"subscription_client\" : "
        + (subscriptionClient == null
            ? "null"
            : ("\"" + subscriptionClient.connectionName() + "\""))
        + ", "
        + "\"tracking_client\" : "
        + (trackingClient == null ? "null" : ("\"" + trackingClient.connectionName() + "\""))
        + "}";
  }
}

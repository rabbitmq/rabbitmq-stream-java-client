// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import static com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay;
import static com.rabbitmq.stream.impl.AsyncRetry.asyncRetry;
import static com.rabbitmq.stream.impl.Utils.offsetBefore;
import static java.lang.String.format;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironment.LocatorNotAvailableException;
import com.rabbitmq.stream.impl.StreamEnvironment.TrackingConsumerRegistration;
import com.rabbitmq.stream.impl.Utils.CompositeConsumerUpdateListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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
  private final Lock lock = new ReentrantLock();

  @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
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
      ConsumerUpdateListener consumerUpdateListener,
      ConsumerFlowStrategy flowStrategy) {
    if (Utils.filteringEnabled(subscriptionProperties) && !environment.filteringSupported()) {
      throw new IllegalArgumentException(
          "Filtering is not supported by the broker "
              + "(requires RabbitMQ 3.13+ and stream_filtering feature flag activated");
    }
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
                    StreamConsumer consumer = (StreamConsumer) context.consumer();
                    return getStoredOffset(consumer, environment, initialOffsetSpecification);
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
            // we know the update listener is either null or a composite one
            if (consumerUpdateListener != null) {
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
                    StreamConsumer consumer = (StreamConsumer) context.consumer();
                    result = getStoredOffset(consumer, environment, initialOffsetSpecification);
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
                    Map.copyOf(subscriptionProperties),
                    flowStrategy);

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

  static OffsetSpecification getStoredOffset(
      StreamConsumer consumer, StreamEnvironment environment, OffsetSpecification fallback) {
    OffsetSpecification result = null;
    while (result == null) {
      try {
        long offset = getStoredOffsetSafely(consumer, environment);
        LOGGER.debug("Stored offset is {}, returning the value + 1 to the server", offset);
        result = OffsetSpecification.offset(offset + 1);
      } catch (NoOffsetException e) {
        LOGGER.debug("No stored offset, using initial offset specification: {}", fallback);
        result = fallback;
      } catch (TimeoutStreamException e) {
        LOGGER.debug("Timeout when looking up stored offset, retrying");
      }
    }
    return result;
  }

  static long getStoredOffsetSafely(StreamConsumer consumer, StreamEnvironment environment) {
    long offset;
    try {
      offset = consumer.storedOffset();
    } catch (IllegalStateException e) {
      LOGGER.debug(
          "Leader connection for '{}' not available to retrieve offset, retrying", consumer.stream);
      // no connection to leader to retrieve the offset, trying with environment connections
      String description =
          format("Stored offset retrieval for '%s' on stream '%s'", consumer.name, consumer.stream);
      CompletableFuture<Long> storedOffetRetrievalFuture =
          asyncRetry(() -> consumer.storedOffset(() -> environment.locator().client()))
              .description(description)
              .scheduler(environment.scheduledExecutorService())
              .retry(
                  ex ->
                      ex instanceof IllegalStateException
                          || ex instanceof LocatorNotAvailableException)
              .delayPolicy(
                  fixedWithInitialDelay(
                      Duration.ZERO,
                      environment.recoveryBackOffDelayPolicy().delay(1),
                      environment.recoveryBackOffDelayPolicy().delay(0).multipliedBy(3)))
              .build();
      try {
        offset =
            storedOffetRetrievalFuture.get(
                environment.rpcTimeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new StreamException(
            format(
                "Could not get stored offset for '%s' on stream '%s'",
                consumer.name, consumer.stream),
            ex);
      } catch (ExecutionException ex) {
        if (ex.getCause() instanceof StreamException) {
          throw (StreamException) ex.getCause();
        } else {
          throw new StreamException(
              format(
                  "Could not get stored offset for '%s' on stream '%s'",
                  consumer.name, consumer.stream),
              ex);
        }
      } catch (TimeoutException ex) {
        storedOffetRetrievalFuture.cancel(true);
        throw new TimeoutStreamException(
            format(
                "Could not get stored offset for '%s' on stream '%s' (timeout)",
                consumer.name, consumer.stream),
            ex);
      }
    }
    return offset;
  }

  void waitForOffsetToBeStored(long expectedStoredOffset) {
    OffsetTrackingUtils.waitForOffsetToBeStored(
        "consumer " + this.id,
        this.environment.scheduledExecutorService(),
        this::storedOffset,
        this.name,
        this.stream,
        expectedStoredOffset);
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
    checkNotClosed();
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

  boolean canTrack() {
    return ((this.status == Status.INITIALIZING || this.status == Status.RUNNING)
            || (this.trackingClient == null && this.status == Status.NOT_AVAILABLE))
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
    this.maybeNotifyActiveToInactiveSac();
    LOGGER.debug("Calling consumer {} closing callback (stream {})", this.id, this.stream);
    this.closingCallback.run();
    closed.set(true);
    this.status = Status.CLOSED;
    LOGGER.debug("Closed consumer successfully");
  }

  void closeAfterStreamDeletion() {
    if (closed.compareAndSet(false, true)) {
      this.maybeNotifyActiveToInactiveSac();
      this.environment.removeConsumer(this);
      this.status = Status.CLOSED;
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void setTrackingClient(Client client) {
    this.trackingClient = client;
  }

  void setSubscriptionClient(Client client) {
    this.subscriptionClient = client;
    if (client == null && this.isSac()) {
      maybeNotifyActiveToInactiveSac();
      // we lost the connection
      this.sacActive = false;
    }
  }

  private void maybeNotifyActiveToInactiveSac() {
    if (this.isSac() && this.sacActive) {
      LOGGER.debug(
          "Single active consumer {} from stream {} with name {} is unavailable, calling consumer update listener",
          this.id,
          this.stream,
          this.name);
      this.consumerUpdate(false);
    }
  }

  synchronized void unavailable() {
    Utils.lock(
        this.lock,
        () -> {
          this.status = Status.NOT_AVAILABLE;
          this.trackingClient = null;
        });
  }

  void lock() {
    this.lock.lock();
  }

  void unlock() {
    this.lock.unlock();
  }

  void running() {
    this.status = Status.RUNNING;
  }

  long storedOffset(Supplier<Client> clientSupplier) {
    checkNotClosed();
    if (canTrack()) {
      return OffsetTrackingUtils.storedOffset(clientSupplier, this.name, this.stream);
    } else if (this.name == null) {
      throw new UnsupportedOperationException(
          "Not possible to query stored offset for a consumer without a name");
    } else {
      throw new IllegalStateException(
          format(
              "Not possible to query offset for consumer %s on stream %s for now, consumer status is %s",
              this.name, this.stream, this.status.name()));
    }
  }

  @Override
  public long storedOffset() {
    return storedOffset(() -> this.trackingClient);
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

  private void checkNotClosed() {
    if (this.status == Status.CLOSED) {
      throw new IllegalStateException("This producer instance has been closed");
    }
  }

  long id() {
    return this.id;
  }

  String subscriptionConnectionName() {
    Client client = this.subscriptionClient;
    if (client == null) {
      return "<no-connection>";
    } else {
      return client.clientConnectionName();
    }
  }
}

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

import static com.rabbitmq.stream.impl.Utils.namedRunnable;
import static com.rabbitmq.stream.impl.Utils.offsetBefore;

import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OffsetTrackingCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetTrackingCoordinator.class);

  private final StreamEnvironment streamEnvironment;

  private final AtomicBoolean started = new AtomicBoolean(false);

  private final Collection<Tracker> trackers = ConcurrentHashMap.newKeySet();

  private final LocalClock clock = new LocalClock();

  private final AtomicBoolean flushingOnGoing = new AtomicBoolean(false);

  private final Duration checkInterval;

  private volatile Future<?> checkFuture;

  OffsetTrackingCoordinator(StreamEnvironment streamEnvironment) {
    this(streamEnvironment, Duration.ofSeconds(1));
  }

  OffsetTrackingCoordinator(StreamEnvironment streamEnvironment, Duration checkInterval) {
    this.streamEnvironment = streamEnvironment;
    this.checkInterval = checkInterval;
  }

  Registration registerTrackingConsumer(
      StreamConsumer consumer, TrackingConfiguration configuration) {

    if (!configuration.enabled()) {
      throw new IllegalArgumentException("Tracking must be enabled");
    }

    Tracker tracker;
    if (configuration.auto()) {
      tracker = new AutoTrackingTracker(consumer, configuration, clock);
    } else {
      if (configuration.manualCheckInterval().isZero()) {
        throw new IllegalArgumentException(
            "There should be no registration if the check interval is 0");
      }
      tracker = new ManualTrackingTracker(consumer, configuration, clock);
    }
    trackers.add(tracker);

    if (started.compareAndSet(false, true)) {
      this.clock.setTime(System.nanoTime());
      this.checkFuture =
          this.executor()
              .scheduleAtFixedRate(
                  namedRunnable(
                      () -> {
                        if (flushingOnGoing.compareAndSet(false, true)) {
                          try {
                            this.clock.setTime(System.nanoTime());
                            LOGGER.debug(
                                "Background offset tracking flushing, {} tracker(s) to check",
                                this.trackers.size());
                            Iterator<Tracker> iterator = trackers.iterator();
                            while (iterator.hasNext()) {
                              if (Thread.currentThread().isInterrupted()) {
                                Thread.currentThread().interrupt();
                                break;
                              }
                              Tracker t = iterator.next();
                              if (t.consumer().isOpen()) {
                                try {
                                  t.flushIfNecessary();
                                } catch (Exception e) {
                                  LOGGER.info("Error while flushing tracker: {}", e.getMessage());
                                }
                              } else {
                                iterator.remove();
                              }
                            }
                          } finally {
                            flushingOnGoing.set(false);
                          }

                          // TODO consider cancelling the task if there are no more consumers to
                          // track
                          // it should then be restarted on demand.

                        }
                      },
                      "Offset tracking background task"),
                  this.checkInterval.toMillis(),
                  this.checkInterval.toMillis(),
                  TimeUnit.MILLISECONDS);
    }

    return new Registration(tracker);
  }

  private ScheduledExecutorService executor() {
    return this.streamEnvironment.scheduledExecutorService();
  }

  public boolean needTrackingRegistration(TrackingConfiguration trackingConfiguration) {
    return trackingConfiguration.enabled()
        && !(trackingConfiguration.manual()
            && Duration.ZERO.equals(trackingConfiguration.manualCheckInterval()));
  }

  void close() {
    if (this.checkFuture != null) {
      checkFuture.cancel(true);
    }
  }

  private interface Tracker {

    Consumer<Context> postProcessingCallback();

    void flushIfNecessary();

    long flush();

    StreamConsumer consumer();

    LongConsumer trackingCallback();

    Runnable closingCallback();
  }

  static class Registration {

    private final Tracker tracker;

    Registration(Tracker tracker) {
      this.tracker = tracker;
    }

    Consumer<Context> postMessageProcessingCallback() {
      return this.tracker.postProcessingCallback();
    }

    LongConsumer trackingCallback() {
      return this.tracker.trackingCallback();
    }

    Runnable closingCallback() {
      return this.tracker.closingCallback();
    }

    long flush() {
      return this.tracker.flush();
    }
  }

  private static final class AutoTrackingTracker implements Tracker {

    private final StreamConsumer consumer;
    private final int messageCountBeforeStorage;
    private final long flushIntervalInNs;
    private final LocalClock clock;
    private volatile long count = 0;
    private volatile AtomicLong lastProcessedOffset = null;
    private volatile long lastTrackingActivity = 0;

    private AutoTrackingTracker(
        StreamConsumer consumer, TrackingConfiguration configuration, LocalClock clock) {
      this.consumer = consumer;
      this.messageCountBeforeStorage = configuration.autoMessageCountBeforeStorage();
      this.flushIntervalInNs = configuration.autoFlushInterval().toNanos();
      this.clock = clock;
    }

    @Override
    public Consumer<Context> postProcessingCallback() {
      return context -> {
        if (++count % messageCountBeforeStorage == 0) {
          context.storeOffset();
          lastTrackingActivity = clock.time();
        }
        if (lastProcessedOffset == null) {
          lastProcessedOffset = new AtomicLong(context.offset());
        } else {
          lastProcessedOffset.set(context.offset());
        }
      };
    }

    @Override
    public void flushIfNecessary() {
      if (this.count > 0) {
        if (this.clock.time() - this.lastTrackingActivity > this.flushIntervalInNs) {
          this.flush();
        }
      }
    }

    @Override
    public long flush() {
      if (lastProcessedOffset == null) {
        return 0;
      } else {
        long result;
        try {
          long lastStoredOffset = consumer.storedOffset();
          if (offsetBefore(lastStoredOffset, lastProcessedOffset.get())) {
            this.consumer.store(this.lastProcessedOffset.get());
          }
          result = lastProcessedOffset.get();
        } catch (NoOffsetException e) {
          this.consumer.store(this.lastProcessedOffset.get());
          result = lastProcessedOffset.get();
        }
        this.lastTrackingActivity = clock.time();
        return result;
      }
    }

    @Override
    public StreamConsumer consumer() {
      return this.consumer;
    }

    @Override
    public LongConsumer trackingCallback() {
      return Utils.NO_OP_LONG_CONSUMER;
    }

    @Override
    public Runnable closingCallback() {
      return () -> {
        if (this.consumer.isSac() && !this.consumer.sacActive()) {
          LOGGER.debug("Not storing offset on closing because consumer is a non-active SAC");
        } else {
          if (this.lastProcessedOffset == null) {
            LOGGER.debug("Not storing anything as nothing has been processed.");
          } else {
            Runnable storageOperation =
                () -> {
                  this.consumer.store(this.lastProcessedOffset.get());
                  if (this.consumer.isSac()) {
                    LOGGER.debug(
                        "Consumer is SAC, making sure offset has been stored, in case another SAC takes over");
                    this.consumer.waitForOffsetToBeStored(lastProcessedOffset.get());
                  }
                };
            try {
              long lastStoredOffset = consumer.storedOffset();
              if (offsetBefore(lastStoredOffset, lastProcessedOffset.get())) {
                LOGGER.debug("Storing {} offset before closing", this.lastProcessedOffset);
                storageOperation.run();
              }
            } catch (NoOffsetException e) {
              LOGGER.debug(
                  "Nothing stored yet, storing {} offset before closing", this.lastProcessedOffset);
              storageOperation.run();
            }
          }
        }
      };
    }
  }

  private static final class ManualTrackingTracker implements Tracker {

    private final StreamConsumer consumer;
    private final LocalClock clock;
    private final long checkIntervalInNs;
    private volatile long lastRequestedOffset = 0;
    private volatile long lastTrackingActivity = 0;

    private ManualTrackingTracker(
        StreamConsumer consumer, TrackingConfiguration configuration, LocalClock clock) {
      this.consumer = consumer;
      this.clock = clock;
      this.checkIntervalInNs = configuration.manualCheckInterval().toNanos();
    }

    @Override
    public Consumer<Context> postProcessingCallback() {
      return null;
    }

    @Override
    public void flushIfNecessary() {
      if (this.clock.time() - this.lastTrackingActivity > this.checkIntervalInNs) {
        try {
          long lastStoredOffset = consumer.storedOffset();
          if (offsetBefore(lastStoredOffset, lastRequestedOffset)) {
            this.consumer.store(this.lastRequestedOffset);
            this.lastTrackingActivity = clock.time();
          }
        } catch (NoOffsetException e) {
          this.consumer.store(this.lastRequestedOffset);
          this.lastTrackingActivity = clock.time();
        }
      }
    }

    @Override
    public long flush() {
      throw new UnsupportedOperationException();
    }

    @Override
    public StreamConsumer consumer() {
      return this.consumer;
    }

    @Override
    public LongConsumer trackingCallback() {
      return requestedOffset -> {
        lastRequestedOffset = requestedOffset;
        lastTrackingActivity = clock.time();
      };
    }

    @Override
    public Runnable closingCallback() {
      return () -> {};
    }
  }

  private static class LocalClock {

    private volatile long time;

    long time() {
      return this.time;
    }

    public void setTime(long time) {
      this.time = time;
    }
  }

  @Override
  public String toString() {
    return "{ \"tracker_count\" : " + this.trackers.size() + " }";
  }
}

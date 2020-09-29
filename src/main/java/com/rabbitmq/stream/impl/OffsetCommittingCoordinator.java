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

import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.CommitConfiguration;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

class OffsetCommittingCoordinator {

  private final StreamEnvironment streamEnvironment;

  private final AtomicBoolean started = new AtomicBoolean(false);

  private final Collection<Tracker> trackers = ConcurrentHashMap.newKeySet();

  private final Clock clock = new Clock();

  private final AtomicBoolean flushingOnGoing = new AtomicBoolean(false);

  private final Duration checkInterval;

  private volatile Future<?> checkFuture;

  OffsetCommittingCoordinator(StreamEnvironment streamEnvironment) {
    this(streamEnvironment, Duration.ofSeconds(1));
  }

  OffsetCommittingCoordinator(StreamEnvironment streamEnvironment, Duration checkInterval) {
    this.streamEnvironment = streamEnvironment;
    this.checkInterval = checkInterval;
  }

  Registration registerCommittingConsumer(
      StreamConsumer consumer, CommitConfiguration configuration) {

    if (!configuration.enabled()) {
      throw new IllegalArgumentException("Commit must be enabled");
    }

    Tracker tracker;
    if (configuration.auto()) {
      tracker = new AutoCommitTracker(consumer, configuration, clock);
    } else {
      if (configuration.manualCheckInterval().isZero()) {
        throw new IllegalArgumentException(
            "There should be no registration if the check interval is 0");
      }
      tracker = new ManualCommitTracker(consumer, configuration, clock);
    }
    trackers.add(tracker);

    if (started.compareAndSet(false, true)) {
      this.clock.setTime(System.nanoTime());
      this.checkFuture =
          this.executor()
              .scheduleAtFixedRate(
                  () -> {
                    if (flushingOnGoing.compareAndSet(false, true)) {
                      try {
                        this.clock.setTime(System.nanoTime());
                        Iterator<Tracker> iterator = trackers.iterator();
                        while (iterator.hasNext()) {
                          if (Thread.currentThread().isInterrupted()) {
                            Thread.currentThread().interrupt();
                            break;
                          }
                          Tracker t = iterator.next();
                          if (t.consumer().isOpen()) {
                            t.flushIfNecessary();
                          } else {
                            iterator.remove();
                          }
                        }
                      } finally {
                        flushingOnGoing.set(false);
                      }

                      // TODO consider cancelling the task if there are no more consumers to track
                      // it should then be restarted on demand.

                    }
                  },
                  this.checkInterval.toMillis(),
                  this.checkInterval.toMillis(),
                  TimeUnit.MILLISECONDS);
    }

    return new Registration(tracker.postProcessingCallback(), tracker.commitCallback());
  }

  private ScheduledExecutorService executor() {
    return this.streamEnvironment.scheduledExecutorService();
  }

  public boolean needCommitRegistration(CommitConfiguration commitConfiguration) {
    return commitConfiguration.enabled()
        && !(commitConfiguration.manual()
            && Duration.ZERO.equals(commitConfiguration.manualCheckInterval()));
  }

  void close() {
    if (this.checkFuture != null) {
      checkFuture.cancel(true);
    }
  }

  private interface Tracker {

    Consumer<Context> postProcessingCallback();

    void flushIfNecessary();

    StreamConsumer consumer();

    LongConsumer commitCallback();
  }

  static class Registration {

    private final java.util.function.Consumer<Context> postMessageProcessingCallback;
    private final LongConsumer commitCallback;

    Registration(Consumer<Context> postMessageProcessingCallback, LongConsumer commitCallback) {
      this.postMessageProcessingCallback = postMessageProcessingCallback;
      this.commitCallback = commitCallback;
    }

    public Consumer<Context> postMessageProcessingCallback() {
      return postMessageProcessingCallback;
    }

    public LongConsumer commitCallback() {
      return commitCallback;
    }
  }

  private static final class AutoCommitTracker implements Tracker {

    private final StreamConsumer consumer;
    private final int messageCountBeforeCommit;
    private final long flushIntervalInNs;
    private final Clock clock;
    private volatile long count = 0;
    private volatile long lastProcessedOffset = 0;
    private volatile long lastCommitActivity = 0;

    private AutoCommitTracker(
        StreamConsumer consumer, CommitConfiguration configuration, Clock clock) {
      this.consumer = consumer;
      this.messageCountBeforeCommit = configuration.autoMessageCountBeforeCommit();
      this.flushIntervalInNs = configuration.autoFlushInterval().toNanos();
      this.clock = clock;
    }

    public Consumer<Context> postProcessingCallback() {
      return context -> {
        if (++count % messageCountBeforeCommit == 0) {
          context.commit();
          lastCommitActivity = clock.time();
        }
        lastProcessedOffset = context.offset();
      };
    }

    public void flushIfNecessary() {
      if (this.count > 0) {
        if (this.clock.time() - this.lastCommitActivity > this.flushIntervalInNs) {
          long lastCommittedOffset = consumer.lastCommittedOffset();
          if (lastCommittedOffset < lastProcessedOffset) {
            this.consumer.commit(this.lastProcessedOffset);
            this.lastCommitActivity = clock.time();
          }
        }
      }
    }

    @Override
    public StreamConsumer consumer() {
      return this.consumer;
    }

    @Override
    public LongConsumer commitCallback() {
      return Utils.NO_OP_LONG_CONSUMER;
    }
  }

  private static final class ManualCommitTracker implements Tracker {

    private final StreamConsumer consumer;
    private final Clock clock;
    private final long checkIntervalInNs;
    private volatile long lastRequestedOffset = 0;
    private volatile long lastCommitActivity = 0;

    private ManualCommitTracker(
        StreamConsumer consumer, CommitConfiguration configuration, Clock clock) {
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
      if (this.clock.time() - this.lastCommitActivity > this.checkIntervalInNs) {
        long lastCommittedOffset = consumer.lastCommittedOffset();
        if (lastCommittedOffset < lastRequestedOffset) {
          this.consumer.commit(this.lastRequestedOffset);
          this.lastCommitActivity = clock.time();
        }
      }
    }

    @Override
    public StreamConsumer consumer() {
      return this.consumer;
    }

    @Override
    public LongConsumer commitCallback() {
      return requestedOffset -> {
        lastRequestedOffset = requestedOffset;
        lastCommitActivity = clock.time();
      };
    }
  }

  private static class Clock {

    volatile long time;

    long time() {
      return this.time = System.nanoTime();
    }

    public void setTime(long time) {
      this.time = time;
    }
  }
}

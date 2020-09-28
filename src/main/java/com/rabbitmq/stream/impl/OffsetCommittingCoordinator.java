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

  java.util.function.Consumer<Context> registerCommittingConsumer(
      StreamConsumer consumer, CommitConfiguration configuration) {

    Tracker tracker = new Tracker(consumer, configuration, clock);
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
                          if (t.consumer.isOpen()) {
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

    return tracker.procProcessingCallback();
  }

  private ScheduledExecutorService executor() {
    return this.streamEnvironment.scheduledExecutorService();
  }

  public boolean needCommitRegistration(CommitConfiguration commitConfiguration) {
    return commitConfiguration.auto();
  }

  private static final class Tracker {

    private volatile long count = 0;
    private volatile long lastProcessedOffset = 0;
    private volatile long lastCommitActivity = 0;
    private final StreamConsumer consumer;
    private final int messageCountBeforeCommit;
    private final long flushIntervalInNs;
    private final Clock clock;

    private Tracker(StreamConsumer consumer, CommitConfiguration configuration, Clock clock) {
      this.consumer = consumer;
      this.messageCountBeforeCommit = configuration.autoMessageCountBeforeCommit();
      this.flushIntervalInNs = configuration.autoFlushInterval().toNanos();
      this.clock = clock;
    }

    Consumer<Context> procProcessingCallback() {
      return context -> {
        if (++count % messageCountBeforeCommit == 0) {
          context.commit();
          lastCommitActivity = clock.time();
        }
        lastProcessedOffset = context.offset();
      };
    }

    void flushIfNecessary() {
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
  }

  void close() {
    if (this.checkFuture != null) {
      checkFuture.cancel(true);
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

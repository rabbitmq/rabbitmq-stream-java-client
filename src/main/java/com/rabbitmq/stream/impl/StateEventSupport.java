// Copyright (c) 2024-2026 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.Resource;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StateEventSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateEventSupport.class);

  private final List<Resource.StateListener> listeners;
  private final Executor executor;
  private final Queue<Resource.Context> queue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean draining = new AtomicBoolean(false);

  StateEventSupport(List<Resource.StateListener> listeners, Executor executor) {
    this.listeners = List.copyOf(listeners);
    this.executor = executor;
  }

  void dispatch(Resource resource, Resource.State previousState, Resource.State currentState) {
    if (this.listeners.isEmpty()) {
      return;
    }
    this.queue.add(new DefaultContext(resource, previousState, currentState));
    scheduleDrain();
  }

  private void scheduleDrain() {
    // a single drain at a time: events are delivered in order, and never concurrently
    if (this.draining.compareAndSet(false, true)) {
      try {
        this.executor.execute(this::drain);
      } catch (Exception e) {
        // the environment is closed: deliver on this thread rather than lose e.g. CLOSED
        LOGGER.debug("Could not schedule state event dispatching: {}", e.getMessage());
        drain();
      }
    }
  }

  private void drain() {
    try {
      Resource.Context context;
      while ((context = this.queue.poll()) != null) {
        for (Resource.StateListener listener : this.listeners) {
          try {
            listener.handle(context);
          } catch (Exception e) {
            LOGGER.warn("Error in resource listener", e);
          }
        }
      }
    } finally {
      this.draining.set(false);
      // an event added after the last poll but before the reset would be stuck otherwise
      if (!this.queue.isEmpty()) {
        scheduleDrain();
      }
    }
  }

  private static class DefaultContext implements Resource.Context {

    private final Resource resource;
    private final Resource.State previousState;
    private final Resource.State currentState;

    private DefaultContext(
        Resource resource, Resource.State previousState, Resource.State currentState) {
      this.resource = resource;
      this.previousState = previousState;
      this.currentState = currentState;
    }

    @Override
    public Resource resource() {
      return this.resource;
    }

    @Override
    public Resource.State previousState() {
      return this.previousState;
    }

    @Override
    public Resource.State currentState() {
      return this.currentState;
    }
  }
}

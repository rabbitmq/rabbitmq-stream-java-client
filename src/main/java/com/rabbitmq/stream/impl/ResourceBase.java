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

import static com.rabbitmq.stream.Resource.State.CLOSED;
import static com.rabbitmq.stream.Resource.State.CLOSING;
import static com.rabbitmq.stream.Resource.State.OPEN;
import static com.rabbitmq.stream.Resource.State.OPENING;
import static com.rabbitmq.stream.Resource.State.RECOVERING;

import com.rabbitmq.stream.InvalidStateException;
import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.ResourceClosedException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

abstract class ResourceBase implements Resource {

  private final AtomicReference<State> state = new AtomicReference<>();
  private final StateEventSupport stateEventSupport;
  private final ConcurrentMap<String, Boolean> componentReadyState;
  private final boolean multiComponent;
  private final Lock stateLock = new ReentrantLock();

  ResourceBase(List<StateListener> listeners, String... componentIds) {
    this.stateEventSupport = new StateEventSupport(listeners);
    if (componentIds != null && componentIds.length > 0) {
      this.multiComponent = true;
      this.componentReadyState = new ConcurrentHashMap<>();
      for (String id : componentIds) {
        componentReadyState.put(id, Boolean.FALSE);
      }
    } else {
      this.multiComponent = false;
      this.componentReadyState = null;
    }
    this.state(OPENING);
  }

  protected void checkOpen() {
    State state = this.state.get();
    if (state == CLOSED) {
      throw new ResourceClosedException("Resource is closed");
    } else if (state != OPEN) {
      throw new InvalidStateException("Resource is not open, current state is %s", state.name());
    }
  }

  protected State state() {
    return this.state.get();
  }

  protected void state(Resource.State state) {
    Resource.State previousState = this.state.getAndSet(state);
    if (state != previousState) {
      this.dispatch(previousState, state);
    }
  }

  protected void componentUnavailable(String componentId) {
    if (!multiComponent) {
      throw new IllegalStateException("Resource is not configured for multi-component tracking");
    }
    Utils.lock(
        stateLock,
        () -> {
          componentReadyState.put(componentId, Boolean.FALSE);
          computeAndSetState();
        });
  }

  protected void componentReady(String componentId) {
    if (!multiComponent) {
      throw new IllegalStateException("Resource is not configured for multi-component tracking");
    }
    Utils.lock(
        stateLock,
        () -> {
          componentReadyState.put(componentId, Boolean.TRUE);
          computeAndSetState();
        });
  }

  private void computeAndSetState() {
    State currentState = this.state.get();
    if (currentState == CLOSING || currentState == CLOSED) {
      return;
    }
    boolean allReady = componentReadyState.values().stream().allMatch(Boolean::booleanValue);
    if (currentState == OPENING) {
      if (allReady) {
        this.state(OPEN);
      }
      return;
    }
    Resource.State newState = allReady ? OPEN : RECOVERING;
    this.state(newState);
  }

  private void dispatch(State previous, State current) {
    this.stateEventSupport.dispatch(this, previous, current);
  }
}

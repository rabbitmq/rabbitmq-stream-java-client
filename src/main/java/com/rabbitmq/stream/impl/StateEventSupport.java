// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StateEventSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateEventSupport.class);

  private final List<Resource.StateListener> listeners;

  StateEventSupport(List<Resource.StateListener> listeners) {
    this.listeners = List.copyOf(listeners);
  }

  void dispatch(Resource resource, Resource.State previousState, Resource.State currentState) {
    if (!this.listeners.isEmpty()) {
      Resource.Context context = new DefaultContext(resource, previousState, currentState);
      this.listeners.forEach(
          l -> {
            try {
              l.handle(context);
            } catch (Exception e) {
              LOGGER.warn("Error in resource listener", e);
            }
          });
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

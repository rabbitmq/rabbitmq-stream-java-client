// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Utils.keyForNode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.Broker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * The connections of a coordinator, with at most one connection being opened per node at a time.
 *
 * <p>Owned by the coordinator's event loop: every instance method must run on it, which is what
 * makes {@link #placement} atomic.
 */
final class ConnectionPool<C extends ConnectionPool.PooledConnection & Comparable<C>> {

  interface PooledConnection {

    Broker node();

    boolean isDead();
  }

  private final NavigableSet<C> connections = new TreeSet<>();
  // one connection creation at a time per node, so concurrent placements share the connection
  // being opened instead of each opening their own
  private final Set<String> creating = new HashSet<>();
  private final Map<String, List<CompletableFuture<Void>>> waiters = new HashMap<>();

  /**
   * Pick an existing connection to the node with room for the agent, or reserve the right to open
   * one.
   */
  Placement<C> placement(Broker node, Predicate<C> hasRoom) {
    this.connections.removeIf(PooledConnection::isDead);
    for (C connection : this.connections) {
      if (node.equals(connection.node()) && hasRoom.test(connection)) {
        return Placement.use(connection);
      }
    }
    String key = keyForNode(node);
    if (this.creating.add(key)) {
      return Placement.create();
    }
    CompletableFuture<Void> waiter = new CompletableFuture<>();
    this.waiters.computeIfAbsent(key, k -> new ArrayList<>()).add(waiter);
    return Placement.waitFor(waiter);
  }

  /**
   * End the creation {@link #placement} reserved for the node, and wake up the placements waiting
   * for it.
   *
   * @param connection the new connection, or null if its creation failed
   */
  void creationFinished(Broker node, C connection) {
    String key = keyForNode(node);
    this.creating.remove(key);
    if (connection != null) {
      this.connections.add(connection);
    }
    List<CompletableFuture<Void>> nodeWaiters = this.waiters.remove(key);
    if (nodeWaiters != null) {
      nodeWaiters.forEach(w -> w.complete(null));
    }
  }

  void remove(C connection) {
    this.connections.remove(connection);
  }

  List<C> connections() {
    return new ArrayList<>(this.connections);
  }

  /** Empty the pool, for closing, and return what it held. */
  List<C> drain() {
    List<C> all = new ArrayList<>(this.connections);
    this.connections.clear();
    return all;
  }

  int size() {
    return this.connections.size();
  }

  /**
   * Wait for the connection creation a placement is waiting for. Blocking, so never on the loop.
   *
   * @param type the kind of connection, for error messages
   */
  static void awaitCreation(CompletableFuture<Void> waiter, Duration timeout, String type) {
    try {
      waiter.get(timeout.toMillis(), MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StreamException("Interrupted while waiting for a " + type + " connection", e);
    } catch (ExecutionException e) {
      throw new StreamException("Error while waiting for a " + type + " connection", e);
    } catch (TimeoutException e) {
      throw new TimeoutStreamException("Timeout while waiting for a " + type + " connection");
    }
  }

  /** Where to put an agent: on an existing connection, on a new one, or after a creation. */
  static final class Placement<C> {

    private final C connection;
    private final CompletableFuture<Void> waitFor;

    private Placement(C connection, CompletableFuture<Void> waitFor) {
      this.connection = connection;
      this.waitFor = waitFor;
    }

    private static <C> Placement<C> use(C connection) {
      return new Placement<>(connection, null);
    }

    private static <C> Placement<C> create() {
      return new Placement<>(null, null);
    }

    private static <C> Placement<C> waitFor(CompletableFuture<Void> waiter) {
      return new Placement<>(null, waiter);
    }

    /** The connection to use, or null to open one, unless {@link #waitFor()} is set. */
    C connection() {
      return this.connection;
    }

    /** The creation to wait for before picking again, or null. */
    CompletableFuture<Void> waitFor() {
      return this.waitFor;
    }
  }
}

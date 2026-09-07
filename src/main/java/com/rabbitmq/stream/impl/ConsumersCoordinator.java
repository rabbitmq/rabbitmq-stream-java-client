// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.CoordinatorUtils.shouldRefreshCandidates;
import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;
import static com.rabbitmq.stream.impl.Utils.AVAILABLE_PROCESSORS;
import static com.rabbitmq.stream.impl.Utils.brokerFromClient;
import static com.rabbitmq.stream.impl.Utils.convertCodeToException;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.isSac;
import static com.rabbitmq.stream.impl.Utils.jsonField;
import static com.rabbitmq.stream.impl.Utils.keyForNode;
import static com.rabbitmq.stream.impl.Utils.namedFunction;
import static com.rabbitmq.stream.impl.Utils.quote;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.SubscriptionListener.SubscriptionContext;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ChunkListener;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.ConsumerUpdateListener;
import com.rabbitmq.stream.impl.Client.CreditNotification;
import com.rabbitmq.stream.impl.Client.MessageIgnoredListener;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.MetadataListener;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.CoordinatorUtils.ClientClosedException;
import com.rabbitmq.stream.impl.SubscriptionStateMachine.State;
import com.rabbitmq.stream.impl.SubscriptionStateMachine.TransitionResult;
import com.rabbitmq.stream.impl.Utils.BrokerWrapper;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumersCoordinator implements AutoCloseable {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;
  static final int MAX_ATTEMPT_BEFORE_FALLING_BACK_TO_LEADER = 5;
  private static final int RECOVERY_THREADS = Math.max(2, Math.min(4, AVAILABLE_PROCESSORS));
  private static final long FIRST_ATTEMPT_EPOCH = 1;
  // sentinel subscription ID: no slot reserved, or a reservation that was rolled back or freed
  private static final byte NO_SLOT = -1;
  // how long a node that just failed a connection attempt is deprioritized for new placements;
  // short enough that a node which has actually come back is not avoided for long
  private static final long SUSPECT_TTL_NANOS = SECONDS.toNanos(5);
  // insurance against a subscription stuck in RECOVERING because of a bug not yet found: every
  // known way to get stuck is already fixed by the epoch-supersede mechanism the watchdog itself
  // uses, so the threshold is generous, not tuned to any known failure timing
  private static final long WATCHDOG_TICK_INTERVAL_MS = SECONDS.toMillis(30);
  static final long WATCHDOG_STUCK_THRESHOLD_NANOS = SECONDS.toNanos(120);
  // how long an emptied connection is kept around before actually closing it, so a subscription
  // landing on the same node moments later (e.g. during a rolling restart) can reuse it instead
  // of reconnecting
  private static final long IDLE_LINGER_MS = SECONDS.toMillis(3);
  private static final java.util.function.Consumer<TrackerState> NO_STATE_CHANGE = s -> {};

  static final OffsetSpecification DEFAULT_OFFSET_SPECIFICATION = OffsetSpecification.next();

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumersCoordinator.class);
  private final StreamEnvironment environment;
  private final ClientFactory clientFactory;
  private final int maxConsumersByConnection;
  private final Function<ClientConnectionType, String> connectionNamingStrategy;
  private final AtomicLong managerIdSequence = new AtomicLong(0);
  private final AtomicLong trackerIdSequence = new AtomicLong(0);
  private final Function<List<Broker>, Broker> brokerPicker;

  private final ExecutorServiceFactory executorServiceFactory =
      new DefaultExecutorServiceFactory(
          AVAILABLE_PROCESSORS, 10, "rabbitmq-stream-consumer-connection-");
  private final boolean forceReplica;
  private final EventExecutorGroup eventExecutorGroup;
  private final boolean privateEventExecutorGroup;
  private final EventLoop eventLoop;
  private final EventLoop.Client<CoordinatorState> state;
  // recovery must not share the environment scheduler: blocking recovery work there starves
  // the AsyncRetry continuations it depends on
  private final ExecutorService recoveryExecutor;
  // lazily started by the first subscribe(), not the constructor: no point ticking before there
  // is anything to watch
  private final AtomicBoolean watchdogScheduled = new AtomicBoolean(false);
  private volatile ScheduledFuture<?> watchdogTask;

  /**
   * @param eventExecutorGroup the group backing the control-plane event loop, or null for the
   *     coordinator to create and own its own. It must have exactly one thread: the loop state is
   *     shared across all connections and subscriptions, so a second thread would silently split
   *     it. Tests inject a deterministic group here; a caller-supplied group is not closed by
   *     {@link #close()}.
   */
  ConsumersCoordinator(
      StreamEnvironment environment,
      int maxConsumersByConnection,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      ClientFactory clientFactory,
      boolean forceReplica,
      Function<List<Broker>, Broker> brokerPicker,
      EventExecutorGroup eventExecutorGroup) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxConsumersByConnection =
        Math.min(maxConsumersByConnection, MAX_SUBSCRIPTIONS_PER_CLIENT);
    this.connectionNamingStrategy = connectionNamingStrategy;
    this.forceReplica = forceReplica;
    this.brokerPicker = brokerPicker;
    if (eventExecutorGroup == null) {
      // not the environment's netty I/O group on purpose: sharing with channel I/O would let the
      // loop thread be the thread blocked on a socket
      this.eventExecutorGroup =
          new DefaultEventExecutorGroup(1, threadFactory("rabbitmq-stream-consumer-coordinator-"));
      this.privateEventExecutorGroup = true;
    } else {
      this.eventExecutorGroup = eventExecutorGroup;
      this.privateEventExecutorGroup = false;
    }
    this.eventLoop = new EventLoop(this.eventExecutorGroup, environment.rpcTimeout());
    this.recoveryExecutor =
        Executors.newFixedThreadPool(
            RECOVERY_THREADS, threadFactory("rabbitmq-stream-consumer-recovery-"));
    this.state = this.eventLoop.register(CoordinatorState::new);
  }

  private BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.environment.recoveryBackOffDelayPolicy();
  }

  private BackOffDelayPolicy metadataUpdateBackOffDelayPolicy() {
    return environment.topologyUpdateBackOffDelayPolicy();
  }

  Runnable subscribe(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      String trackingReference,
      SubscriptionListener subscriptionListener,
      Runnable trackingClosingCallback,
      MessageHandler messageHandler,
      Map<String, String> subscriptionProperties,
      ConsumerFlowStrategy flowStrategy) {
    ensureWatchdogScheduled();
    List<BrokerWrapper> candidates = findCandidateNodes(stream, forceReplica);
    Broker newNode = pickBroker(this.brokerPicker, usableCandidates(candidates));
    if (newNode == null) {
      throw new IllegalStateException("No available node to subscribe to");
    }

    // create stream subscription to track final and changing state of this very subscription
    // we keep this instance when we move the subscription from a client to another one
    SubscriptionTracker subscriptionTracker =
        new SubscriptionTracker(
            this.trackerIdSequence.getAndIncrement(),
            consumer,
            stream,
            offsetSpecification,
            trackingReference,
            subscriptionListener,
            trackingClosingCallback,
            messageHandler,
            subscriptionProperties,
            flowStrategy);

    registerSubscription(subscriptionTracker);
    try {
      addToManager(newNode, candidates, subscriptionTracker, offsetSpecification, true);
    } catch (RuntimeException e) {
      // the initial subscription does not retry, the failure goes back to the caller
      trackerEvent(
          subscriptionTracker,
          recoveryBackOffDelayPolicy(),
          (st, epoch) -> SubscriptionStateMachine.onAssignmentFailed(st, epoch, epoch, e, false));
      if (e instanceof ConnectionStreamException) {
        // these exceptions are not public
        throw new StreamException(e.getMessage());
      }
      throw e;
    }
    assignmentSucceeded(subscriptionTracker, recoveryBackOffDelayPolicy(), FIRST_ATTEMPT_EPOCH);

    return () -> {
      // cancel() first, synchronously: if the assignment is already confirmed, this is the only
      // remover in the race and always wins it. Posting onCancelled afterward means its async
      // releaseAssignment effect finds nothing left to do in that case, and remains the sole,
      // eventual remover for an assignment that was still being established (see
      // SubscriptionTracker.confirmAssignment)
      subscriptionTracker.cancel();
      trackerEvent(
          subscriptionTracker, recoveryBackOffDelayPolicy(), SubscriptionStateMachine::onCancelled);
    };
  }

  private void addToManager(
      Broker node,
      List<BrokerWrapper> candidates,
      SubscriptionTracker tracker,
      OffsetSpecification offsetSpecification,
      boolean isInitialSubscription) {
    ClientParameters clientParameters =
        environment
            .clientParametersCopy()
            .executorServiceFactory(this.executorServiceFactory)
            .host(node.getHost())
            .port(node.getPort());
    LOGGER.debug("Finding a manager for consumer {}", tracker.consumer.id());
    while (true) {
      Placement placement = placement(node);
      if (placement.waitFor != null) {
        // a connection to this node is being opened, share it instead of opening another one
        awaitConnectionCreation(placement.waitFor);
        continue;
      }
      ClientSubscriptionsManager pickedManager = placement.manager;
      if (pickedManager == null) {
        String name = keyForNode(node);
        LOGGER.debug("Creating subscription manager on {}", name);
        try {
          pickedManager = new ClientSubscriptionsManager(node, candidates, clientParameters);
        } catch (RuntimeException e) {
          creationFinished(node, null);
          throw e;
        }
        LOGGER.debug("Created subscription manager on {}, id {}", name, pickedManager.id);
        creationFinished(node, pickedManager);
      }
      try {
        pickedManager.add(tracker, offsetSpecification, isInitialSubscription);
        LOGGER.debug(
            "Assigned tracker {} to manager {} (node {}), subscription ID {}, consumer {}",
            tracker.label(),
            pickedManager.id,
            pickedManager.name,
            tracker.subscriptionIdInClient(),
            tracker.consumer.id());
        return;
      } catch (IllegalStateException e) {
        // full or closed in the meantime, pick again
      } catch (RuntimeException e) {
        if (shouldRefreshCandidates(e)) {
          // manager connection is dead or stream not available: deprioritize this node for new
          // placements for a short while, so a subscription being redistributed does not keep
          // landing back on a node that is mid-restart
          submitState(
              s -> s.suspectUntil.put(keyForNode(node), System.nanoTime() + SUSPECT_TTL_NANOS));
          // scheduling manager closing if necessary in another thread to avoid blocking this one
          if (pickedManager.isEmpty()) {
            ClientSubscriptionsManager toClose = pickedManager;
            ConsumersCoordinator.this.environment.execute(
                toClose::closeIfEmpty,
                "Consumer manager closing after timeout, consumer %d on stream '%s'",
                tracker.consumer.id(),
                tracker.stream);
          }
        } else {
          pickedManager.closeIfEmpty();
        }
        throw e;
      }
    }
  }

  /**
   * Pick an existing connection to the node with spare capacity, or reserve the right to open one.
   *
   * <p>Atomic by construction: it runs on the event loop, which is the single writer of the pool.
   */
  private Placement placement(Broker node) {
    String key = keyForNode(node);
    return this.state.query(
        s -> {
          s.connections.removeIf(ClientSubscriptionsManager::isDead);
          for (ClientSubscriptionsManager manager : s.connections) {
            if (node.equals(manager.node) && !manager.isFull()) {
              return Placement.use(manager);
            }
          }
          if (s.creating.add(key)) {
            return Placement.create();
          }
          CompletableFuture<Void> waiter = new CompletableFuture<>();
          s.waiters.computeIfAbsent(key, k -> new ArrayList<>()).add(waiter);
          return Placement.waitFor(waiter);
        });
  }

  /**
   * Candidates with recently-failed nodes deprioritized, unless that leaves nothing to pick from.
   */
  private List<BrokerWrapper> usableCandidates(List<BrokerWrapper> candidates) {
    // nanoTime, not currentTimeMillis: this is a deadline comparison, and must not be disturbed
    // by a wall-clock adjustment
    return this.state.query(
        s -> deprioritizeSuspects(candidates, s.suspectUntil, System.nanoTime()));
  }

  private void creationFinished(Broker node, ClientSubscriptionsManager manager) {
    String key = keyForNode(node);
    submitState(
        s -> {
          s.creating.remove(key);
          if (manager != null) {
            s.connections.add(manager);
          }
          List<CompletableFuture<Void>> waiters = s.waiters.remove(key);
          if (waiters != null) {
            waiters.forEach(w -> w.complete(null));
          }
        });
  }

  private void awaitConnectionCreation(CompletableFuture<Void> waiter) {
    try {
      waiter.get(this.environment.rpcTimeout().toMillis(), MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StreamException("Interrupted while waiting for a consumer connection", e);
    } catch (ExecutionException e) {
      throw new StreamException("Error while waiting for a consumer connection", e);
    } catch (TimeoutException e) {
      throw new TimeoutStreamException("Timeout while waiting for a consumer connection");
    }
  }

  /**
   * Read loop-owned state for monitoring, falling back when the loop is gone.
   *
   * <p>Monitoring outlives the coordinator: {@code StreamEnvironment.toString()} is legitimately
   * called on a closed environment, and must not throw.
   */
  private <R> R queryState(
      java.util.function.Function<CoordinatorState, R> query, R valueIfClosed) {
    if (this.state.isClosed()) {
      return valueIfClosed;
    }
    try {
      return this.state.query(query);
    } catch (IllegalStateException e) {
      // the loop was closed concurrently
      return valueIfClosed;
    }
  }

  /**
   * Post to the loop, tolerating a closed loop.
   *
   * <p>Callers include netty I/O threads, whose connection events can arrive while the coordinator
   * is closing; an exception there would surface on an I/O thread.
   */
  private void submitState(java.util.function.Consumer<CoordinatorState> task) {
    try {
      this.state.submit(task);
    } catch (IllegalStateException e) {
      LOGGER.debug("Coordinator event loop is closed, dropping task");
    }
  }

  private void registerSubscription(SubscriptionTracker tracker) {
    submitState(s -> s.subscriptions.put(tracker.id, new TrackerState(tracker)));
  }

  /**
   * Apply a decision function to a subscription's control state on the event loop.
   *
   * <p>Fire-and-forget, because the callers include netty I/O threads, which must never wait on the
   * loop.
   */
  private void trackerEvent(
      SubscriptionTracker tracker,
      BackOffDelayPolicy delayPolicy,
      BiFunction<State, Long, TransitionResult> decision) {
    trackerEvent(tracker, delayPolicy, NO_STATE_CHANGE, decision);
  }

  /**
   * Same, with a mutation applied to the subscription's counters in the very same loop task as the
   * decision, for events that both update a counter and transition on it.
   */
  private void trackerEvent(
      SubscriptionTracker tracker,
      BackOffDelayPolicy delayPolicy,
      java.util.function.Consumer<TrackerState> beforeDecision,
      BiFunction<State, Long, TransitionResult> decision) {
    submitState(
        s -> {
          TrackerState trackerState = s.subscriptions.get(tracker.id);
          if (trackerState == null) {
            return;
          }
          beforeDecision.accept(trackerState);
          TransitionResult result = decision.apply(trackerState.state, trackerState.epoch);
          boolean newAttempt =
              result.state() == State.RECOVERING && result.epoch() != trackerState.epoch;
          trackerState.state = result.state();
          trackerState.epoch = result.epoch();
          if (newAttempt) {
            trackerState.attempts++;
            // the attempt is either dispatched right away or scheduled after the back-off delay,
            // and which one is only decided in the effect. Assume the delay applies, so the
            // watchdog measures "stuck" from the point the attempt is due at the latest and never
            // cuts short a configured back-off
            trackerState.nextAttemptAt =
                System.nanoTime() + backOffNanos(delayPolicy, trackerState.attempts);
          }
          if (result.state() == State.ACTIVE) {
            // a successful assignment ends the recovery episode: the retry timeout is meant to
            // bound one episode, not the subscription's whole life
            trackerState.attempts = 0;
            trackerState.failedLookups = 0;
          }
          if (result.state().terminal()) {
            s.subscriptions.remove(tracker.id);
          }
          if (result.hasEffect()) {
            // the whole effect goes to a single task: the effects of one transition are ordered
            // (detach before re-assign, for instance), which separate tasks on a multi-threaded
            // pool would not guarantee
            TrackerActions actions =
                new TrackerActions(
                    tracker, delayPolicy, trackerState.attempts, trackerState.failedLookups);
            submitRecovery(
                () -> {
                  try {
                    result.applyEffect(actions);
                  } catch (Throwable e) {
                    LOGGER.warn(
                        "Error while applying transition effect for subscription {}: {}",
                        tracker.label(),
                        e.getMessage());
                  }
                });
          }
        });
  }

  /**
   * The back-off delay for an attempt, in nanoseconds, or 0 if the policy has given up.
   *
   * <p>{@link BackOffDelayPolicy#TIMEOUT} is {@code Duration.ofMillis(Long.MAX_VALUE)}, so it has
   * to be excluded before converting: {@code toNanos()} would overflow on it.
   */
  private static long backOffNanos(BackOffDelayPolicy delayPolicy, int attempts) {
    Duration delay = delayPolicy.delay(attempts);
    return BackOffDelayPolicy.TIMEOUT.equals(delay) ? 0 : delay.toNanos();
  }

  /**
   * Whether the watchdog should start a fresh attempt for a subscription in this state.
   *
   * <p>Measured against when the current attempt is <b>due</b>, not when it was created: a
   * subscription waiting out its back-off delay is waiting by design, not stuck, so comparing
   * against the creation time would let the watchdog cut short any configured delay longer than the
   * stuck threshold.
   */
  static boolean watchdogShouldReDispatch(State state, long nextAttemptAt, long now) {
    // subtraction, not a direct comparison, so this stays correct across a nanoTime() wraparound
    return state == State.RECOVERING && now - nextAttemptAt > WATCHDOG_STUCK_THRESHOLD_NANOS;
  }

  private void ensureWatchdogScheduled() {
    if (this.watchdogScheduled.compareAndSet(false, true)) {
      this.watchdogTask =
          this.environment
              .scheduledExecutorService()
              .scheduleAtFixedRate(
                  this::watchdogTick,
                  WATCHDOG_TICK_INTERVAL_MS,
                  WATCHDOG_TICK_INTERVAL_MS,
                  MILLISECONDS);
    }
  }

  /**
   * Re-trigger any subscription that has been {@code RECOVERING} past the stuck threshold.
   *
   * <p>Insurance against bugs not yet found, not a fix for a known one: every known way to get
   * stuck in {@code RECOVERING} is already fixed by the epoch-supersede mechanism this reuses (see
   * {@link SubscriptionStateMachine}).
   *
   * <p>Package-protected for testing: a test can call this directly instead of waiting out the real
   * tick interval.
   */
  void watchdogTick() {
    submitState(
        s -> {
          long now = System.nanoTime();
          // collected first, then dispatched from a separate pass: dispatching inline while
          // iterating s.subscriptions.values() would corrupt the iterator, since a transition can
          // remove its own entry (e.g. onCancelled, for a consumer that closed while stuck)
          List<TrackerState> stuck = new ArrayList<>();
          for (TrackerState trackerState : s.subscriptions.values()) {
            if (watchdogShouldReDispatch(trackerState.state, trackerState.nextAttemptAt, now)) {
              stuck.add(trackerState);
            }
          }
          for (TrackerState trackerState : stuck) {
            long attemptEpoch = trackerState.epoch;
            if (!trackerState.tracker.consumer.isOpen()) {
              trackerEvent(
                  trackerState.tracker,
                  recoveryBackOffDelayPolicy(),
                  SubscriptionStateMachine::onCancelled);
            } else {
              trackerEvent(
                  trackerState.tracker,
                  recoveryBackOffDelayPolicy(),
                  (st, epoch) -> SubscriptionStateMachine.onWatchdogTick(st, epoch, attemptEpoch));
            }
          }
        });
  }

  // test support: bring every subscription's next attempt forward by the given amount, so a test
  // can exercise watchdogTick() deterministically instead of waiting out the real stuck threshold
  // or a back-off delay deliberately set longer than it
  void ageWatchdogClocksBy(Duration duration) {
    this.state.query(
        s -> {
          for (TrackerState trackerState : s.subscriptions.values()) {
            trackerState.nextAttemptAt -= duration.toNanos();
          }
          return null;
        });
  }

  private void assignmentSucceeded(
      SubscriptionTracker tracker, BackOffDelayPolicy delayPolicy, long attemptEpoch) {
    trackerEvent(
        tracker,
        delayPolicy,
        (st, epoch) -> SubscriptionStateMachine.onAssignmentSucceeded(st, epoch, attemptEpoch));
  }

  private void assignmentFailed(
      SubscriptionTracker tracker,
      BackOffDelayPolicy delayPolicy,
      long attemptEpoch,
      Throwable cause,
      boolean recoverable) {
    trackerEvent(
        tracker,
        delayPolicy,
        (st, epoch) ->
            SubscriptionStateMachine.onAssignmentFailed(
                st, epoch, attemptEpoch, cause, recoverable));
  }

  /**
   * A failed candidate lookup: count it and park the subscription for another attempt later.
   *
   * <p>The counter update and the transition that reads it share one loop task, so the next
   * attempt's {@link TrackerActions} always sees this failure.
   */
  private void lookupFailed(
      SubscriptionTracker tracker,
      BackOffDelayPolicy delayPolicy,
      long attemptEpoch,
      Throwable cause) {
    trackerEvent(
        tracker,
        delayPolicy,
        trackerState -> trackerState.failedLookups++,
        (st, epoch) ->
            SubscriptionStateMachine.onAssignmentFailed(st, epoch, attemptEpoch, cause, true));
  }

  /**
   * One assignment attempt. Blocking, so it always runs on the recovery pool.
   *
   * <p>Bounded on purpose: exactly one candidate lookup, then either an assignment or a transition.
   * A lookup that fails does not retry here — it parks the subscription, so the recovery pool has
   * only {@code RECOVERY_THREADS} threads and a stream that stays unreachable must never own one
   * while it waits. The back-off policy is the retry mechanism, and {@link
   * TrackerActions#scheduleAssignment} is where it gives up.
   */
  private void assign(
      SubscriptionTracker tracker,
      long attemptEpoch,
      BackOffDelayPolicy delayPolicy,
      int failedLookups) {
    if (!tracker.consumer.isOpen()) {
      LOGGER.debug(
          "Not re-assigning consumer {} (stream '{}') because it has been closed",
          tracker.consumer.id(),
          tracker.stream);
      trackerEvent(tracker, delayPolicy, SubscriptionStateMachine::onCancelled);
      return;
    }
    List<BrokerWrapper> candidates;
    boolean mustUseReplica =
        this.forceReplica && failedLookups < MAX_ATTEMPT_BEFORE_FALLING_BACK_TO_LEADER;
    try {
      candidates = findCandidateNodes(tracker.stream, mustUseReplica);
    } catch (StreamDoesNotExistException e) {
      // the stream is gone: there is nothing to come back to, so this subscription is over
      LOGGER.debug("Stream '{}' does not exist, closing subscription", tracker.stream);
      assignmentFailed(tracker, delayPolicy, attemptEpoch, e, false);
      return;
    } catch (Exception e) {
      LOGGER.debug(
          "Candidate lookup for stream '{}' failed, parking subscription: {}",
          tracker.stream,
          Utils.exceptionMessage(e));
      lookupFailed(tracker, delayPolicy, attemptEpoch, e);
      return;
    }
    try {
      Broker broker = pickBroker(this.brokerPicker, usableCandidates(candidates));
      LOGGER.debug("Using {} to resume consuming from {}", broker, tracker.stream);
      OffsetSpecification offsetSpecification =
          tracker.hasReceivedSomething
              ? OffsetSpecification.offset(tracker.offset)
              : tracker.initialOffsetSpecification;
      addToManager(broker, candidates, tracker, offsetSpecification, false);
      assignmentSucceeded(tracker, delayPolicy, attemptEpoch);
    } catch (Exception e) {
      LOGGER.debug(
          "Error while assigning subscription {}: {}", tracker.label(), Utils.exceptionMessage(e));
      assignmentFailed(
          tracker, delayPolicy, attemptEpoch, e, SubscriptionStateMachine.recoverable(e));
    }
  }

  private void submitRecovery(Runnable task) {
    try {
      this.recoveryExecutor.execute(task);
    } catch (RejectedExecutionException e) {
      LOGGER.debug("Consumer recovery task rejected, the coordinator is closing");
    }
  }

  /**
   * Runs the effects of a transition.
   *
   * <p>Always invoked from a single task on the recovery pool, never on the event loop: the calls
   * here block, and the consumer notifications take {@link StreamConsumer}'s lock, which is shared
   * with the offset-tracking coordinator.
   */
  private final class TrackerActions implements SubscriptionStateMachine.Actions {

    private final SubscriptionTracker tracker;
    private final BackOffDelayPolicy delayPolicy;
    private final int attempts;
    private final int failedLookups;

    private TrackerActions(
        SubscriptionTracker tracker,
        BackOffDelayPolicy delayPolicy,
        int attempts,
        int failedLookups) {
      this.tracker = tracker;
      this.delayPolicy = delayPolicy;
      this.attempts = attempts;
      this.failedLookups = failedLookups;
    }

    @Override
    public void dispatchAssignment(long attemptEpoch) {
      assign(this.tracker, attemptEpoch, this.delayPolicy, this.failedLookups);
    }

    @Override
    public void scheduleAssignment(long attemptEpoch, Throwable cause) {
      Duration delay = this.delayPolicy.delay(this.attempts);
      if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
        LOGGER.debug(
            "Giving up on subscription {} after {} attempt(s)",
            this.tracker.label(),
            this.attempts);
        assignmentFailed(this.tracker, this.delayPolicy, attemptEpoch, cause, false);
        return;
      }
      environment
          .scheduledExecutorService()
          .schedule(
              () ->
                  submitRecovery(
                      () ->
                          assign(this.tracker, attemptEpoch, this.delayPolicy, this.failedLookups)),
              delay.toMillis(),
              MILLISECONDS);
    }

    @Override
    public void markRecovering() {
      // user code runs here: detaching notifies a single active consumer it became inactive.
      // it must not be able to prevent the re-assignment that follows in this same task
      notifyConsumer(
          () -> {
            this.tracker.detachFromManager();
            this.tracker.markRecovering();
          },
          "marking recovering");
    }

    @Override
    public void markOpen() {
      notifyConsumer(this.tracker::markOpen, "marking open");
    }

    private void notifyConsumer(Runnable notification, String description) {
      try {
        notification.run();
      } catch (Exception e) {
        LOGGER.warn(
            "Error while {} for subscription {}: {}",
            description,
            this.tracker.label(),
            Utils.exceptionMessage(e));
      }
    }

    @Override
    public void closeConsumerAfterStreamDeletion(Throwable cause) {
      try {
        this.tracker.consumer.closeAfterStreamDeletion();
      } catch (Exception e) {
        LOGGER.debug("Error while closing consumer: {}", e.getMessage());
      }
    }

    @Override
    public void releaseAssignment() {
      ClientSubscriptionsManager manager = this.tracker.manager();
      if (manager != null) {
        // detachIfOwnedBy() is idempotent, so this is a no-op if the assignment has already
        // been released (e.g. by the direct call in cancel())
        manager.remove(this.tracker);
      }
    }
  }

  private static final class Placement {

    private final ClientSubscriptionsManager manager;
    private final CompletableFuture<Void> waitFor;

    private Placement(ClientSubscriptionsManager manager, CompletableFuture<Void> waitFor) {
      this.manager = manager;
      this.waitFor = waitFor;
    }

    private static Placement use(ClientSubscriptionsManager manager) {
      return new Placement(manager, null);
    }

    private static Placement create() {
      return new Placement(null, null);
    }

    private static Placement waitFor(CompletableFuture<Void> waiter) {
      return new Placement(null, waiter);
    }
  }

  /** Result of {@code reserveSlot}: a slot number, or the reason none was reserved. */
  private static final class SlotReservation {

    private static final SlotReservation FULL = new SlotReservation(NO_SLOT, true, false);
    private static final SlotReservation DEAD = new SlotReservation(NO_SLOT, false, true);

    private final byte subscriptionId;
    private final boolean full;
    private final boolean dead;

    private SlotReservation(byte subscriptionId, boolean full, boolean dead) {
      this.subscriptionId = subscriptionId;
      this.full = full;
      this.dead = dead;
    }

    private static SlotReservation reserved(byte subscriptionId) {
      return new SlotReservation(subscriptionId, false, false);
    }
  }

  int managerCount() {
    return queryState(s -> s.connections.size(), 0);
  }

  // the connection pool is coordinator-owned state, so managers do not reach into it directly.
  // step 4 of the redesign replaces this call with an event posted to the event loop
  private void removeFromPool(ClientSubscriptionsManager manager) {
    // fire-and-forget: this is called from netty I/O threads, which must never wait on the loop
    submitState(s -> s.connections.remove(manager));
  }

  // package protected for testing
  List<BrokerWrapper> findCandidateNodes(String stream, boolean forceReplica) {
    LOGGER.debug(
        "Candidate lookup to consumer from '{}', forcing replica? {}", stream, forceReplica);
    Map<String, Client.StreamMetadata> metadata =
        this.environment.locatorOperation(
            namedFunction(
                c -> c.metadata(stream), "Candidate lookup to consume from '%s'", stream));
    return candidatesFromMetadata(stream, metadata, forceReplica);
  }

  // pure: the blocking locator lookup above hands its result to this function, so the decision
  // can be applied wherever the result arrives
  static List<BrokerWrapper> candidatesFromMetadata(
      String stream, Map<String, Client.StreamMetadata> metadata, boolean forceReplica) {
    if (metadata.isEmpty() || metadata.get(stream) == null) {
      // this is not supposed to happen
      throw new StreamDoesNotExistException(stream);
    }

    Client.StreamMetadata streamMetadata = metadata.get(stream);
    if (!streamMetadata.isResponseOk()) {
      if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
        throw new StreamDoesNotExistException(stream);
      } else {
        throw new IllegalStateException(
            "Could not get stream metadata, response code: "
                + formatConstant(streamMetadata.getResponseCode()));
      }
    }

    Broker leader = streamMetadata.getLeader();
    List<Broker> replicas = streamMetadata.getReplicas();
    if ((replicas == null || replicas.isEmpty()) && leader == null) {
      throw new IllegalStateException("No node available to consume from stream " + stream);
    }

    List<BrokerWrapper> brokers;
    if (replicas == null || replicas.isEmpty()) {
      if (forceReplica) {
        throw new IllegalStateException(
            format(
                "Only the leader node is available for consuming from %s and "
                    + "consuming from leader has been deactivated for this consumer",
                stream));
      } else {
        brokers = Collections.singletonList(new BrokerWrapper(leader, true));
        LOGGER.debug("Only leader node {} for consuming from {}", leader, stream);
      }
    } else {
      LOGGER.debug("Replicas for consuming from {}: {}", stream, replicas);
      brokers =
          replicas.stream()
              .map(b -> new BrokerWrapper(b, false))
              .collect(Collectors.toCollection(ArrayList::new));
      if (!forceReplica && leader != null) {
        brokers.add(new BrokerWrapper(leader, true));
      }
    }

    LOGGER.debug("Candidates to consume from {}: {}", stream, brokers);

    return brokers;
  }

  public void close() {
    if (this.state.isClosed()) {
      return;
    }
    if (this.watchdogTask != null) {
      this.watchdogTask.cancel(false);
    }
    List<ClientSubscriptionsManager> connections =
        queryState(
            s -> {
              List<ClientSubscriptionsManager> all = new ArrayList<>(s.connections);
              s.connections.clear();
              return all;
            },
            Collections.emptyList());
    for (ClientSubscriptionsManager manager : connections) {
      try {
        manager.close();
      } catch (Exception e) {
        LOGGER.info(
            "Error while closing manager {} connected to node {}: {}",
            manager.id,
            manager.name,
            e.getMessage());
      }
    }
    try {
      this.executorServiceFactory.close();
    } catch (Exception e) {
      LOGGER.info("Error while closing executor service factory: {}", e.getMessage());
    }
    try {
      this.state.close();
      this.eventLoop.close();
    } catch (Exception e) {
      LOGGER.info("Error while closing coordinator event loop: {}", e.getMessage());
    }
    this.recoveryExecutor.shutdownNow();
    if (this.privateEventExecutorGroup) {
      closeEventExecutorGroup(this.eventExecutorGroup);
    }
  }

  private static void closeEventExecutorGroup(EventExecutorGroup group) {
    try {
      if (!group.isShuttingDown()) {
        // no quiet period: the loop is a control plane, there is no in-flight batch to drain
        group.shutdownGracefully(0, 10, SECONDS).get(10, SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.info("Error while closing coordinator event executor group: {}", e.getMessage());
    }
  }

  @Override
  public String toString() {
    List<ClientSubscriptionsManager> connections =
        queryState(s -> new ArrayList<>(s.connections), Collections.emptyList());
    StringBuilder builder = new StringBuilder("{");
    builder.append(jsonField("client_count", connections.size())).append(", ");
    builder
        .append(
            jsonField("consumer_count", connections.stream().mapToInt(m -> m.trackerCount).sum()))
        .append(",");
    builder.append(quote("clients")).append(" : [");
    builder.append(
        connections.stream()
            .map(
                m -> {
                  StringBuilder managerBuilder = new StringBuilder("{");
                  managerBuilder
                      .append(jsonField("id", m.id))
                      .append(",")
                      .append(jsonField("node", m.name))
                      .append(",")
                      .append(jsonField("consumer_count", m.trackerCount))
                      .append(",");
                  managerBuilder.append("\"subscriptions\" : [");
                  List<SubscriptionTracker> trackers = m.subscriptionTrackers;
                  managerBuilder.append(
                      IntStream.range(0, trackers.size())
                          .filter(i -> trackers.get(i) != null)
                          .mapToObj(
                              i -> {
                                SubscriptionTracker t = trackers.get(i);
                                return "{"
                                    + jsonField("stream", t.stream)
                                    + ","
                                    + jsonField("id", t.id)
                                    + ","
                                    + jsonField("subscription_id", i)
                                    + ","
                                    + jsonField("state", t.consumer.state())
                                    + "}";
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("]");
                  return managerBuilder.append("}").toString();
                })
            .collect(Collectors.joining(",")));
    builder.append("]");
    builder.append("}");
    return builder.toString();
  }

  /**
   * Data structure that keeps track of a given {@link StreamConsumer} and its message callback.
   *
   * <p>An instance is "moved" between {@link ClientSubscriptionsManager} instances on stream
   * failure or on disconnection.
   */
  private static class SubscriptionTracker {

    private final long id;
    private final String stream;
    private final OffsetSpecification initialOffsetSpecification;
    private final String offsetTrackingReference;
    private final MessageHandler messageHandler;
    private final StreamConsumer consumer;
    private final SubscriptionListener subscriptionListener;
    private final Runnable trackingClosingCallback;
    private final Map<String, String> subscriptionProperties;
    private volatile long offset;
    private volatile boolean hasReceivedSomething = false;
    // set only once a manager has confirmed the subscription with the broker, never during the
    // reserve-then-dispatch window: a concurrent cancel or supersede sees no manager yet and
    // defers to the epoch check that runs once the in-flight attempt finishes (see
    // ConsumersCoordinator.assignmentSucceeded)
    private final AtomicReference<Assignment> assignment = new AtomicReference<>(Assignment.NONE);
    private final ConsumerFlowStrategy flowStrategy;

    private SubscriptionTracker(
        long id,
        StreamConsumer consumer,
        String stream,
        OffsetSpecification initialOffsetSpecification,
        String offsetTrackingReference,
        SubscriptionListener subscriptionListener,
        Runnable trackingClosingCallback,
        MessageHandler messageHandler,
        Map<String, String> subscriptionProperties,
        ConsumerFlowStrategy flowStrategy) {
      this.id = id;
      this.consumer = consumer;
      this.stream = stream;
      this.initialOffsetSpecification = initialOffsetSpecification;
      this.offsetTrackingReference = offsetTrackingReference;
      this.subscriptionListener = subscriptionListener;
      this.trackingClosingCallback = trackingClosingCallback;
      this.messageHandler = messageHandler;
      this.flowStrategy = flowStrategy;
      if (this.offsetTrackingReference == null) {
        this.subscriptionProperties = subscriptionProperties;
      } else {
        Map<String, String> properties = new ConcurrentHashMap<>(subscriptionProperties.size() + 1);
        properties.putAll(subscriptionProperties);
        // we propagate the subscription name, used for monitoring
        properties.put("name", this.offsetTrackingReference);
        this.subscriptionProperties = Collections.unmodifiableMap(properties);
      }
    }

    void cancel() {
      // the flow of messages in the user message handler should stop, we can call the tracking
      // closing callback; with automatic offset tracking, it will store the last dispatched
      // offset
      LOGGER.debug("Calling tracking consumer closing callback (may be no-op)");
      this.trackingClosingCallback.run();
      ClientSubscriptionsManager manager = this.manager();
      if (manager != null) {
        LOGGER.debug("Removing tracker {} from manager", this.label());
        manager.remove(this);
      } else {
        LOGGER.debug("No manager to remove consumer from");
      }
    }

    ClientSubscriptionsManager manager() {
      return this.assignment.get().manager;
    }

    byte subscriptionIdInClient() {
      return this.assignment.get().subscriptionIdInClient;
    }

    void confirmAssignment(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      this.assignment.set(new Assignment(subscriptionIdInClient, manager));
      this.consumer.setSubscriptionClient(manager.client);
    }

    /**
     * Release this tracker's assignment to {@code expected}, if it is still the current one.
     *
     * <p>Idempotent by construction: of two concurrent callers (the direct call in {@link
     * #cancel()} and the async {@code releaseAssignment} effect), only the one that wins the CAS
     * gets a slot back to release; the other sees the assignment already cleared and no-ops.
     */
    int detachIfOwnedBy(ClientSubscriptionsManager expected) {
      Assignment current = this.assignment.get();
      if (current.manager != expected) {
        return -1;
      }
      if (!this.assignment.compareAndSet(current, Assignment.NONE)) {
        return -1;
      }
      // masked to an unsigned 0-255 range: a plain byte cannot serve as its own "no slot"
      // sentinel, since slot 255's byte representation (0xFF) is indistinguishable from -1
      return current.subscriptionIdInClient & 0xFF;
    }

    void detachFromManager() {
      this.assignment.set(Assignment.NONE);
      this.consumer.setSubscriptionClient(null);
    }

    private void markOpen() {
      if (this.consumer != null) {
        this.consumer.markOpen();
      }
    }

    private void markRecovering() {
      if (this.consumer != null) {
        this.consumer.markRecovering();
      }
    }

    String label() {
      return String.format(
          "[id %d, stream %s, name %s, consumer %d]",
          this.id, this.stream, this.offsetTrackingReference, this.consumer.id());
    }
  }

  /** A subscription's current manager and slot, or {@link #NONE} if it has none. */
  private static final class Assignment {

    private static final Assignment NONE = new Assignment(NO_SLOT, null);

    private final byte subscriptionIdInClient;
    private final ClientSubscriptionsManager manager;

    private Assignment(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      this.subscriptionIdInClient = subscriptionIdInClient;
      this.manager = manager;
    }
  }

  /**
   * Control-plane state owned by the event loop.
   *
   * <p>The rule this class exists to enforce: the loop is the <b>single writer</b> of the
   * connection pool, the subscription-to-connection assignment, slot allocation, connection and
   * subscription states, and epochs. The data plane stays off-loop and lock-free — the immutable
   * tracker array published through a volatile field, plus the per-message volatile writes from
   * netty threads — and {@code ConsumerUpdateListener} must never wait on the loop, because the
   * protocol requires it to answer a netty thread synchronously.
   *
   * <p>Empty for now: the state moves in when the blocking I/O moves off-loop, since the two cannot
   * be separated (see the implementation plan).
   */
  static final class CoordinatorState {

    private final NavigableSet<ClientSubscriptionsManager> connections = new TreeSet<>();
    private final Map<Long, TrackerState> subscriptions = new HashMap<>();
    // one connection creation at a time per node, so concurrent placements share the connection
    // being opened instead of each opening their own
    private final Set<String> creating = new HashSet<>();
    private final Map<String, List<CompletableFuture<Void>>> waiters = new HashMap<>();
    // broker key -> suspect-until deadline (System.nanoTime() terms); consulted lazily by
    // deprioritizeSuspects, so a stale entry just stops mattering once its TTL passes, no active
    // expiry needed
    private final Map<String, Long> suspectUntil = new HashMap<>();
  }

  /** Per-subscription control state. Read and written only by the event loop. */
  private static final class TrackerState {

    private final SubscriptionTracker tracker;
    private State state = State.OPENING;
    private long epoch = 1;
    private int attempts;
    // candidate lookups that failed in the current recovery episode. Drives the fallback from
    // replicas to the leader when forceReplica is on: it must survive across attempts, since an
    // attempt performs a single lookup and then parks
    private int failedLookups;
    // when the current attempt is due, i.e. dispatched immediately or at the end of its back-off
    // delay (see ConsumersCoordinator.trackerEvent); consulted by watchdogTick() to detect an
    // attempt that never called back
    private long nextAttemptAt;

    private TrackerState(SubscriptionTracker tracker) {
      this.tracker = tracker;
    }
  }

  private static final class MessageHandlerContext implements Context {

    private final long offset;
    private final long timestamp;
    private final long committedOffset;
    private final StreamConsumer consumer;
    private final ConsumerFlowStrategy.MessageProcessedCallback processedCallback;

    private MessageHandlerContext(
        long offset,
        long timestamp,
        long committedOffset,
        StreamConsumer consumer,
        ConsumerFlowStrategy.MessageProcessedCallback processedCallback) {
      this.offset = offset;
      this.timestamp = timestamp;
      this.committedOffset = committedOffset;
      this.consumer = consumer;
      this.processedCallback = processedCallback;
    }

    @Override
    public long offset() {
      return this.offset;
    }

    @Override
    public void storeOffset() {
      this.consumer.store(this.offset);
    }

    @Override
    public long timestamp() {
      return this.timestamp;
    }

    @Override
    public long committedChunkId() {
      return committedOffset;
    }

    public String stream() {
      return this.consumer.stream();
    }

    @Override
    public Consumer consumer() {
      return this.consumer;
    }

    @Override
    public void processed() {
      this.processedCallback.processed(this);
    }
  }

  /**
   * Maintains a set of {@link SubscriptionTracker} instances on a {@link Client}.
   *
   * <p>It dispatches inbound messages to the appropriate {@link SubscriptionTracker} and
   * re-allocates {@link SubscriptionTracker}s in case of stream unavailability or disconnection.
   */
  private class ClientSubscriptionsManager implements Comparable<ClientSubscriptionsManager> {

    private final long id;
    private final Broker node;
    private final Client client;
    // <host>:<port> (actual or advertised)
    private volatile String name;
    // trackers and tracker count must be kept in sync; the array has a single writer, the event
    // loop, so a slot picked there is never picked twice, and a slot freed there is never freed
    // while its unsubscribe RPC is still in flight (the array stays occupied until then)
    private volatile List<SubscriptionTracker> subscriptionTrackers =
        createSubscriptionTrackerList();
    private final AtomicInteger consumerIndexSequence = new AtomicInteger(0);
    private volatile int trackerCount;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean clientInitialized = new AtomicBoolean(false);

    private ClientSubscriptionsManager(
        Broker targetNode,
        List<BrokerWrapper> candidates,
        Client.ClientParameters clientParameters) {
      this.id = managerIdSequence.getAndIncrement();
      this.trackerCount = 0;
      String connectionName = connectionNamingStrategy.apply(ClientConnectionType.CONSUMER);
      ClientFactoryContext clientFactoryContext =
          new ClientFactoryContext(
              clientParameters
                  .clientProperty("connection_name", connectionName)
                  .chunkListener(chunkListener())
                  .creditNotification(creditNotification())
                  .messageListener(messageListener())
                  .messageIgnoredListener(messageIgnoredListener())
                  .shutdownListener(shutdownListener())
                  .metadataListener(metadataListener())
                  .consumerUpdateListener(consumerUpdateListener()),
              keyForNode(targetNode),
              candidates.stream().map(BrokerWrapper::broker).collect(toList()));
      this.client = clientFactory.client(clientFactoryContext);
      this.node = brokerFromClient(this.client);
      this.name = keyForNode(this.node);
      LOGGER.debug("creating subscription manager on {}", name);
      LOGGER.debug("Created consumer connection '{}'", connectionName);
      this.clientInitialized.set(true);
    }

    private ChunkListener chunkListener() {
      return (client, subscriptionId, offset, messageCount, dataSize) -> {
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        ConsumerFlowStrategy.MessageProcessedCallback processCallback;
        if (subscriptionTracker != null && subscriptionTracker.consumer.isOpen()) {
          processCallback =
              subscriptionTracker.flowStrategy.start(
                  new DefaultConsumerFlowStrategyContext(
                      subscriptionId, client, messageCount, offset));
        } else {
          LOGGER.debug(
              "Could not find stream subscription {} or subscription closing, not providing credits",
              subscriptionId & 0xFF);
          processCallback = null;
        }
        return processCallback;
      };
    }

    private CreditNotification creditNotification() {
      return (subscriptionId, responseCode) -> {
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        String stream = subscriptionTracker == null ? "?" : subscriptionTracker.stream;
        LOGGER.debug(
            "Received credit notification for subscription {} (stream '{}'): {}",
            subscriptionId & 0xFF,
            stream,
            Utils.formatConstant(responseCode));
      };
    }

    private MessageListener messageListener() {
      return (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext, message) -> {
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        if (subscriptionTracker != null) {
          subscriptionTracker.offset = offset;
          subscriptionTracker.hasReceivedSomething = true;
          subscriptionTracker.messageHandler.handle(
              new MessageHandlerContext(
                  offset,
                  chunkTimestamp,
                  committedChunkId,
                  subscriptionTracker.consumer,
                  (ConsumerFlowStrategy.MessageProcessedCallback) chunkContext),
              message);
        } else {
          LOGGER.debug(
              "Could not find stream subscription {} in manager {}, node {} for message listener",
              subscriptionId,
              this.id,
              this.name);
        }
      };
    }

    private MessageIgnoredListener messageIgnoredListener() {
      return (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        if (subscriptionTracker != null) {
          // message at the beginning of the first chunk is ignored
          // we "simulate" the processing if possible
          if (chunkContext != null) {
            MessageHandlerContext messageHandlerContext =
                new MessageHandlerContext(
                    offset,
                    chunkTimestamp,
                    committedChunkId,
                    subscriptionTracker.consumer,
                    (ConsumerFlowStrategy.MessageProcessedCallback) chunkContext);
            ((ConsumerFlowStrategy.MessageProcessedCallback) chunkContext)
                .processed(messageHandlerContext);
          }
        } else {
          LOGGER.debug(
              "Could not find stream subscription {} in manager {}, node {} for message ignored listener",
              subscriptionId,
              this.id,
              this.name);
        }
      };
    }

    private ShutdownListener shutdownListener() {
      return shutdownContext -> {
        if (this.clientInitialized.get()) {
          this.closed.set(true);
          removeFromPool(this);
        }
        if (shutdownContext.isShutdownUnexpected()) {
          LOGGER.debug(
              "Unexpected shutdown notification on subscription connection {}, notifying subscriptions",
              this.name);
          if (LOGGER.isDebugEnabled()) {
            List<SubscriptionTracker> trackers = this.subscriptionTrackers;
            long consumerCount = trackers.stream().filter(Objects::nonNull).count();
            long streamCount =
                trackers.stream().filter(Objects::nonNull).map(t -> t.stream).distinct().count();
            LOGGER.debug(
                "Subscription connection has {} consumer(s) over {} stream(s) to recover",
                consumerCount,
                streamCount);
          }
          iterate(
              this.subscriptionTrackers,
              t ->
                  trackerEvent(
                      t, recoveryBackOffDelayPolicy(), SubscriptionStateMachine::onConnectionLost));
        }
      };
    }

    private MetadataListener metadataListener() {
      return (stream, code) -> {
        LOGGER.debug(
            "Received metadata notification for '{}', stream is likely to have become unavailable",
            stream);
        // fire-and-forget: this runs on a netty I/O thread, which must never wait on the loop
        submitState(
            s -> {
              List<SubscriptionTracker> current = this.subscriptionTrackers;
              List<SubscriptionTracker> affected = new ArrayList<>();
              for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                SubscriptionTracker t = current.get(i);
                if (t != null && t.stream.equals(stream)) {
                  affected.add(t);
                }
              }
              if (affected.isEmpty()) {
                return;
              }
              List<SubscriptionTracker> updated = createSubscriptionTrackerList();
              for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                updated.set(i, current.get(i));
              }
              for (SubscriptionTracker subscription : affected) {
                LOGGER.debug(
                    "Subscription {} ({}) was at offset {} (received something? {})",
                    subscription.subscriptionIdInClient(),
                    subscription.label(),
                    subscription.offset,
                    subscription.hasReceivedSomething);
                updated.set(subscription.subscriptionIdInClient() & 0xFF, null);
              }
              this.setSubscriptionTrackers(updated);

              LOGGER.debug(
                  "Trying to move {} subscription(s) (stream '{}')", affected.size(), stream);
              iterate(
                  affected,
                  t ->
                      trackerEvent(
                          t,
                          metadataUpdateBackOffDelayPolicy(),
                          SubscriptionStateMachine::onStreamUnavailable));
              submitRecovery(this::closeIfEmpty);
            });
      };
    }

    private ConsumerUpdateListener consumerUpdateListener() {
      return (client, subscriptionId, active) -> {
        OffsetSpecification result = null;
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        if (subscriptionTracker != null) {
          if (isSac(subscriptionTracker.subscriptionProperties)) {
            result = subscriptionTracker.consumer.consumerUpdate(active);
          } else {
            LOGGER.debug(
                "Subscription {} is not a single active consumer, nothing to do.", subscriptionId);
          }
        } else {
          LOGGER.debug("Could not find stream subscription {} for consumer update", subscriptionId);
        }
        return result;
      };
    }

    private List<SubscriptionTracker> createSubscriptionTrackerList() {
      List<SubscriptionTracker> newSubscriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
      IntStream.range(0, MAX_SUBSCRIPTIONS_PER_CLIENT).forEach(i -> newSubscriptions.add(null));
      return newSubscriptions;
    }

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    void add(
        SubscriptionTracker tracker,
        OffsetSpecification offsetSpecification,
        boolean isInitialSubscription) {
      byte subscriptionId = reserveSlot(tracker);
      LOGGER.debug(
          "Subscribing to {}, requested offset specification is {}, offset tracking reference is {}, properties are {}, "
              + "subscription ID is {}, consumer {}",
          tracker.stream,
          offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification,
          tracker.offsetTrackingReference,
          tracker.subscriptionProperties,
          subscriptionId,
          tracker.consumer.id());
      try {
        String offsetTrackingReference = tracker.offsetTrackingReference;
        if (offsetTrackingReference != null) {
          checkNotClosed();
          QueryOffsetResponse queryOffsetResponse =
              Utils.callAndMaybeRetry(
                  () -> client.queryOffset(offsetTrackingReference, tracker.stream),
                  RETRY_ON_TIMEOUT,
                  "Offset query for consumer %s on stream '%s' (reference %s)",
                  tracker.consumer.id(),
                  tracker.stream,
                  offsetTrackingReference);
          if (queryOffsetResponse.isOk() && queryOffsetResponse.getOffset() != 0) {
            if (offsetSpecification != null && isInitialSubscription) {
              // subscription call (not recovery), so telling the user their offset specification
              // is ignored
              LOGGER.info(
                  "Requested offset specification {} not used in favor of stored offset found for reference {}",
                  offsetSpecification,
                  offsetTrackingReference);
            }
            LOGGER.debug(
                "Using offset {} to start consuming from {} with consumer {} " + "(instead of {})",
                queryOffsetResponse.getOffset(),
                tracker.stream,
                offsetTrackingReference,
                offsetSpecification);
            offsetSpecification = OffsetSpecification.offset(queryOffsetResponse.getOffset() + 1);
          }
        }

        offsetSpecification =
            offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification;

        // TODO consider using/emulating ConsumerUpdateListener, to have only one API, not 2
        // even when the consumer is not a SAC.
        SubscriptionContext subscriptionContext =
            new DefaultSubscriptionContext(offsetSpecification, tracker.stream);
        tracker.subscriptionListener.preSubscribe(subscriptionContext);
        LOGGER.info(
            "Computed offset specification {}, offset specification used after subscription listener {}",
            offsetSpecification,
            subscriptionContext.offsetSpecification());

        checkNotClosed();
        Client.Response subscribeResponse =
            Utils.callAndMaybeRetry(
                () ->
                    client.subscribe(
                        subscriptionId,
                        tracker.stream,
                        subscriptionContext.offsetSpecification(),
                        tracker.flowStrategy.initialCredits(),
                        tracker.subscriptionProperties),
                RETRY_ON_TIMEOUT,
                "Subscribe request for consumer %d on stream '%s'",
                tracker.consumer.id(),
                tracker.stream);
        if (subscribeResponse == null) {
          // The subscribe call returned no response: the connection was torn down
          // between the request being written and the response being read, or the
          // stream was deleted concurrently.
          if (!client.isOpen()) {
            throw new ConnectionStreamException(
                "Connection closed during subscribe on stream '" + tracker.stream + "'");
          }
          throw new StreamDoesNotExistException(tracker.stream);
        }
        if (!subscribeResponse.isOk()) {
          String message =
              "Subscription to stream "
                  + tracker.stream
                  + " failed with code "
                  + formatConstant(subscribeResponse.getResponseCode());
          LOGGER.debug(message);
          throw convertCodeToException(
              subscribeResponse.getResponseCode(), tracker.stream, () -> message);
        }
        // only confirmed now: a cancellation racing the RPCs above finds no manager yet on the
        // tracker, and defers to the epoch check that runs once this attempt finishes (see
        // ConsumersCoordinator.assignmentSucceeded)
        tracker.confirmAssignment(subscriptionId, this);
        LOGGER.debug("Subscribed to '{}'", tracker.stream);
      } catch (RuntimeException e) {
        releaseSlot(subscriptionId);
        throw e;
      }
    }

    /**
     * Reserve a slot and publish it in the tracker array, so a fast first chunk finds its tracker
     * before the subscribe RPC below even completes.
     *
     * <p>Atomic by construction: it runs on the event loop, the single writer of the array.
     */
    private byte reserveSlot(SubscriptionTracker tracker) {
      SlotReservation reservation =
          ConsumersCoordinator.this.state.query(
              s -> {
                if (this.isFull()) {
                  return SlotReservation.FULL;
                }
                if (this.isDead()) {
                  return SlotReservation.DEAD;
                }
                byte subscriptionId =
                    (byte) pickSlot(this.subscriptionTrackers, this.consumerIndexSequence);
                this.setSubscriptionTrackers(
                    update(this.subscriptionTrackers, subscriptionId, tracker));
                return SlotReservation.reserved(subscriptionId);
              });
      if (reservation.full) {
        LOGGER.debug(
            "Cannot add subscription tracker for stream '{}', manager is full", tracker.stream);
        throw new IllegalStateException("Cannot add subscription tracker, the manager is full");
      }
      if (reservation.dead) {
        LOGGER.debug(
            "Cannot add subscription tracker for stream '{}', manager is closed", tracker.stream);
        throw new IllegalStateException("Cannot add subscription tracker, the manager is closed");
      }
      return reservation.subscriptionId;
    }

    // undo a reservation that failed to subscribe: the tracker was never confirmed, so there is
    // nothing to unsubscribe, only the array slot to free. Blocking (not fire-and-forget): the
    // caller is addToManager(), off-loop, which checks isEmpty() right after this returns, so the
    // free must be visible by then
    private void releaseSlot(byte subscriptionId) {
      ConsumersCoordinator.this.state.query(
          s -> {
            this.setSubscriptionTrackers(update(this.subscriptionTrackers, subscriptionId, null));
            return null;
          });
    }

    void remove(SubscriptionTracker subscriptionTracker) {
      int slot = subscriptionTracker.detachIfOwnedBy(this);
      if (slot < 0) {
        // already removed, e.g. by the direct call in cancel() racing the async
        // releaseAssignment effect: detachIfOwnedBy() is idempotent, so this is a no-op
        return;
      }
      byte subscriptionIdInClient = (byte) slot;

      try {
        Client.Response unsubscribeResponse =
            Utils.callAndMaybeRetry(
                () -> {
                  if (client.isOpen()) {
                    return client.unsubscribe(subscriptionIdInClient);
                  } else {
                    return Client.responseOk();
                  }
                },
                RETRY_ON_TIMEOUT,
                "Unsubscribe request for consumer %d on stream '%s'",
                subscriptionTracker.consumer.id(),
                subscriptionTracker.stream);
        if (!unsubscribeResponse.isOk()) {
          LOGGER.warn(
              "Unexpected response code when unsubscribing from {}: {} (subscription ID {})",
              subscriptionTracker.stream,
              formatConstant(unsubscribeResponse.getResponseCode()),
              subscriptionIdInClient);
        }
      } catch (TimeoutStreamException e) {
        LOGGER.debug(
            "Reached timeout when trying to unsubscribe consumer {} from stream '{}'",
            subscriptionTracker.consumer.id(),
            subscriptionTracker.stream);
      }

      // the array keeps the slot occupied until now, so no new subscription can reuse the same
      // numeric ID while the unsubscribe above is in flight; freeing the slot and checking
      // emptiness happen together so a concurrent add() cannot slip in between and be torn down
      // by close()
      boolean empty =
          ConsumersCoordinator.this.state.query(
              s -> {
                this.setSubscriptionTrackers(
                    update(this.subscriptionTrackers, subscriptionIdInClient, null));
                return this.isEmpty();
              });
      if (empty) {
        this.closeIfEmpty();
      }
    }

    private List<SubscriptionTracker> update(
        List<SubscriptionTracker> original, byte index, SubscriptionTracker newValue) {
      List<SubscriptionTracker> newSubcriptions = createSubscriptionTrackerList();
      int intIndex = index & 0xFF;
      for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
        newSubcriptions.set(i, i == intIndex ? newValue : original.get(i));
      }
      return newSubcriptions;
    }

    private void setSubscriptionTrackers(List<SubscriptionTracker> trackers) {
      this.subscriptionTrackers = trackers;
      this.trackerCount = (int) this.subscriptionTrackers.stream().filter(Objects::nonNull).count();
    }

    boolean isFull() {
      return this.trackerCount == maxConsumersByConnection;
    }

    boolean isEmpty() {
      return this.trackerCount == 0;
    }

    // deliberately side-effect free: a predicate that closes a connection and mutates the pool
    // makes this class impossible to reason about, and the loop must never close inline
    boolean isDead() {
      return this.closed.get() || !this.client.isOpen();
    }

    /**
     * If this manager is currently empty, close it after a short linger delay instead of right
     * away, so a subscription landing on the same node moments later (e.g. during a rolling
     * restart) can reuse the connection instead of paying for a reconnect.
     *
     * <p>No epoch guard needed: re-checking {@link #isEmpty()} at the deferred point is enough by
     * itself. A subscription that arrived in the meantime makes it a no-op; a connection that died
     * in the meantime was already closed by the shutdown path, and {@link #close()}'s own {@code
     * closed} CAS makes a second call harmless.
     */
    void closeIfEmpty() {
      if (this.isEmpty()) {
        ConsumersCoordinator.this
            .environment
            .scheduledExecutorService()
            .schedule(
                () -> ConsumersCoordinator.this.submitRecovery(this::closeIfStillEmpty),
                IDLE_LINGER_MS,
                MILLISECONDS);
      }
    }

    // the deferred re-check scheduled by closeIfEmpty(); dispatched onto the recovery pool, not
    // run inline on the shared scheduler thread, because close() can block on network I/O
    private void closeIfStillEmpty() {
      if (this.isEmpty()) {
        this.close();
      }
    }

    void close() {
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }
      removeFromPool(this);
      LOGGER.debug("Closing consumer subscription manager on {}, id {}", this.name, this.id);
      if (this.client != null && this.client.isOpen()) {
        List<SubscriptionTracker> trackers = this.subscriptionTrackers;
        for (int i = 0; i < trackers.size(); i++) {
          SubscriptionTracker tracker = trackers.get(i);
          if (tracker != null) {
            try {
              if (this.client.isOpen() && tracker.consumer.isOpen()) {
                this.client.unsubscribe((byte) i);
              }
            } catch (Exception e) {
              // OK, moving on
              LOGGER.debug("Error while unsubscribing from {}, registration {}", tracker.stream, i);
            }
          }
        }
        submitState(s -> this.setSubscriptionTrackers(createSubscriptionTrackerList()));

        if (this.client.isOpen()) {
          this.client.close();
        }
      }
    }

    @Override
    public int compareTo(ClientSubscriptionsManager o) {
      return Long.compare(this.id, o.id);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClientSubscriptionsManager that = (ClientSubscriptionsManager) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  private static final class DefaultSubscriptionContext implements SubscriptionContext {

    private volatile OffsetSpecification offsetSpecification;
    private final String name;

    private DefaultSubscriptionContext(
        OffsetSpecification computedOffsetSpecification, String name) {
      this.offsetSpecification = computedOffsetSpecification;
      this.name = name;
    }

    @Override
    public OffsetSpecification offsetSpecification() {
      return this.offsetSpecification;
    }

    @Override
    public void offsetSpecification(OffsetSpecification offsetSpecification) {
      this.offsetSpecification = offsetSpecification;
    }

    @Override
    public String stream() {
      return this.name;
    }

    @Override
    public String toString() {
      return "SubscriptionContext{" + "offsetSpecification=" + offsetSpecification + '}';
    }
  }

  private static final Predicate<Exception> RETRY_ON_TIMEOUT =
      e -> e instanceof TimeoutStreamException;

  private static class DefaultConsumerFlowStrategyContext implements ConsumerFlowStrategy.Context {

    private final byte subscriptionId;
    private final Client client;
    private final long messageCount;
    private final long chunkId;

    private DefaultConsumerFlowStrategyContext(
        byte subscriptionId, Client client, long messageCount, long chunkId) {
      this.subscriptionId = subscriptionId;
      this.client = client;
      this.messageCount = messageCount;
      this.chunkId = chunkId;
    }

    @Override
    public void credits(int credits) {
      try {
        client.credit(subscriptionId, credits);
      } catch (Exception e) {
        LOGGER.info(
            "Error while providing {} credit(s) to subscription {}: {}",
            credits,
            subscriptionId,
            e.getMessage());
      }
    }

    @Override
    public long messageCount() {
      return this.messageCount;
    }

    @Override
    public long chunkId() {
      return this.chunkId;
    }
  }

  static <T> int pickSlot(List<T> list, AtomicInteger sequence) {
    int index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    while (list.get(index) != null) {
      index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    }
    return index;
  }

  private static List<Broker> keepReplicasIfPossible(Collection<BrokerWrapper> brokers) {
    if (brokers.size() > 1) {
      return brokers.stream()
          .filter(w -> !w.isLeader())
          .map(BrokerWrapper::broker)
          .collect(toList());
    } else {
      return brokers.stream().map(BrokerWrapper::broker).collect(toList());
    }
  }

  static Broker pickBroker(
      Function<List<Broker>, Broker> picker, Collection<BrokerWrapper> candidates) {
    return picker.apply(keepReplicasIfPossible(candidates));
  }

  /**
   * Drop candidates whose node recently failed a connection attempt, unless that would leave
   * nothing to pick from.
   *
   * <p>Pure: {@code now} is a parameter rather than read internally, so this needs no clock
   * injection to test. {@code now} and the values in {@code suspectUntil} are expected to be {@link
   * System#nanoTime()} readings; the comparison below is written as a subtraction rather than
   * {@code deadline <= now} so it stays correct across a {@code nanoTime()} wraparound.
   */
  static List<BrokerWrapper> deprioritizeSuspects(
      Collection<BrokerWrapper> candidates, Map<String, Long> suspectUntil, long now) {
    List<BrokerWrapper> notSuspect = new ArrayList<>();
    for (BrokerWrapper candidate : candidates) {
      Long deadline = suspectUntil.get(keyForNode(candidate.broker()));
      if (deadline == null || deadline - now <= 0) {
        notSuspect.add(candidate);
      }
    }
    return notSuspect.isEmpty() ? new ArrayList<>(candidates) : notSuspect;
  }

  private static void iterate(
      Collection<SubscriptionTracker> l, java.util.function.Consumer<SubscriptionTracker> c) {
    for (SubscriptionTracker tracker : l) {
      if (tracker != null) {
        c.accept(tracker);
      }
    }
  }
}

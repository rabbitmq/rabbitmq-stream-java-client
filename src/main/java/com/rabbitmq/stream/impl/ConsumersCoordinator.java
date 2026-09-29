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

import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS;
import static com.rabbitmq.stream.impl.CoordinatorUtils.NO_SLOT;
import static com.rabbitmq.stream.impl.CoordinatorUtils.SLOTS_PER_CLIENT;
import static com.rabbitmq.stream.impl.CoordinatorUtils.SlotReservation;
import static com.rabbitmq.stream.impl.CoordinatorUtils.WATCHDOG_TICK_INTERVAL_MS;
import static com.rabbitmq.stream.impl.CoordinatorUtils.backOffNanos;
import static com.rabbitmq.stream.impl.CoordinatorUtils.emptySlots;
import static com.rabbitmq.stream.impl.CoordinatorUtils.pickSlot;
import static com.rabbitmq.stream.impl.CoordinatorUtils.shouldRefreshCandidates;
import static com.rabbitmq.stream.impl.CoordinatorUtils.update;
import static com.rabbitmq.stream.impl.CoordinatorUtils.watchdogShouldReDispatch;
import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;
import static com.rabbitmq.stream.impl.Utils.AVAILABLE_PROCESSORS;
import static com.rabbitmq.stream.impl.Utils.brokerFromClient;
import static com.rabbitmq.stream.impl.Utils.convertCodeToException;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.isSac;
import static com.rabbitmq.stream.impl.Utils.jsonField;
import static com.rabbitmq.stream.impl.Utils.keyForNode;
import static com.rabbitmq.stream.impl.Utils.lock;
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
import com.rabbitmq.stream.ConsumerFlowStrategy.CreditUnit;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.SubscriptionListener.SubscriptionContext;
import com.rabbitmq.stream.impl.AgentStateMachine.State;
import com.rabbitmq.stream.impl.AgentStateMachine.TransitionResult;
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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumersCoordinator implements AutoCloseable {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = SLOTS_PER_CLIENT;
  static final int MAX_ATTEMPT_BEFORE_FALLING_BACK_TO_LEADER = 5;
  private static final int RECOVERY_THREADS = Math.max(2, Math.min(4, AVAILABLE_PROCESSORS));
  private static final long FIRST_ATTEMPT_EPOCH = 1;
  // how long a node that just failed a connection attempt is deprioritized for new placements;
  // short enough that a node which has actually come back is not avoided for long
  private static final long SUSPECT_TTL_NANOS = SECONDS.toNanos(5);
  // how long an emptied connection is kept around before actually closing it, so a subscription
  // landing on the same node moments later (e.g. during a rolling restart) can reuse it instead
  // of reconnecting. Has to outlast the recovery back-off delay (5s by default), since a
  // redistributed subscription only comes back once its first attempt is due, but not by much: the
  // connection is held idle for the whole window and the cost of being wrong is one reconnect
  private static final long IDLE_LINGER_MS = SECONDS.toMillis(6);
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
    Assignment assignment;
    try {
      assignment =
          addToManager(
              newNode,
              candidates,
              subscriptionTracker,
              offsetSpecification,
              true,
              FIRST_ATTEMPT_EPOCH);
    } catch (RuntimeException e) {
      // the initial subscription does not retry, the failure goes back to the caller
      trackerEvent(
          subscriptionTracker,
          recoveryBackOffDelayPolicy(),
          (st, epoch) -> AgentStateMachine.onAssignmentFailed(st, epoch, epoch, e, false));
      throw publicException(e);
    }
    RuntimeException invalidation =
        completeInitialAssignment(subscriptionTracker, assignment, recoveryBackOffDelayPolicy());
    if (invalidation != null) {
      throw publicException(invalidation);
    }

    return () -> {
      // cancel() first, synchronously: if the assignment is already confirmed, this is the only
      // remover in the race and always wins it. Posting onCancelled afterward means its async
      // releaseAssignment effect finds nothing left to do in that case. An assignment still being
      // established is not the tracker's yet: its success, found stale, releases it
      subscriptionTracker.cancel();
      trackerEvent(
          subscriptionTracker, recoveryBackOffDelayPolicy(), AgentStateMachine::onCancelled);
    };
  }

  private static RuntimeException publicException(RuntimeException e) {
    if (e instanceof ConnectionStreamException) {
      // these exceptions are not public
      return new StreamException(e.getMessage());
    }
    return e;
  }

  private Assignment addToManager(
      Broker node,
      List<BrokerWrapper> candidates,
      SubscriptionTracker tracker,
      OffsetSpecification offsetSpecification,
      boolean isInitialSubscription,
      long attemptEpoch) {
    ClientParameters clientParameters =
        environment
            .clientParametersCopy()
            .executorServiceFactory(this.executorServiceFactory)
            .host(node.getHost())
            .port(node.getPort());
    LOGGER.debug("Finding a manager for consumer {}", tracker.consumer.id());
    while (true) {
      ConnectionPool.Placement<ClientSubscriptionsManager> placement = placement(node);
      if (placement.waitFor() != null) {
        // a connection to this node is being opened, share it instead of opening another one
        ConnectionPool.awaitCreation(
            placement.waitFor(), this.environment.rpcTimeout(), "consumer");
        continue;
      }
      ClientSubscriptionsManager pickedManager = placement.connection();
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
        Assignment assignment =
            pickedManager.add(tracker, offsetSpecification, isInitialSubscription, attemptEpoch);
        LOGGER.debug(
            "Assigned tracker {} to manager {} (node {}), subscription ID {}, consumer {}",
            tracker.label(),
            pickedManager.id,
            pickedManager.name,
            assignment.slot(),
            tracker.consumer.id());
        return assignment;
      } catch (IllegalStateException e) {
        // full or closed in the meantime, pick again
      } catch (RuntimeException e) {
        if (shouldRefreshCandidates(e)) {
          // manager connection is dead or stream not available: deprioritize this node for new
          // placements for a short while, so a subscription being redistributed does not keep
          // landing back on a node that is mid-restart
          state.submitIfOpen(
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
  private ConnectionPool.Placement<ClientSubscriptionsManager> placement(Broker node) {
    return this.state.query(s -> s.pool.placement(node, m -> !m.isFull()));
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
    state.submitIfOpen(s -> s.pool.creationFinished(node, manager));
  }

  private void registerSubscription(SubscriptionTracker tracker) {
    state.submitIfOpen(s -> s.subscriptions.put(tracker.id, new TrackerState(tracker)));
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
    state.submitIfOpen(
        s -> applyTransition(s, tracker, delayPolicy, beforeDecision, decision, null));
  }

  /**
   * @param assignment the assignment established by the attempt the event comes from, or null if it
   *     does not come from a successful attempt
   */
  // loop only
  private void applyTransition(
      CoordinatorState s,
      SubscriptionTracker tracker,
      BackOffDelayPolicy delayPolicy,
      java.util.function.Consumer<TrackerState> beforeDecision,
      BiFunction<State, Long, TransitionResult> decision,
      Assignment assignment) {
    TrackerState trackerState = s.subscriptions.get(tracker.id);
    if (trackerState == null) {
      if (assignment != null) {
        // the subscription is over, and the attempt's assignment was never published for its
        // cancellation to find
        submitRecovery(() -> assignment.manager.release(tracker, assignment));
      }
      return;
    }
    beforeDecision.accept(trackerState);
    TransitionResult result = decision.apply(trackerState.state, trackerState.epoch);
    boolean newAttempt = result.state() == State.RECOVERING && result.epoch() != trackerState.epoch;
    trackerState.state = result.state();
    trackerState.epoch = result.epoch();
    // 0-based, as BackOffDelayPolicy defines it (see AsyncRetry): delay(0) is the wait before
    // an episode's first attempt, delay(n) the wait after n failed ones. Read before the
    // increment and passed to the effect, so the deadline below and the delay
    // scheduleAssignment applies come from the same index
    int backOffIndex = trackerState.attempts;
    if (newAttempt) {
      trackerState.attempts++;
      // the attempt is either scheduled after its back-off delay or, for a watchdog rescue,
      // dispatched right away, and which one is only decided in the effect. Assume the delay
      // applies, so the watchdog measures "stuck" from the point the attempt is due at the
      // latest and never cuts short a configured back-off
      trackerState.nextAttemptAt = System.nanoTime() + backOffNanos(delayPolicy, backOffIndex);
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
              tracker, delayPolicy, backOffIndex, trackerState.failedLookups, assignment);
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
   * {@link AgentStateMachine}).
   *
   * <p>Package-protected for testing: a test can call this directly instead of waiting out the real
   * tick interval.
   */
  void watchdogTick() {
    state.submitIfOpen(
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
                  AgentStateMachine::onCancelled);
            } else {
              trackerEvent(
                  trackerState.tracker,
                  recoveryBackOffDelayPolicy(),
                  (st, epoch) -> AgentStateMachine.onWatchdogTick(st, epoch, attemptEpoch));
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
      SubscriptionTracker tracker,
      Assignment assignment,
      BackOffDelayPolicy delayPolicy,
      long attemptEpoch) {
    state.submitIfOpen(
        s ->
            applyTransition(
                s,
                tracker,
                delayPolicy,
                NO_STATE_CHANGE,
                successDecision(tracker, assignment, attemptEpoch, null),
                assignment));
  }

  /**
   * The initial subscription's success, applied synchronously so the subscribing thread can fail if
   * the assignment is already gone.
   *
   * @return the reason the assignment is no longer valid, or null
   */
  private RuntimeException completeInitialAssignment(
      SubscriptionTracker tracker, Assignment assignment, BackOffDelayPolicy delayPolicy) {
    AtomicReference<RuntimeException> invalidation = new AtomicReference<>();
    // not run if the coordinator is closing: no failure to report then
    state.queryIfOpen(
        s -> {
          applyTransition(
              s,
              tracker,
              delayPolicy,
              NO_STATE_CHANGE,
              successDecision(tracker, assignment, FIRST_ATTEMPT_EPOCH, invalidation),
              assignment);
          return null;
        },
        null);
    return invalidation.get();
  }

  private BiFunction<State, Long, TransitionResult> successDecision(
      SubscriptionTracker tracker,
      Assignment assignment,
      long attemptEpoch,
      AtomicReference<RuntimeException> invalidationHolder) {
    return (st, epoch) -> {
      if (AgentStateMachine.isStale(epoch, attemptEpoch) || st.terminal()) {
        return AgentStateMachine.onAssignmentSucceeded(st, epoch, attemptEpoch);
      }
      ClientSubscriptionsManager manager = assignment.manager;
      // published before the validation, not after: see ClientSubscriptionsManager.invalidation
      tracker.publish(assignment);
      RuntimeException invalidation = manager.invalidation(tracker, assignment);
      if (invalidation == null) {
        // a plain field write for a non-null client, fine on the loop
        tracker.consumer.setSubscriptionClient(manager.client);
        return AgentStateMachine.onAssignmentSucceeded(st, epoch, attemptEpoch);
      }
      tracker.unpublish(assignment);
      LOGGER.debug(
          "Assignment of subscription {} is already gone: {}",
          tracker.label(),
          Utils.exceptionMessage(invalidation));
      if (invalidationHolder != null) {
        invalidationHolder.set(invalidation);
      }
      return AgentStateMachine.onAssignmentInvalidated(st, epoch, attemptEpoch, invalidation);
    };
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
            AgentStateMachine.onAssignmentFailed(st, epoch, attemptEpoch, cause, recoverable));
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
        (st, epoch) -> AgentStateMachine.onAssignmentFailed(st, epoch, attemptEpoch, cause, true));
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
      trackerEvent(tracker, delayPolicy, AgentStateMachine::onCancelled);
      return;
    }
    if (superseded(tracker, attemptEpoch)) {
      // typically an attempt that waited out its back-off delay while newer events took over
      LOGGER.debug("Skipping superseded assignment attempt for subscription {}", tracker.label());
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
      if (superseded(tracker, attemptEpoch)) {
        // re-checked after the lookup, the slow part of an attempt, and as late as possible before
        // the broker gets involved
        LOGGER.debug("Not assigning superseded attempt for subscription {}", tracker.label());
        return;
      }
      Assignment assignment =
          addToManager(broker, candidates, tracker, offsetSpecification, false, attemptEpoch);
      assignmentSucceeded(tracker, assignment, delayPolicy, attemptEpoch);
    } catch (Exception e) {
      LOGGER.debug(
          "Error while assigning subscription {}: {}", tracker.label(), Utils.exceptionMessage(e));
      assignmentFailed(tracker, delayPolicy, attemptEpoch, e, recoverable(e));
    }
  }

  /**
   * Whether a failed assignment is worth another attempt, mirroring the classification the blocking
   * recovery loop performed.
   */
  static boolean recoverable(Throwable cause) {
    if (cause == null) {
      return false;
    }
    if (shouldRefreshCandidates(cause)) {
      return true;
    }
    if (cause instanceof StreamException) {
      short code = ((StreamException) cause).getCode();
      return code == RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS;
    }
    return false;
  }

  /**
   * Whether an attempt has been superseded, and so must not touch the broker.
   *
   * <p>A superseded attempt that subscribes anyway is undone by the {@code releaseAssignment}
   * effect of its stale success, but only once the broker has already started delivering to it,
   * which the application sees as duplicate messages. The event that superseded it always started
   * an attempt of its own, so giving up here does not cost the subscription its recovery.
   */
  private boolean superseded(SubscriptionTracker tracker, long attemptEpoch) {
    Boolean superseded =
        this.state.query(
            s -> {
              TrackerState trackerState = s.subscriptions.get(tracker.id);
              // gone from the map: the subscription reached a terminal state, so there is nothing
              // left to assign either
              return trackerState == null
                  || AgentStateMachine.isStale(trackerState.epoch, attemptEpoch);
            });
    // null when the loop did not run the query at all, which means the coordinator is closing:
    // there is no state left to be current with, so the attempt stops here as well
    return superseded == null || superseded;
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
  private final class TrackerActions implements AgentStateMachine.Actions {

    private final SubscriptionTracker tracker;
    private final BackOffDelayPolicy delayPolicy;
    // the number of attempts already made in this recovery episode, which is also the index this
    // attempt's delay comes from
    private final int backOffIndex;
    private final int failedLookups;
    // the assignment of the attempt a success comes from, null for any other event
    private final Assignment assignment;

    private TrackerActions(
        SubscriptionTracker tracker,
        BackOffDelayPolicy delayPolicy,
        int backOffIndex,
        int failedLookups,
        Assignment assignment) {
      this.tracker = tracker;
      this.delayPolicy = delayPolicy;
      this.backOffIndex = backOffIndex;
      this.failedLookups = failedLookups;
      this.assignment = assignment;
    }

    @Override
    public void dispatchAssignment(long attemptEpoch) {
      assign(this.tracker, attemptEpoch, this.delayPolicy, this.failedLookups);
    }

    @Override
    public void scheduleAssignment(long attemptEpoch, Throwable cause) {
      Duration delay = this.delayPolicy.delay(this.backOffIndex);
      if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
        LOGGER.debug(
            "Giving up on subscription {} after {} attempt(s)",
            this.tracker.label(),
            this.backOffIndex);
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
    public void closeAfterStreamDeletion(Throwable cause) {
      try {
        this.tracker.consumer.closeAfterStreamDeletion();
      } catch (Exception e) {
        LOGGER.debug("Error while closing consumer: {}", e.getMessage());
      }
    }

    @Override
    public void releaseAssignment() {
      // exactly the attempt's own for a success, which may never have been published, the current
      // one otherwise
      Assignment toRelease = this.assignment == null ? this.tracker.assignment() : this.assignment;
      if (toRelease.manager != null) {
        toRelease.manager.release(this.tracker, toRelease);
      }
    }
  }

  int managerCount() {
    return state.queryIfOpen(s -> s.pool.size(), 0);
  }

  // the connection pool is coordinator-owned state, so managers do not reach into it directly.
  // step 4 of the redesign replaces this call with an event posted to the event loop
  private void removeFromPool(ClientSubscriptionsManager manager) {
    // fire-and-forget: this is called from netty I/O threads, which must never wait on the loop
    state.submitIfOpen(s -> s.pool.remove(manager));
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
        state.queryIfOpen(s -> s.pool.drain(), Collections.emptyList());
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
      CoordinatorUtils.closeEventExecutorGroup(this.eventExecutorGroup);
    }
  }

  @Override
  public String toString() {
    List<ClientSubscriptionsManager> connections =
        state.queryIfOpen(s -> s.pool.connections(), Collections.emptyList());
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
    // an attempt carries the assignment it establishes to the event loop instead of writing it
    // here: only the loop publishes one, when it applies a current, valid success. Cleared by
    // releases and by the markRecovering effect
    private final AtomicReference<Assignment> assignment = new AtomicReference<>(Assignment.NONE);
    private final ConsumerFlowStrategy flowStrategy;
    private final CreditAccountant creditAccountant;

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
      this.creditAccountant =
          flowStrategy.unit() == CreditUnit.BYTE
              ? new ByteCreditAccountant()
              : ChunkCreditAccountant.INSTANCE;
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
      Assignment current = this.assignment.get();
      if (current.manager != null) {
        LOGGER.debug("Removing tracker {} from manager", this.label());
        current.manager.release(this, current);
      } else {
        LOGGER.debug("No manager to remove consumer from");
      }
    }

    Assignment assignment() {
      return this.assignment.get();
    }

    // loop only
    void publish(Assignment assignment) {
      this.assignment.set(assignment);
    }

    boolean unpublish(Assignment expected) {
      return this.assignment.compareAndSet(expected, Assignment.NONE);
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

  /**
   * The manager and slot an attempt established, or {@link #NONE}.
   *
   * <p>Compared by identity: two attempts landing on the same manager make two assignments.
   */
  private static final class Assignment {

    private static final Assignment NONE = new Assignment(NO_SLOT, null);

    private final byte subscriptionIdInClient;
    private final ClientSubscriptionsManager manager;
    private final AtomicBoolean released = new AtomicBoolean(false);

    private Assignment(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      this.subscriptionIdInClient = subscriptionIdInClient;
      this.manager = manager;
    }

    // masked to an unsigned 0-255 range: slot 255's byte representation is the same as NO_SLOT's
    private int slot() {
      return this.subscriptionIdInClient & 0xFF;
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

    private final ConnectionPool<ClientSubscriptionsManager> pool = new ConnectionPool<>();
    private final Map<Long, TrackerState> subscriptions = new HashMap<>();
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
  private class ClientSubscriptionsManager
      implements ConnectionPool.PooledConnection, Comparable<ClientSubscriptionsManager> {

    private final long id;
    private final Broker node;
    private final Client client;
    // <host>:<port> (actual or advertised)
    private volatile String name;
    // trackers and tracker count must be kept in sync; the array has a single writer, the event
    // loop, so a slot picked there is never picked twice, and a slot freed there is never freed
    // while its unsubscribe RPC is still in flight (the array stays occupied until then)
    private volatile List<SubscriptionTracker> subscriptionTrackers = emptySlots();
    private final AtomicInteger consumerIndexSequence = new AtomicInteger(0);
    // loop only: subscriptions whose attempt was in flight here when their stream became
    // unavailable. Their assignment must not become active, but their slot stays theirs until the
    // attempt releases it, so its ID cannot be reused while its subscribe or unsubscribe is in
    // flight
    private final Set<SubscriptionTracker> poisoned =
        Collections.newSetFromMap(new IdentityHashMap<>());
    // loop only: the epoch of the attempt that reserved each slot
    private final long[] slotEpochs = new long[MAX_SUBSCRIPTIONS_PER_CLIENT];
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
      return (client, subscriptionId, offset, messageCount, dataSize, chunkByteCount) -> {
        SubscriptionTracker subscriptionTracker = subscriptionTrackers.get(subscriptionId & 0xFF);
        ConsumerFlowStrategy.MessageProcessedCallback processCallback;
        if (subscriptionTracker != null && subscriptionTracker.consumer.isOpen()) {
          subscriptionTracker.creditAccountant.chunkArrived(client, subscriptionId, chunkByteCount);
          processCallback =
              subscriptionTracker.flowStrategy.start(
                  new DefaultConsumerFlowStrategyContext(
                      subscriptionId,
                      client,
                      messageCount,
                      offset,
                      chunkByteCount,
                      subscriptionTracker.creditAccountant));
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
        if (responseCode == Constants.RESPONSE_CODE_PRECONDITION_FAILED) {
          // a unit mismatch between the subscription and the credit frame, necessarily a
          // client bug; the credit was dropped, so the subscription is short of credit for
          // good
          LOGGER.warn(
              "Received credit notification for subscription {} (stream '{}'): {}",
              subscriptionId & 0xFF,
              stream,
              Utils.formatConstant(responseCode));
        } else {
          LOGGER.debug(
              "Received credit notification for subscription {} (stream '{}'): {}",
              subscriptionId & 0xFF,
              stream,
              Utils.formatConstant(responseCode));
        }
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
          // on the loop, which knows each subscription's current attempt: a slot reserved by a
          // superseded attempt belongs to that attempt, which either fails with the connection or
          // has its success found stale, and must not disrupt the subscription's current
          // assignment on another connection
          state.submitIfOpen(
              s -> {
                Set<SubscriptionTracker> affected =
                    Collections.newSetFromMap(new IdentityHashMap<>());
                List<SubscriptionTracker> trackers = this.subscriptionTrackers;
                for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                  SubscriptionTracker t = trackers.get(i);
                  if (t != null && this.isCurrentAttempt(s, t, i)) {
                    affected.add(t);
                  }
                }
                affected.forEach(
                    t ->
                        applyTransition(
                            s,
                            t,
                            recoveryBackOffDelayPolicy(),
                            NO_STATE_CHANGE,
                            AgentStateMachine::onConnectionLost,
                            null));
              });
        }
      };
    }

    private MetadataListener metadataListener() {
      return (stream, code) -> {
        LOGGER.debug(
            "Received metadata notification for '{}', stream is likely to have become unavailable",
            stream);
        // fire-and-forget: this runs on a netty I/O thread, which must never wait on the loop
        state.submitIfOpen(
            s -> {
              List<SubscriptionTracker> current = this.subscriptionTrackers;
              List<SubscriptionTracker> updated = emptySlots();
              List<SubscriptionTracker> affected = new ArrayList<>();
              for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                SubscriptionTracker t = current.get(i);
                updated.set(i, t);
                if (t != null && t.stream.equals(stream)) {
                  affected.add(t);
                  if (this.isLiveAssignment(s, t, i)) {
                    LOGGER.debug(
                        "Subscription {} ({}) was at offset {} (received something? {})",
                        i,
                        t.label(),
                        t.offset,
                        t.hasReceivedSomething);
                    updated.set(i, null);
                  } else {
                    // an attempt is in flight in this slot: it releases the slot itself, freeing
                    // it here would let it be reused while the attempt's subscribe or
                    // unsubscribe for this ID is still in flight
                    this.poisoned.add(t);
                  }
                }
              }
              if (affected.isEmpty()) {
                return;
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
                          AgentStateMachine::onStreamUnavailable));
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

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    /**
     * Establish an assignment for the attempt of a subscription, without making it the
     * subscription's: that is up to the loop, once it knows the attempt is still the current one.
     */
    Assignment add(
        SubscriptionTracker tracker,
        OffsetSpecification offsetSpecification,
<<<<<<< HEAD
        boolean isInitialSubscription,
        long attemptEpoch) {
      byte subscriptionId = reserveSlot(tracker, attemptEpoch);
=======
        boolean isInitialSubscription) {
      if (tracker.flowStrategy.unit() == CreditUnit.BYTE && !this.client.byteCreditSupported()) {
        // must not be an IllegalStateException: addToManager treats that as "this manager
        // cannot take the subscription" and loops looking for another one, which would spin
        // forever on a node that will never support Subscribe/Credit version 2
        throw new StreamException(
            "Byte-based consumer credit requires a broker supporting Subscribe version 2 "
                + "and Credit version 2");
      }

      byte subscriptionId = reserveSlot(tracker);
>>>>>>> 95cc1e0976 (Fix conflicts after consumer coordinator refactoring)
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
        int initialCredits = tracker.flowStrategy.initialCredits();
        // resetting on every subscription, including recovery, keeps the mirror correct
        // after a reconnection or a stream move
        tracker.creditAccountant.reset(initialCredits);
        Client.Response subscribeResponse =
            Utils.callAndMaybeRetry(
                () ->
                    client.subscribe(
                        subscriptionId,
                        tracker.stream,
                        subscriptionContext.offsetSpecification(),
                        initialCredits,
                        tracker.subscriptionProperties,
                        tracker.flowStrategy.unit()),
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
        LOGGER.debug("Subscribed to '{}'", tracker.stream);
        return new Assignment(subscriptionId, this);
      } catch (RuntimeException e) {
        releaseSlot(subscriptionId, tracker);
        throw e;
      }
    }

    /**
     * Reserve a slot and publish it in the tracker array, so a fast first chunk finds its tracker
     * before the subscribe RPC below even completes.
     *
     * <p>Atomic by construction: it runs on the event loop, the single writer of the array.
     */
    private byte reserveSlot(SubscriptionTracker tracker, long attemptEpoch) {
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
                this.slotEpochs[subscriptionId & 0xFF] = attemptEpoch;
                // the poison is for an older attempt, which is stale by now, and its release can
                // lag behind this attempt's validation
                this.poisoned.remove(tracker);
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
      return reservation.slot;
    }

    // undo a reservation that failed to subscribe: the tracker was never confirmed, so there is
    // nothing to unsubscribe, only the array slot to free. Blocking (not fire-and-forget): the
    // caller is addToManager(), off-loop, which checks isEmpty() right after this returns, so the
    // free must be visible by then
    private void releaseSlot(byte subscriptionId, SubscriptionTracker tracker) {
      ConsumersCoordinator.this.state.query(
          s -> {
            this.freeSlot(subscriptionId & 0xFF, tracker);
            return null;
          });
    }

    // loop only; only if the slot still holds this tracker: it may have been freed and reused
    // since, and nulling it would silently cut the new owner off
    private void freeSlot(int slot, SubscriptionTracker tracker) {
      if (this.subscriptionTrackers.get(slot) == tracker) {
        this.setSubscriptionTrackers(update(this.subscriptionTrackers, (byte) slot, null));
      }
      this.poisoned.remove(tracker);
    }

    /**
     * Loop only: why an assignment made here by the subscription's current attempt must not become
     * active, or null if it still stands.
     *
     * <p>Sound for the connection case without any lock: the shutdown listener sets {@code closed}
     * and then posts the loop task that reads the slots, the loop publishes the assignment before
     * this reads {@code closed}. Either this sees the connection dead, or the listener's task runs
     * after this one, and finds the published assignment, whose attempt is the current one. The
     * metadata listener's loop task runs before the success event it races with, since the listener
     * posts it before the attempt returns.
     */
    private RuntimeException invalidation(SubscriptionTracker tracker, Assignment assignment) {
      if (this.isDead()) {
        return new ClientClosedException();
      }
      if (this.poisoned.contains(tracker)
          || this.subscriptionTrackers.get(assignment.slot()) != tracker) {
        return new StreamNotAvailableException(tracker.stream);
      }
      return null;
    }

    // loop only: the slot was reserved by the subscription's current attempt, which includes its
    // published assignment while it is active
    private boolean isCurrentAttempt(CoordinatorState s, SubscriptionTracker tracker, int slot) {
      TrackerState trackerState = s.subscriptions.get(tracker.id);
      return trackerState != null && this.slotEpochs[slot] == trackerState.epoch;
    }

    // loop only: the tracker is active and its published assignment is this very slot, as opposed
    // to a reservation of an attempt still in flight
    private boolean isLiveAssignment(CoordinatorState s, SubscriptionTracker tracker, int slot) {
      TrackerState trackerState = s.subscriptions.get(tracker.id);
      // a single read: a release can clear it concurrently, off-loop
      Assignment assignment = tracker.assignment.get();
      return trackerState != null
          && trackerState.state == State.ACTIVE
          && assignment.manager == this
          && assignment.slot() == slot;
    }

    /**
     * Release exactly the given assignment of the subscription: its broker subscription and its
     * slot, and the subscription's assignment if it is still this one. At most once per assignment,
     * so the direct call in {@link SubscriptionTracker#cancel()} and the {@code releaseAssignment}
     * effect cannot both unsubscribe it.
     */
    void release(SubscriptionTracker subscriptionTracker, Assignment assignment) {
      if (!assignment.released.compareAndSet(false, true)) {
        return;
      }
      subscriptionTracker.unpublish(assignment);
      int slot = assignment.slot();
      byte subscriptionIdInClient = assignment.subscriptionIdInClient;
      // not if the slot is no longer the subscription's, e.g. freed by a metadata update: its ID
      // may belong to another subscription by now
      boolean held =
          ConsumersCoordinator.this.state.query(
              s -> this.subscriptionTrackers.get(slot) == subscriptionTracker);
      if (held) {
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
      }

      // the array keeps the slot occupied until now, so no new subscription can reuse the same
      // numeric ID while the unsubscribe above is in flight; freeing the slot and checking
      // emptiness happen together so a concurrent add() cannot slip in between and be torn down
      // by close()
      boolean empty =
          ConsumersCoordinator.this.state.query(
              s -> {
                this.freeSlot(slot, subscriptionTracker);
                return this.isEmpty();
              });
      if (empty) {
        this.closeIfEmpty();
      }
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

    @Override
    public Broker node() {
      return this.node;
    }

    // deliberately side-effect free: a predicate that closes a connection and mutates the pool
    // makes this class impossible to reason about, and the loop must never close inline
    @Override
    public boolean isDead() {
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
        state.submitIfOpen(s -> this.setSubscriptionTrackers(emptySlots()));

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
    private final long chunkByteCount;
    private final CreditAccountant creditAccountant;
    // guards against releasing this chunk's credit more than once, no matter how many times
    // the strategy calls credits(...) for it
    private final AtomicBoolean released = new AtomicBoolean(false);

    private DefaultConsumerFlowStrategyContext(
        byte subscriptionId,
        Client client,
        long messageCount,
        long chunkId,
        long chunkByteCount,
        CreditAccountant creditAccountant) {
      this.subscriptionId = subscriptionId;
      this.client = client;
      this.messageCount = messageCount;
      this.chunkId = chunkId;
      this.chunkByteCount = chunkByteCount;
      this.creditAccountant = creditAccountant;
    }

    @Override
    public void credits(int credits) {
      if (!this.released.compareAndSet(false, true)) {
        LOGGER.debug(
            "Credit already released for subscription {}, chunk {}, ignoring extra call",
            subscriptionId,
            chunkId);
        return;
      }
      try {
        this.creditAccountant.release(client, subscriptionId, credits, chunkByteCount);
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

    @Override
    public long chunkByteCount() {
      return this.chunkByteCount;
    }
  }

<<<<<<< HEAD
=======
  /**
   * Grants credit for a subscription.
   *
   * <p>{@link #chunkArrived(Client, byte, long)} is called once per chunk, before the chunk's
   * {@link ConsumerFlowStrategy} context is created. {@link #release(Client, byte, int, long)} is
   * called at most once per chunk, from that chunk's flow strategy context.
   */
  interface CreditAccountant {

    /**
     * Called before subscription.
     *
     * @param initialCredits
     */
    void reset(int initialCredits);

    /**
     * Called on chunk arrival.
     *
     * @param client
     * @param subscriptionId
     * @param chunkByteCount
     */
    void chunkArrived(Client client, byte subscriptionId, long chunkByteCount);

    /**
     * Called when the flow strategy provide credits via the default context.
     *
     * @param client
     * @param subscriptionId
     * @param chunks
     * @param chunkByteCount
     */
    void release(Client client, byte subscriptionId, int chunks, long chunkByteCount);
  }

  /** Chunk-based credit: a pass-through to {@link Client#credit(byte, int)}. */
  static final class ChunkCreditAccountant implements CreditAccountant {

    static final CreditAccountant INSTANCE = new ChunkCreditAccountant();

    @Override
    public void reset(int initialCredits) {}

    @Override
    public void chunkArrived(Client client, byte subscriptionId, long chunkByteCount) {}

    @Override
    public void release(Client client, byte subscriptionId, int chunks, long chunkByteCount) {
      client.credit(subscriptionId, chunks);
    }
  }

  /**
   * Byte-based credit: keeps an exact mirror of the broker-side credit for a subscription, and
   * batches grants instead of sending one {@code Credit} frame per released chunk.
   *
   * <p>{@code credit = window + granted - received}, so it only ever decreases in {@link
   * #chunkArrived(Client, byte, long)} and only ever increases when a grant is flushed. A grant is
   * flushed once {@code credit} drops to {@code flushThreshold}, three quarters of the window,
   * deliberately above the broker's {@code send_limit} (half the window, see {@code
   * rabbit_stream_reader:send_chunks/6}): the client provably owes nothing by the time the broker
   * can become blocked, so no timer or further delivery is needed to get the grant out.
   */
  static final class ByteCreditAccountant implements CreditAccountant {

    // chunks arrive on the connection dispatching thread, processed() can be called from any
    // application thread
    private final Lock lock = new ReentrantLock();
    private long window;
    private long flushThreshold;
    private long pending;
    private long credit;

    @Override
    public void reset(int initialCredits) {
      lock(
          this.lock,
          () -> {
            this.window = initialCredits;
            this.flushThreshold = this.window - this.window / 4;
            this.credit = this.window;
            this.pending = 0;
          });
    }

    @Override
    public void chunkArrived(Client client, byte subscriptionId, long chunkByteCount) {
      long toGrant;
      this.lock.lock();
      try {
        this.credit -= chunkByteCount;
        toGrant = maybeFlushLocked();
      } finally {
        this.lock.unlock();
      }
      grant(client, subscriptionId, toGrant);
    }

    @Override
    public void release(Client client, byte subscriptionId, int chunks, long chunkByteCount) {
      if (chunks != 1) {
        LOGGER.debug(
            "Byte-based credit release called with {} chunk(s) instead of 1, "
                + "ignoring the chunk count",
            chunks);
      }
      long toGrant;
      this.lock.lock();
      try {
        this.pending += chunkByteCount;
        toGrant = maybeFlushLocked();
      } finally {
        this.lock.unlock();
      }
      grant(client, subscriptionId, toGrant);
    }

    // must be called with the lock held
    private long maybeFlushLocked() {
      if (this.pending > 0 && this.credit <= this.flushThreshold) {
        long toGrant = this.pending;
        this.credit += this.pending;
        this.pending = 0;
        return toGrant;
      }
      return 0;
    }

    // the Credit frame is written outside the lock, concurrent grants are additive so their
    // order does not matter
    private static void grant(Client client, byte subscriptionId, long credit) {
      if (credit > 0) {
        client.credit(subscriptionId, (int) credit, CreditUnit.BYTE);
      }
    }

    // for tests
    long credit() {
      this.lock.lock();
      try {
        return this.credit;
      } finally {
        this.lock.unlock();
      }
    }

    // for tests
    long pending() {
      this.lock.lock();
      try {
        return this.pending;
      } finally {
        this.lock.unlock();
      }
    }
  }

  static <T> int pickSlot(List<T> list, AtomicInteger sequence) {
    int index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    while (list.get(index) != null) {
      index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    }
    return index;
  }

>>>>>>> 3bf5953df6 (Implement ByteCreditAccountant)
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

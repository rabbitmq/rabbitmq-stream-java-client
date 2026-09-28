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

import static com.rabbitmq.stream.impl.CoordinatorUtils.ClientClosedException;
import static com.rabbitmq.stream.impl.CoordinatorUtils.shouldRefreshCandidates;
import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;
import static com.rabbitmq.stream.impl.Utils.AVAILABLE_PROCESSORS;
import static com.rabbitmq.stream.impl.Utils.callAndMaybeRetry;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.jsonField;
import static com.rabbitmq.stream.impl.Utils.keyForNode;
import static com.rabbitmq.stream.impl.Utils.namedFunction;
import static com.rabbitmq.stream.impl.Utils.quote;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.AgentStateMachine.State;
import com.rabbitmq.stream.impl.AgentStateMachine.TransitionResult;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.MetadataListener;
import com.rabbitmq.stream.impl.Client.PublishConfirmListener;
import com.rabbitmq.stream.impl.Client.PublishErrorListener;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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

final class ProducersCoordinator implements AutoCloseable {

  static final int MAX_PRODUCERS_PER_CLIENT = 256;
  static final int MAX_TRACKING_CONSUMERS_PER_CLIENT = 50;
  private static final int RECOVERY_THREADS = Math.max(2, Math.min(4, AVAILABLE_PROCESSORS));
  private static final long FIRST_ATTEMPT_EPOCH = 1;
  // sentinel publisher ID: no slot reserved, or a reservation that was rolled back or freed
  private static final byte NO_SLOT = -1;
  private static final boolean DEBUG = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);
  private final StreamEnvironment environment;
  private final ClientFactory clientFactory;
  private final int maxProducersByClient, maxTrackingConsumersByClient;
  private final Function<ClientConnectionType, String> connectionNamingStrategy;
  private final AtomicLong managerIdSequence = new AtomicLong(0);
  private final AtomicLong trackerIdSequence = new AtomicLong(0);
  private final List<ProducerTracker> producerTrackers = new CopyOnWriteArrayList<>();
  private final ExecutorServiceFactory executorServiceFactory =
      new DefaultExecutorServiceFactory(
          AVAILABLE_PROCESSORS, 10, "rabbitmq-stream-producer-connection-");
  private final boolean forceLeader;
  private final EventExecutorGroup eventExecutorGroup;
  private final boolean privateEventExecutorGroup;
  private final EventLoop eventLoop;
  private final EventLoop.Client<CoordinatorState> state;
  // recovery must not share the environment scheduler: blocking recovery work there starves
  // the scheduled continuations it depends on
  private final ExecutorService recoveryExecutor;

  /**
   * @param eventExecutorGroup the group backing the control-plane event loop, or null for the
   *     coordinator to create and own its own. It must have exactly one thread: the loop state is
   *     shared across all connections and agents, so a second thread would silently split it. Tests
   *     inject a deterministic group here; a caller-supplied group is not closed by {@link
   *     #close()}.
   */
  ProducersCoordinator(
      StreamEnvironment environment,
      int maxProducersByClient,
      int maxTrackingConsumersByClient,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      ClientFactory clientFactory,
      boolean forceLeader,
      EventExecutorGroup eventExecutorGroup) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxProducersByClient = maxProducersByClient;
    this.maxTrackingConsumersByClient = maxTrackingConsumersByClient;
    this.connectionNamingStrategy = connectionNamingStrategy;
    this.forceLeader = forceLeader;
    if (eventExecutorGroup == null) {
      // not the environment's netty I/O group on purpose: sharing with channel I/O would let the
      // loop thread be the thread blocked on a socket
      this.eventExecutorGroup =
          new DefaultEventExecutorGroup(1, threadFactory("rabbitmq-stream-producer-coordinator-"));
      this.privateEventExecutorGroup = true;
    } else {
      this.eventExecutorGroup = eventExecutorGroup;
      this.privateEventExecutorGroup = false;
    }
    this.eventLoop = new EventLoop(this.eventExecutorGroup, environment.rpcTimeout());
    this.recoveryExecutor =
        Executors.newFixedThreadPool(
            RECOVERY_THREADS, threadFactory("rabbitmq-stream-producer-recovery-"));
    this.state = this.eventLoop.register(CoordinatorState::new);
  }

  private BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.environment.recoveryBackOffDelayPolicy();
  }

  private BackOffDelayPolicy metadataUpdateBackOffDelayPolicy() {
    return this.environment.topologyUpdateBackOffDelayPolicy();
  }

  Runnable registerProducer(StreamProducer producer, String reference, String stream) {
    ProducerTracker tracker =
        new ProducerTracker(trackerIdSequence.getAndIncrement(), reference, stream, producer);
    if (DEBUG) {
      this.producerTrackers.add(tracker);
    }
    return registerAgentTracker(tracker, stream);
  }

  Runnable registerTrackingConsumer(StreamConsumer consumer) {
    return registerAgentTracker(
        new TrackingConsumerTracker(
            trackerIdSequence.getAndIncrement(), consumer.stream(), consumer),
        consumer.stream());
  }

  private Runnable registerAgentTracker(AgentTracker tracker, String stream) {
    List<BrokerWrapper> candidates = findCandidateNodes(stream, this.forceLeader);
    Broker broker = pickBroker(candidates);
    registerAgent(tracker);
    try {
      addToManager(broker, candidates, tracker);
    } catch (RuntimeException e) {
      // the initial registration does not retry, the failure goes back to the caller
      trackerEvent(
          tracker,
          recoveryBackOffDelayPolicy(),
          (st, epoch) -> AgentStateMachine.onAssignmentFailed(st, epoch, epoch, e, false));
      throw e;
    }
    assignmentSucceeded(tracker, recoveryBackOffDelayPolicy(), FIRST_ATTEMPT_EPOCH);

    Runnable cancel =
        () -> {
          // cancel() first, synchronously, then post onCancelled: see the same comment in
          // ConsumersCoordinator.subscribe
          tracker.cancel();
          trackerEvent(tracker, recoveryBackOffDelayPolicy(), AgentStateMachine::onCancelled);
        };
    if (DEBUG) {
      return () -> {
        if (tracker instanceof ProducerTracker) {
          try {
            this.producerTrackers.remove(tracker);
          } catch (Exception e) {
            LOGGER.debug("Error while removing producer tracker from list");
          }
        }
        cancel.run();
      };
    } else {
      return cancel;
    }
  }

  private void registerAgent(AgentTracker tracker) {
    submitState(s -> s.agents.put(tracker.uniqueId(), new TrackerState(tracker)));
  }

  /**
   * Apply a decision function to an agent's control state on the event loop.
   *
   * <p>Fire-and-forget, because the callers include netty I/O threads, which must never wait on the
   * loop.
   */
  private void trackerEvent(
      AgentTracker tracker,
      BackOffDelayPolicy delayPolicy,
      BiFunction<State, Long, TransitionResult> decision) {
    submitState(
        s -> {
          TrackerState trackerState = s.agents.get(tracker.uniqueId());
          if (trackerState == null) {
            return;
          }
          State previous = trackerState.state;
          TransitionResult result = decision.apply(trackerState.state, trackerState.epoch);
          boolean newAttempt =
              result.state() == State.RECOVERING && result.epoch() != trackerState.epoch;
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
            trackerState.nextAttemptAt =
                System.nanoTime() + backOffNanos(delayPolicy, backOffIndex);
          }
          if (result.state() == State.ACTIVE) {
            // a successful assignment ends the recovery episode: the retry timeout is meant to
            // bound one episode, not the agent's whole life
            trackerState.attempts = 0;
          }
          if (result.state().terminal()) {
            s.agents.remove(tracker.uniqueId());
          }
          if (result.hasEffect()) {
            // the whole effect goes to a single task: the effects of one transition are ordered
            // (detach before re-assign, for instance), which separate tasks on a multi-threaded
            // pool would not guarantee
            AgentActions actions =
                new AgentActions(tracker, delayPolicy, backOffIndex, previous == State.OPENING);
            submitRecovery(
                () -> {
                  try {
                    result.applyEffect(actions);
                  } catch (Throwable e) {
                    LOGGER.warn(
                        "Error while applying transition effect for {}: {}",
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

  private void assignmentSucceeded(
      AgentTracker tracker, BackOffDelayPolicy delayPolicy, long attemptEpoch) {
    trackerEvent(
        tracker,
        delayPolicy,
        (st, epoch) -> AgentStateMachine.onAssignmentSucceeded(st, epoch, attemptEpoch));
  }

  private void assignmentFailed(
      AgentTracker tracker,
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

  /** A failed candidate lookup: park the agent for another attempt later. */
  private void lookupFailed(
      AgentTracker tracker, BackOffDelayPolicy delayPolicy, long attemptEpoch, Throwable cause) {
    assignmentFailed(tracker, delayPolicy, attemptEpoch, cause, true);
  }

  /**
   * One assignment attempt. Blocking, so it always runs on the recovery pool.
   *
   * <p>Bounded on purpose: exactly one candidate lookup, then either an assignment or a transition.
   * A lookup that fails does not retry here — it parks the agent, so the recovery pool has only
   * {@code RECOVERY_THREADS} threads and a stream that stays unreachable must never own one while
   * it waits. The back-off policy is the retry mechanism, and {@link
   * AgentActions#scheduleAssignment} is where it gives up.
   */
  private void assign(AgentTracker tracker, long attemptEpoch, BackOffDelayPolicy delayPolicy) {
    if (!tracker.isOpen()) {
      LOGGER.debug("Not recovering {} because it has been closed", tracker.label());
      trackerEvent(tracker, delayPolicy, AgentStateMachine::onCancelled);
      return;
    }
    if (superseded(tracker, attemptEpoch)) {
      // typically an attempt that waited out its back-off delay while newer events took over
      LOGGER.debug("Skipping superseded assignment attempt for {}", tracker.label());
      return;
    }
    List<BrokerWrapper> candidates;
    Broker broker;
    try {
      candidates = findCandidateNodes(tracker.stream(), this.forceLeader);
      // part of the lookup: a stream without a leader is usually electing one, which is worth
      // waiting for, not a reason to close the agent
      broker = pickBroker(candidates);
    } catch (StreamDoesNotExistException e) {
      // the stream is gone: there is nothing to come back to, so this agent is over
      LOGGER.debug("Stream '{}' does not exist, closing {}", tracker.stream(), tracker.label());
      assignmentFailed(tracker, delayPolicy, attemptEpoch, e, false);
      return;
    } catch (Exception e) {
      LOGGER.debug(
          "Candidate lookup for {} failed, parking it: {}",
          tracker.label(),
          Utils.exceptionMessage(e));
      lookupFailed(tracker, delayPolicy, attemptEpoch, e);
      return;
    }
    try {
      if (superseded(tracker, attemptEpoch)) {
        // re-checked after the lookup, the slow part of an attempt, and as late as possible before
        // the broker gets involved
        LOGGER.debug("Not assigning superseded attempt for {}", tracker.label());
        return;
      }
      LOGGER.debug("Using {} to resume {}", broker.label(), tracker.label());
      addToManager(broker, candidates, tracker);
      if (!tracker.isOpen()) {
        // closed while this attempt was assigning: cancel() may have found no manager to
        // unregister from, and the success event cannot release anything once the closed agent
        // is gone from the loop state. The agent is flagged closed before cancel() runs, so
        // either cancel() or this check sees the new assignment
        LOGGER.debug("{} closed during its assignment, releasing it", tracker.label());
        ClientProducersManager manager = tracker.manager();
        if (manager != null) {
          manager.unregister(tracker);
        }
        trackerEvent(tracker, delayPolicy, AgentStateMachine::onCancelled);
        return;
      }
      assignmentSucceeded(tracker, delayPolicy, attemptEpoch);
    } catch (Exception e) {
      LOGGER.debug("Error while assigning {}: {}", tracker.label(), Utils.exceptionMessage(e));
      assignmentFailed(tracker, delayPolicy, attemptEpoch, e, recoverable(e));
    }
  }

  /** Whether a failed assignment is worth another attempt. */
  static boolean recoverable(Throwable cause) {
    if (cause == null) {
      return false;
    }
    if (shouldRefreshCandidates(cause)) {
      return true;
    }
    if (cause instanceof StreamException) {
      short code = ((StreamException) cause).getCode();
      // the publisher id is still declared on the broker from a previous attempt, or was deleted
      // under us: worth another attempt, the same way the consumer side treats a subscription id
      // that already exists
      return code == Constants.RESPONSE_CODE_PRECONDITION_FAILED
          || code == Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST;
    }
    return false;
  }

  private static short codeFor(Throwable cause) {
    // cause is null only for a transition triggered by a disruption rather than a failure, which
    // never gets here with a deleted stream
    return cause instanceof StreamDoesNotExistException
            || (cause != null && cause.getCause() instanceof StreamDoesNotExistException)
        ? Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST
        : Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE;
  }

  /**
   * Whether an attempt has been superseded, and so must not touch the broker.
   *
   * <p>A superseded attempt that assigns anyway is undone by the {@code releaseAssignment} effect.
   * The event that superseded it always started an attempt of its own, so giving up here does not
   * cost the agent its recovery.
   */
  private boolean superseded(AgentTracker tracker, long attemptEpoch) {
    Boolean superseded =
        this.state.query(
            s -> {
              TrackerState trackerState = s.agents.get(tracker.uniqueId());
              // gone from the map: the agent reached a terminal state, so there is nothing left to
              // assign either
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
      LOGGER.debug("Producer recovery task rejected, the coordinator is closing");
    }
  }

  /**
   * Runs the effects of a transition.
   *
   * <p>Always invoked from a single task on the recovery pool, never on the event loop: the calls
   * here block, and the agent notifications run application code.
   */
  private final class AgentActions implements AgentStateMachine.Actions {

    private final AgentTracker tracker;
    private final BackOffDelayPolicy delayPolicy;
    // the number of attempts already made in this recovery episode, which is also the index this
    // attempt's delay comes from
    private final int backOffIndex;
    // the transition left OPENING, i.e. this is the registration made from the agent's own
    // constructor (see markOpen)
    private final boolean initialAssignment;

    private AgentActions(
        AgentTracker tracker,
        BackOffDelayPolicy delayPolicy,
        int backOffIndex,
        boolean initialAssignment) {
      this.tracker = tracker;
      this.delayPolicy = delayPolicy;
      this.backOffIndex = backOffIndex;
      this.initialAssignment = initialAssignment;
    }

    @Override
    public void dispatchAssignment(long attemptEpoch) {
      assign(this.tracker, attemptEpoch, this.delayPolicy);
    }

    @Override
    public void scheduleAssignment(long attemptEpoch, Throwable cause) {
      Duration delay = this.delayPolicy.delay(this.backOffIndex);
      if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
        LOGGER.debug(
            "Giving up on {} after {} attempt(s)", this.tracker.label(), this.backOffIndex);
        assignmentFailed(this.tracker, this.delayPolicy, attemptEpoch, cause, false);
        return;
      }
      environment
          .scheduledExecutorService()
          .schedule(
              () -> submitRecovery(() -> assign(this.tracker, attemptEpoch, this.delayPolicy)),
              delay.toMillis(),
              MILLISECONDS);
    }

    /**
     * Only drops the assignment: the agent itself was already flipped to unavailable inline, on the
     * thread that saw the disruption, so publishing stops immediately.
     *
     * <p>That is enough because the only transitions into {@code RECOVERING} from {@code ACTIVE}
     * are {@code onConnectionLost} and {@code onStreamUnavailable}, and both are posted by
     * listeners that make the inline flip first. {@code onWatchdogTick} and {@code
     * onAssignmentFailed} go from {@code RECOVERING} to {@code RECOVERING}, where the agent is
     * already unavailable.
     */
    @Override
    public void markRecovering() {
      notifyAgent(this.tracker::detachFromManager, "marking recovering");
    }

    @Override
    public void markOpen() {
      if (this.initialAssignment) {
        // the agent is still being constructed by the thread that registered it, and becomes
        // open on its own once registration returns: there is nothing to recover, and running()
        // would touch producer state that does not exist yet
        return;
      }
      // blocking: republishes the producer's unconfirmed messages under the producer lock
      notifyAgent(this.tracker::running, "marking open");
    }

    private void notifyAgent(Runnable notification, String description) {
      try {
        notification.run();
      } catch (Exception e) {
        LOGGER.warn(
            "Error while {} for {}: {}",
            description,
            this.tracker.label(),
            Utils.exceptionMessage(e));
      }
    }

    @Override
    public void closeAfterStreamDeletion(Throwable cause) {
      try {
        this.tracker.closeAfterStreamDeletion(codeFor(cause));
      } catch (Exception e) {
        LOGGER.debug("Error while closing {}: {}", this.tracker.label(), e.getMessage());
      }
    }

    @Override
    public void releaseAssignment() {
      ClientProducersManager manager = this.tracker.manager();
      if (manager != null) {
        manager.unregister(this.tracker);
      }
    }
  }

  private void addToManager(Broker node, List<BrokerWrapper> candidates, AgentTracker tracker) {
    ClientParameters clientParameters =
        environment
            .clientParametersCopy()
            .host(node.getHost())
            .port(node.getPort())
            .executorServiceFactory(this.executorServiceFactory)
            .dispatchingExecutorServiceFactory(Utils.NO_OP_EXECUTOR_SERVICE_FACTORY);
    while (true) {
      Placement placement = placement(node, tracker);
      if (placement.waitFor != null) {
        // a connection to this node is being opened, share it instead of opening another one
        awaitConnectionCreation(placement.waitFor);
        continue;
      }
      ClientProducersManager pickedManager = placement.manager;
      if (pickedManager == null) {
        String name = keyForNode(node);
        LOGGER.debug("Trying to create producer manager on {}", name);
        try {
          pickedManager =
              new ClientProducersManager(node, candidates, this.clientFactory, clientParameters);
        } catch (RuntimeException e) {
          creationFinished(node, null);
          throw e;
        }
        LOGGER.debug("Created producer manager on {}, id {}", name, pickedManager.id);
        creationFinished(node, pickedManager);
      }
      try {
        pickedManager.register(tracker);
        LOGGER.debug(
            "Assigned {} tracker {} (stream '{}') to manager {} (node {}), publisher ID {}",
            tracker.type(),
            tracker.uniqueId(),
            tracker.stream(),
            pickedManager.id,
            pickedManager.name,
            tracker.identifiable() ? tracker.id() : "N/A");
        return;
      } catch (IllegalStateException e) {
        // full or closed in the meantime, pick again
      } catch (RuntimeException e) {
        if (shouldRefreshCandidates(e)) {
          // manager connection is dead or stream not available
          // scheduling manager closing if necessary in another thread to avoid blocking this one
          if (pickedManager.isEmpty()) {
            this.environment.execute(
                pickedManager::closeIfEmpty,
                "Producer manager closing after timeout, producer %d on stream '%s'",
                tracker.uniqueId(),
                tracker.stream());
          }
        } else {
          pickedManager.closeIfEmpty();
        }
        throw e;
      }
    }
  }

  /**
   * Pick an existing connection to the node with spare capacity for the agent, or reserve the right
   * to open one.
   *
   * <p>Atomic by construction: it runs on the event loop, which is the single writer of the pool.
   */
  private Placement placement(Broker node, AgentTracker tracker) {
    String key = keyForNode(node);
    return this.state.query(
        s -> {
          s.connections.removeIf(ClientProducersManager::isDead);
          for (ClientProducersManager manager : s.connections) {
            if (node.equals(manager.node) && !manager.isFullFor(tracker)) {
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

  private void creationFinished(Broker node, ClientProducersManager manager) {
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
      throw new StreamException("Interrupted while waiting for a producer connection", e);
    } catch (ExecutionException e) {
      throw new StreamException("Error while waiting for a producer connection", e);
    } catch (TimeoutException e) {
      throw new TimeoutStreamException("Timeout while waiting for a producer connection");
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

  // the connection pool is coordinator-owned state, so managers do not reach into it directly
  private void removeFromPool(ClientProducersManager manager) {
    // fire-and-forget: this is called from netty I/O threads, which must never wait on the loop
    submitState(s -> s.connections.remove(manager));
  }

  // package protected for testing
  List<BrokerWrapper> findCandidateNodes(String stream, boolean forceLeader) {
    Map<String, Client.StreamMetadata> metadata =
        this.environment.locatorOperation(
            namedFunction(c -> c.metadata(stream), "Candidate lookup to publish to '%s'", stream));
    if (metadata.isEmpty() || metadata.get(stream) == null) {
      throw new StreamDoesNotExistException(stream);
    }

    Client.StreamMetadata streamMetadata = metadata.get(stream);
    if (!streamMetadata.isResponseOk()) {
      if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
        throw new StreamDoesNotExistException(stream);
      } else {
        throw new IllegalStateException(
            "Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
      }
    }

    List<BrokerWrapper> candidates = new ArrayList<>();
    Client.Broker leader = streamMetadata.getLeader();
    if (leader == null) {
      if (forceLeader) {
        throw new IllegalStateException("Not leader available for stream " + stream);
      }
    } else {
      candidates.add(new BrokerWrapper(leader, true));
    }

    if (!forceLeader && streamMetadata.hasReplicas()) {
      candidates.addAll(
          streamMetadata.getReplicas().stream()
              .map(b -> new BrokerWrapper(b, false))
              .collect(toList()));
    }

    if (candidates.isEmpty()) {
      throw new IllegalStateException("No stream member available to publish for stream " + stream);
    } else {
      LOGGER.debug("Candidates to publish to {}: {}", stream, candidates);
    }

    return List.copyOf(candidates);
  }

  static Broker pickBroker(List<BrokerWrapper> candidates) {
    return candidates.stream()
        .filter(BrokerWrapper::isLeader)
        .findFirst()
        .map(BrokerWrapper::broker)
        .orElseThrow(() -> new IllegalStateException("Not leader available"));
  }

  public void close() {
    if (this.state.isClosed()) {
      return;
    }
    List<ClientProducersManager> connections =
        queryState(
            s -> {
              List<ClientProducersManager> all = new ArrayList<>(s.connections);
              s.connections.clear();
              return all;
            },
            Collections.emptyList());
    for (ClientProducersManager manager : connections) {
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

  int clientCount() {
    return queryState(s -> s.connections.size(), 0);
  }

  int nodesConnected() {
    return queryState(s -> s.connections.stream().map(m -> m.name).collect(toSet()).size(), 0);
  }

  @Override
  public String toString() {
    List<ClientProducersManager> connections =
        queryState(s -> new ArrayList<>(s.connections), Collections.emptyList());
    StringBuilder builder = new StringBuilder("{");
    builder.append(jsonField("client_count", connections.size())).append(",");
    builder
        .append(
            jsonField("producer_count", connections.stream().mapToInt(m -> m.producerCount).sum()))
        .append(",");
    builder
        .append(
            jsonField(
                "tracking_consumer_count",
                connections.stream().mapToInt(m -> m.trackingConsumerTrackers.size()).sum()))
        .append(",");
    if (DEBUG) {
      builder.append(jsonField("producer_tracker_count", this.producerTrackers.size())).append(",");
    }
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
                      .append(jsonField("producer_count", m.producerCount))
                      .append(",")
                      .append(
                          jsonField("tracking_consumer_count", m.trackingConsumerTrackers.size()))
                      .append(",");
                  managerBuilder.append("\"producers\" : [");
                  List<ProducerTracker> trackers = m.producerTrackers;
                  managerBuilder.append(
                      IntStream.range(0, trackers.size())
                          .filter(i -> trackers.get(i) != null)
                          .mapToObj(
                              i -> {
                                ProducerTracker p = trackers.get(i);
                                return "{"
                                    + jsonField("stream", p.stream())
                                    + ","
                                    + jsonField("producer_id", i)
                                    + ","
                                    + jsonField("state", p.producer.state())
                                    + "}";
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("],");
                  managerBuilder.append("\"tracking_consumers\" : [");
                  managerBuilder.append(
                      m.trackingConsumerTrackers.stream()
                          .map(
                              t -> {
                                StringBuilder trackerBuilder = new StringBuilder("{");
                                trackerBuilder.append(jsonField("stream", t.stream()));
                                return trackerBuilder.append("}").toString();
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("]");
                  return managerBuilder.append("}").toString();
                })
            .collect(Collectors.joining(",")));
    builder.append("]");
    if (DEBUG) {
      builder.append(",");
      builder.append("\"producer_trackers\" : [");
      builder.append(
          this.producerTrackers.stream()
              .map(
                  t -> {
                    StringBuilder b = new StringBuilder("{");
                    b.append(quote("stream")).append(":").append(quote(t.stream)).append(",");
                    b.append(quote("node")).append(":");
                    Client client = null;
                    ClientProducersManager manager = t.manager();
                    if (manager != null) {
                      client = manager.client;
                    }
                    if (client == null) {
                      b.append("null");
                    } else {
                      b.append(quote(client.getHost() + ":" + client.getPort()));
                    }
                    return b.append("}").toString();
                  })
              .collect(Collectors.joining(",")));
      builder.append("]");
    }
    return builder.append("}").toString();
  }

  private interface AgentTracker {

    void assign(byte producerId, Client client, ClientProducersManager manager);

    boolean identifiable();

    byte id();

    /**
     * Clear the assignment and notify the agent it is unavailable. Called inline by the thread that
     * saw the disruption, so publishing stops right away.
     */
    void markUnavailable();

    /** Clear the assignment only. Idempotent. */
    void detachFromManager();

    ClientProducersManager manager();

    /**
     * Release the assignment to {@code expected}, if it is still the current one.
     *
     * @return the slot to free, or a negative value if there is nothing to release
     */
    int detachIfOwnedBy(ClientProducersManager expected);

    void running();

    void cancel();

    void closeAfterStreamDeletion(short code);

    String stream();

    String reference();

    boolean isOpen();

    long uniqueId();

    String type();

    default String label() {
      return String.format("[%s %d, stream '%s']", type(), uniqueId(), stream());
    }
  }

  private static class ProducerTracker implements AgentTracker {

    private final long uniqueId;
    private final String reference;
    private final String stream;
    private final StreamProducer producer;
    // set only once the broker has confirmed the publisher declaration, never while the slot is
    // merely reserved
    private final AtomicReference<Assignment> assignment = new AtomicReference<>(Assignment.NONE);

    private ProducerTracker(
        long uniqueId, String reference, String stream, StreamProducer producer) {
      this.uniqueId = uniqueId;
      this.reference = reference;
      this.stream = stream;
      this.producer = producer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      this.assignment.set(new Assignment(producerId, manager));
      this.producer.setPublisherId(producerId);
      this.producer.setClient(client);
    }

    @Override
    public boolean identifiable() {
      return true;
    }

    @Override
    public byte id() {
      return this.assignment.get().publisherId;
    }

    @Override
    public String reference() {
      return this.reference;
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void markUnavailable() {
      this.detachFromManager();
      this.producer.unavailable();
    }

    @Override
    public void detachFromManager() {
      this.assignment.set(Assignment.NONE);
    }

    @Override
    public ClientProducersManager manager() {
      return this.assignment.get().manager;
    }

    /**
     * Release this tracker's assignment to {@code expected}, if it is still the current one.
     *
     * <p>Idempotent by construction: of two concurrent callers (the direct call in {@link
     * #cancel()} and the async {@code releaseAssignment} effect), only the one that wins the CAS
     * gets a slot back to release; the other sees the assignment already cleared and no-ops.
     */
    @Override
    public int detachIfOwnedBy(ClientProducersManager expected) {
      Assignment current = this.assignment.get();
      if (current.manager != expected) {
        return -1;
      }
      if (!this.assignment.compareAndSet(current, Assignment.NONE)) {
        return -1;
      }
      // masked to an unsigned 0-255 range: a plain byte cannot serve as its own "no slot"
      // sentinel, since slot 255's byte representation (0xFF) is indistinguishable from -1
      return current.publisherId & 0xFF;
    }

    @Override
    public void running() {
      this.producer.running();
    }

    @Override
    public void cancel() {
      ClientProducersManager manager = this.manager();
      if (manager != null) {
        manager.unregister(this);
      }
    }

    @Override
    public void closeAfterStreamDeletion(short code) {
      this.producer.closeAfterStreamDeletion(code);
    }

    @Override
    public boolean isOpen() {
      return producer.isOpen();
    }

    @Override
    public long uniqueId() {
      return this.uniqueId;
    }

    @Override
    public String type() {
      return "producer";
    }
  }

  /** A producer's current manager and publisher ID, or {@link #NONE} if it has none. */
  private static final class Assignment {

    private static final Assignment NONE = new Assignment(NO_SLOT, null);

    private final byte publisherId;
    private final ClientProducersManager manager;

    private Assignment(byte publisherId, ClientProducersManager manager) {
      this.publisherId = publisherId;
      this.manager = manager;
    }
  }

  private static class TrackingConsumerTracker implements AgentTracker {

    private final long uniqueId;
    private final String stream;
    private final StreamConsumer consumer;
    private final AtomicReference<ClientProducersManager> manager = new AtomicReference<>();

    private TrackingConsumerTracker(long uniqueId, String stream, StreamConsumer consumer) {
      this.uniqueId = uniqueId;
      this.stream = stream;
      this.consumer = consumer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      this.manager.set(manager);
      this.consumer.setTrackingClient(client);
    }

    @Override
    public boolean identifiable() {
      return false;
    }

    @Override
    public byte id() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String reference() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void markUnavailable() {
      this.detachFromManager();
      this.consumer.unavailable();
    }

    @Override
    public void detachFromManager() {
      this.manager.set(null);
    }

    @Override
    public ClientProducersManager manager() {
      return this.manager.get();
    }

    // no slot to give back, only the ownership, hence 0 for the winner of the CAS
    @Override
    public int detachIfOwnedBy(ClientProducersManager expected) {
      return expected != null && this.manager.compareAndSet(expected, null) ? 0 : -1;
    }

    @Override
    public void running() {
      this.consumer.running();
    }

    @Override
    public void cancel() {
      ClientProducersManager manager = this.manager();
      if (manager != null) {
        manager.unregister(this);
      }
    }

    @Override
    public void closeAfterStreamDeletion(short code) {
      // nothing to do here, the consumer will be closed by the consumers coordinator if
      // the stream has been deleted
    }

    @Override
    public boolean isOpen() {
      return this.consumer.isOpen();
    }

    @Override
    public long uniqueId() {
      return this.uniqueId;
    }

    @Override
    public String type() {
      return "tracking consumer";
    }
  }

  private class ClientProducersManager implements Comparable<ClientProducersManager> {

    private final long id;
    private final String name;
    private final Broker node;
    // trackers and producer count must be kept in sync; the array has a single writer, the event
    // loop, so a slot picked there is never picked twice, and a slot freed there is never freed
    // while its delete-publisher RPC is still in flight (the array stays occupied until then)
    private volatile List<ProducerTracker> producerTrackers = createProducerTrackerList();
    private final AtomicInteger producerIndexSequence = new AtomicInteger(0);
    // written by the event loop only, concurrent because the shutdown listener iterates it on a
    // netty thread
    private final Set<AgentTracker> trackingConsumerTrackers =
        ConcurrentHashMap.newKeySet(maxTrackingConsumersByClient);
    // written by the event loop only, but concurrent on purpose: the metadata listener reads it
    // on a netty thread, to flip the affected agents inline before posting to the loop
    private final Map<String, Set<AgentTracker>> streamToTrackers = new ConcurrentHashMap<>();
    private final Client client;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile int producerCount;
    private volatile int trackingConsumerCount;

    private ClientProducersManager(
        Broker targetNode,
        List<BrokerWrapper> candidates,
        ClientFactory cf,
        Client.ClientParameters clientParameters) {
      this.id = managerIdSequence.getAndIncrement();
      AtomicReference<Client> ref = new AtomicReference<>();
      AtomicBoolean clientInitializedInManager = new AtomicBoolean(false);
      PublishConfirmListener publishConfirmListener =
          (publisherId, publishingId) -> {
            ProducerTracker producerTracker = producerTrackers.get(publisherId & 0xFF);
            if (producerTracker == null) {
              LOGGER.info("Received publish confirm for unknown producer: {}", publisherId);
            } else {
              producerTracker.producer.confirm(publishingId);
            }
          };
      PublishErrorListener publishErrorListener =
          (publisherId, publishingId, errorCode) -> {
            ProducerTracker producerTracker = producerTrackers.get(publisherId & 0xFF);
            if (producerTracker == null) {
              LOGGER.info(
                  "Received publish error for unknown producer: {}, error code {}",
                  publisherId,
                  Utils.formatConstant(errorCode));
            } else {
              producerTracker.producer.error(publishingId, errorCode);
            }
          };
      ShutdownListener shutdownListener =
          shutdownContext -> {
            if (clientInitializedInManager.get()) {
              this.closed.set(true);
              removeFromPool(this);
            }
            if (shutdownContext.isShutdownUnexpected()) {
              LOGGER.debug(
                  "Recovering {} producer(s) and {} tracking consumer(s) after unexpected connection termination",
                  producerCount,
                  trackingConsumerTrackers.size());
              // only the confirmed assignments: a slot reserved for an attempt still in flight
              // belongs to that attempt, which fails on its own with the connection
              List<AgentTracker> affected = new ArrayList<>();
              iterate(this.producerTrackers, t -> addIfAssignedHere(t, affected));
              iterate(this.trackingConsumerTrackers, t -> addIfAssignedHere(t, affected));
              // inline, on the thread that saw the disruption: publishing must stop before
              // anything else
              affected.forEach(AgentTracker::markUnavailable);
              affected.forEach(
                  tracker ->
                      trackerEvent(
                          tracker,
                          recoveryBackOffDelayPolicy(),
                          AgentStateMachine::onConnectionLost));
            }
          };
      MetadataListener metadataListener =
          (stream, code) -> {
            LOGGER.debug(
                "Received metadata notification for '{}', stream is likely to have become unavailable",
                stream);
            // collected and notified on this thread, not on the loop: publishing has to stop
            // before anything else happens, and a state listener is application code that must
            // never run on the loop thread
            List<AgentTracker> affected = trackersFor(stream);
            LOGGER.debug(
                "Affected publishers and consumer trackers after metadata update: {}",
                affected.size());
            if (affected.isEmpty()) {
              return;
            }
            affected.forEach(AgentTracker::markUnavailable);
            // fire-and-forget: this runs on a netty I/O thread, which must never wait on the loop
            submitState(
                s -> {
                  // one by one, not the whole stream entry: a tracker registered for this stream
                  // since the collection above must stay reachable by the next notification.
                  // Slots are found by identity, the inline flip above cleared the assignments
                  Set<AgentTracker> affectedSet =
                      Collections.newSetFromMap(new IdentityHashMap<>());
                  affectedSet.addAll(affected);
                  List<ProducerTracker> updated = createProducerTrackerList();
                  List<ProducerTracker> current = this.producerTrackers;
                  for (int i = 0; i < MAX_PRODUCERS_PER_CLIENT; i++) {
                    ProducerTracker t = current.get(i);
                    updated.set(i, affectedSet.contains(t) ? null : t);
                  }
                  this.setProducerTrackers(updated);
                  for (AgentTracker tracker : affected) {
                    if (!tracker.identifiable()) {
                      trackingConsumerTrackers.remove(tracker);
                    }
                    removeFromStreamToTrackers(tracker);
                  }
                  this.trackingConsumerCount = this.trackingConsumerTrackers.size();
                  affected.forEach(
                      t ->
                          trackerEvent(
                              t,
                              metadataUpdateBackOffDelayPolicy(),
                              AgentStateMachine::onStreamUnavailable));
                  submitRecovery(this::closeIfEmpty);
                });
          };
      String connectionName = connectionNamingStrategy.apply(ClientConnectionType.PRODUCER);
      ClientFactoryContext connectionFactoryContext =
          new ClientFactoryContext(
              clientParameters
                  .publishConfirmListener(publishConfirmListener)
                  .publishErrorListener(publishErrorListener)
                  .shutdownListener(shutdownListener)
                  .metadataListener(metadataListener)
                  .clientProperty("connection_name", connectionName),
              keyForNode(targetNode),
              candidates.stream().map(BrokerWrapper::broker).collect(toList()));
      this.client = cf.client(connectionFactoryContext);
      this.node = Utils.brokerFromClient(this.client);
      this.name = keyForNode(this.node);
      LOGGER.debug("Created producer connection '{}'", connectionName);
      clientInitializedInManager.set(true);
      ref.set(this.client);
    }

    private void addIfAssignedHere(AgentTracker tracker, List<AgentTracker> trackers) {
      if (tracker.manager() == this) {
        trackers.add(tracker);
      }
    }

    private List<AgentTracker> trackersFor(String stream) {
      Set<AgentTracker> trackers = this.streamToTrackers.get(stream);
      return trackers == null ? Collections.emptyList() : new ArrayList<>(trackers);
    }

    private void register(AgentTracker tracker) {
      if (tracker.identifiable()) {
        ProducerTracker producerTracker = (ProducerTracker) tracker;
        byte publisherId = reserveSlot(producerTracker);
        try {
          checkNotClosed();
          Response response =
              callAndMaybeRetry(
                  () ->
                      this.client.declarePublisher(
                          publisherId, tracker.reference(), tracker.stream()),
                  RETRY_ON_TIMEOUT,
                  "Declare publisher request for publisher %d on stream '%s'",
                  producerTracker.uniqueId(),
                  producerTracker.stream());
          if (!response.isOk()) {
            String message =
                "Error while declaring publisher: "
                    + formatConstant(response.getResponseCode())
                    + ". Could not assign producer to client.";
            LOGGER.info(message);
            throw new StreamException(message, response.getResponseCode());
          }
          tracker.assign(publisherId, this.client, this);
        } catch (RuntimeException e) {
          releaseSlot(publisherId, producerTracker);
          throw e;
        }
      } else {
        reserveTrackingConsumer(tracker);
        tracker.assign((byte) 0, this.client, this);
      }
      queryState(
          s -> {
            // not if it has been detached in the meantime, e.g. by a cancellation: the entry would
            // outlive the unregistration that was meant to remove it
            if (tracker.manager() == this) {
              this.streamToTrackers
                  .computeIfAbsent(tracker.stream(), st -> ConcurrentHashMap.newKeySet())
                  .add(tracker);
            }
            return null;
          },
          null);
    }

    /**
     * Reserve a slot and publish it in the tracker array, so a publish confirm arriving right after
     * the declare-publisher RPC finds its tracker.
     *
     * <p>Atomic by construction: it runs on the event loop, the single writer of the array.
     */
    private byte reserveSlot(ProducerTracker tracker) {
      SlotReservation reservation =
          ProducersCoordinator.this.state.query(
              s -> {
                if (this.isFullFor(tracker)) {
                  return SlotReservation.FULL;
                }
                if (this.isDead()) {
                  return SlotReservation.DEAD;
                }
                byte publisherId =
                    (byte)
                        ConsumersCoordinator.pickSlot(
                            this.producerTrackers, this.producerIndexSequence);
                this.setProducerTrackers(update(this.producerTrackers, publisherId, tracker));
                return SlotReservation.reserved(publisherId);
              });
      checkReservation(reservation, tracker);
      return reservation.publisherId;
    }

    private void reserveTrackingConsumer(AgentTracker tracker) {
      SlotReservation reservation =
          ProducersCoordinator.this.state.query(
              s -> {
                if (this.isFullFor(tracker)) {
                  return SlotReservation.FULL;
                }
                if (this.isDead()) {
                  return SlotReservation.DEAD;
                }
                this.trackingConsumerTrackers.add(tracker);
                this.trackingConsumerCount = this.trackingConsumerTrackers.size();
                return SlotReservation.reserved(NO_SLOT);
              });
      checkReservation(reservation, tracker);
    }

    private void checkReservation(SlotReservation reservation, AgentTracker tracker) {
      if (reservation != null && reservation.full) {
        LOGGER.debug(
            "Cannot add {} for stream '{}', manager is full", tracker.type(), tracker.stream());
        throw new IllegalStateException("Cannot add " + tracker.type() + ", the manager is full");
      }
      // null when the loop declined the query, i.e. the coordinator is closing
      if (reservation == null || reservation.dead) {
        LOGGER.debug(
            "Cannot add {} for stream '{}', manager is closed", tracker.type(), tracker.stream());
        throw new IllegalStateException("Cannot add " + tracker.type() + ", the manager is closed");
      }
    }

    // undo a reservation that failed to declare the publisher: the tracker was never confirmed, so
    // there is nothing to delete on the broker, only the array slot to free. Blocking (not
    // fire-and-forget): the caller is addToManager(), off-loop, which checks isEmpty() right after
    // this returns, so the free must be visible by then
    private void releaseSlot(byte publisherId, ProducerTracker tracker) {
      queryState(
          s -> {
            this.freeSlot(publisherId & 0xFF, tracker);
            return null;
          },
          null);
    }

    // only if the slot still holds this tracker: the metadata listener frees slots too, and the
    // slot may have been handed to another producer in between
    private void freeSlot(int slot, ProducerTracker tracker) {
      if (this.producerTrackers.get(slot) == tracker) {
        this.setProducerTrackers(update(this.producerTrackers, (byte) slot, null));
      }
    }

    private void unregister(AgentTracker tracker) {
      int slot = tracker.detachIfOwnedBy(this);
      if (slot < 0) {
        // already removed, e.g. by the direct call in cancel() racing the async
        // releaseAssignment effect: detachIfOwnedBy() is idempotent, so this is a no-op
        return;
      }
      LOGGER.debug(
          "Unregistering {} {} from manager on {}", tracker.type(), tracker.uniqueId(), this.name);
      if (tracker.identifiable() && this.client.isOpen()) {
        try {
          Response response =
              callAndMaybeRetry(
                  () -> this.client.deletePublisher((byte) slot),
                  RETRY_ON_TIMEOUT,
                  "Delete publisher request for publisher %d on stream '%s'",
                  tracker.uniqueId(),
                  tracker.stream());
          if (!response.isOk()) {
            LOGGER.warn(
                "Unexpected response code when deleting publisher on stream '{}': {} (publisher ID {})",
                tracker.stream(),
                formatConstant(response.getResponseCode()),
                slot);
          }
        } catch (TimeoutStreamException e) {
          LOGGER.debug(
              "Reached timeout when trying to delete publisher {} on stream '{}'",
              tracker.uniqueId(),
              tracker.stream());
        }
      }
      // the slot stays occupied until now, so no new producer can reuse the same publisher ID while
      // the delete above is in flight; freeing the slot and checking emptiness happen together so a
      // concurrent register() cannot slip in between and be torn down by close()
      Boolean empty =
          queryState(
              s -> {
                if (tracker.identifiable()) {
                  this.freeSlot(slot, (ProducerTracker) tracker);
                } else {
                  this.trackingConsumerTrackers.remove(tracker);
                  this.trackingConsumerCount = this.trackingConsumerTrackers.size();
                }
                this.removeFromStreamToTrackers(tracker);
                return this.isEmpty();
              },
              false);
      if (Boolean.TRUE.equals(empty)) {
        this.closeIfEmpty();
      }
    }

    private void removeFromStreamToTrackers(AgentTracker tracker) {
      this.streamToTrackers.computeIfPresent(
          tracker.stream(),
          (st, trackers) -> {
            trackers.remove(tracker);
            return trackers.isEmpty() ? null : trackers;
          });
    }

    private List<ProducerTracker> createProducerTrackerList() {
      List<ProducerTracker> trackers = new ArrayList<>(MAX_PRODUCERS_PER_CLIENT);
      IntStream.range(0, MAX_PRODUCERS_PER_CLIENT).forEach(i -> trackers.add(null));
      return trackers;
    }

    private List<ProducerTracker> update(
        List<ProducerTracker> original, byte index, ProducerTracker newValue) {
      List<ProducerTracker> newTrackers = createProducerTrackerList();
      int intIndex = index & 0xFF;
      for (int i = 0; i < MAX_PRODUCERS_PER_CLIENT; i++) {
        newTrackers.set(i, i == intIndex ? newValue : original.get(i));
      }
      return newTrackers;
    }

    private void setProducerTrackers(List<ProducerTracker> trackers) {
      this.producerTrackers = trackers;
      this.producerCount = (int) this.producerTrackers.stream().filter(Objects::nonNull).count();
    }

    boolean isFullFor(AgentTracker tracker) {
      if (tracker.identifiable()) {
        return this.producerCount >= maxProducersByClient;
      } else {
        return this.trackingConsumerCount >= maxTrackingConsumersByClient;
      }
    }

    boolean isEmpty() {
      return this.producerCount == 0 && this.trackingConsumerCount == 0;
    }

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    // deliberately side-effect free: a predicate that closes a connection and mutates the pool
    // makes this class impossible to reason about, and the loop must never close inline
    boolean isDead() {
      return this.closed.get() || !this.client.isOpen();
    }

    private void closeIfEmpty() {
      if (!closed.get()) {
        if (this.isEmpty()) {
          this.close();
        } else {
          LOGGER.debug("Not closing producer manager {} because it is not empty", this.id);
        }
      }
    }

    private void close() {
      if (closed.compareAndSet(false, true)) {
        removeFromPool(this);
        try {
          if (this.client.isOpen()) {
            this.client.close();
          }
        } catch (Exception e) {
          LOGGER.debug("Error while closing client producer connection: ", e.getMessage());
        }
      }
    }

    @Override
    public int compareTo(ClientProducersManager o) {
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
      ClientProducersManager that = (ClientProducersManager) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  private static final class Placement {

    private final ClientProducersManager manager;
    private final CompletableFuture<Void> waitFor;

    private Placement(ClientProducersManager manager, CompletableFuture<Void> waitFor) {
      this.manager = manager;
      this.waitFor = waitFor;
    }

    private static Placement use(ClientProducersManager manager) {
      return new Placement(manager, null);
    }

    private static Placement create() {
      return new Placement(null, null);
    }

    private static Placement waitFor(CompletableFuture<Void> waiter) {
      return new Placement(null, waiter);
    }
  }

  private static final Predicate<Exception> RETRY_ON_TIMEOUT =
      e -> e instanceof TimeoutStreamException;

  /** Result of {@code reserveSlot}: a slot number, or the reason none was reserved. */
  private static final class SlotReservation {

    private static final SlotReservation FULL = new SlotReservation(NO_SLOT, true, false);
    private static final SlotReservation DEAD = new SlotReservation(NO_SLOT, false, true);

    private final byte publisherId;
    private final boolean full;
    private final boolean dead;

    private SlotReservation(byte publisherId, boolean full, boolean dead) {
      this.publisherId = publisherId;
      this.full = full;
      this.dead = dead;
    }

    private static SlotReservation reserved(byte publisherId) {
      return new SlotReservation(publisherId, false, false);
    }
  }

  private static <T> void iterate(Collection<T> trackers, java.util.function.Consumer<T> action) {
    for (T tracker : trackers) {
      if (tracker != null) {
        action.accept(tracker);
      }
    }
  }

  /**
   * Control-plane state owned by the event loop.
   *
   * <p>The rule this class exists to enforce: the loop is the <b>single writer</b> of the
   * connection pool and of the agent states and epochs. Netty I/O threads only post to it and never
   * wait on it.
   */
  static final class CoordinatorState {

    private final NavigableSet<ClientProducersManager> connections = new TreeSet<>();
    private final Map<Long, TrackerState> agents = new HashMap<>();
    // one connection creation at a time per node, so concurrent placements share the connection
    // being opened instead of each opening their own
    private final Set<String> creating = new HashSet<>();
    private final Map<String, List<CompletableFuture<Void>>> waiters = new HashMap<>();
  }

  /** Per-agent control state. Read and written only by the event loop. */
  private static final class TrackerState {

    private final AgentTracker tracker;
    private State state = State.OPENING;
    private long epoch = 1;
    private int attempts;
    // when the current attempt is due, i.e. dispatched immediately or at the end of its back-off
    // delay; consulted by the watchdog to detect an attempt that never called back
    private long nextAttemptAt;

    private TrackerState(AgentTracker tracker) {
      this.tracker = tracker;
    }
  }
}

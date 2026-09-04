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

import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS;
import static com.rabbitmq.stream.impl.CoordinatorUtils.shouldRefreshCandidates;

import com.rabbitmq.stream.StreamException;
import java.util.function.Consumer;

/**
 * Decision logic for the lifecycle of a single subscription, as pure functions of {@code (state,
 * attempt epoch, event)} returning the next state and a side effect.
 *
 * <p>The functions are pure so that the interleavings that matter can be tested without threads,
 * mocks, or a clock; the event loop applies the result by setting its state fields and then running
 * the effect.
 *
 * <p>The attempt epoch is bumped every time an assignment attempt starts. Continuations of an
 * attempt carry the epoch they were started with, so a late one is recognised as stale and becomes
 * a no-op that cleans up whatever it managed to establish. This inverts the semantics of the
 * compare-and-set it replaces: a new trigger <b>supersedes</b> an in-flight attempt rather than
 * being <b>dropped by</b> it, which is what stops a subscription being stranded in {@code
 * RECOVERING} forever.
 *
 * <p>There is deliberately no connection-level state machine or connection epoch: a {@code Client}
 * is created once per connection and never reused, so a replacement connection is a new entry with
 * a new monotonic id, and an event carrying an id that is no longer in the pool is stale by that
 * fact alone.
 */
final class SubscriptionStateMachine {

  private SubscriptionStateMachine() {}

  enum State {
    OPENING,
    ACTIVE,
    RECOVERING,
    CLOSED;

    /**
     * Whether this state is terminal, i.e. no event and no watchdog may bring the subscription
     * back. Reached by user cancellation, stream deletion, and giving up on recovery.
     */
    boolean terminal() {
      return this == CLOSED;
    }
  }

  /**
   * The effect surface of a transition.
   *
   * <p>Every method here must be implemented so it does not block the event loop: the consumer
   * notifications in particular take {@code StreamConsumer}'s lock, which is shared with the
   * offset-tracking coordinator, so they have to run off-loop.
   */
  interface Actions {

    /** Start an assignment attempt for the given epoch, off-loop. */
    void dispatchAssignment(long attemptEpoch);

    /** Start an assignment attempt for the given epoch after the recovery back-off delay. */
    void scheduleAssignment(long attemptEpoch, Throwable cause);

    void markRecovering();

    void markOpen();

    void closeConsumerAfterStreamDeletion(Throwable cause);

    /**
     * Tear down an assignment established by a superseded attempt, or by an attempt whose
     * subscription has since been cancelled: unsubscribe on the broker and release the slot.
     * Without this a live subscription is left on the broker owned by nobody.
     */
    void releaseAssignment();
  }

  static final class TransitionResult {

    private static final Consumer<Actions> NO_OP = a -> {};

    private final State state;
    private final long epoch;
    private final Consumer<Actions> effect;

    private TransitionResult(State state, long epoch, Consumer<Actions> effect) {
      this.state = state;
      this.epoch = epoch;
      this.effect = effect;
    }

    private static TransitionResult noChange(State state, long epoch) {
      return new TransitionResult(state, epoch, NO_OP);
    }

    private static TransitionResult of(State state, long epoch, Consumer<Actions> effect) {
      return new TransitionResult(state, epoch, effect);
    }

    State state() {
      return this.state;
    }

    long epoch() {
      return this.epoch;
    }

    /** Whether there is anything to run, so callers can skip dispatching a no-op. */
    boolean hasEffect() {
      return this.effect != NO_OP;
    }

    void applyEffect(Actions actions) {
      this.effect.accept(actions);
    }
  }

  // --------------------------------------------------------------
  // Decision logic
  // --------------------------------------------------------------

  static TransitionResult onAssignmentSucceeded(State state, long epoch, long eventEpoch) {
    if (isStale(epoch, eventEpoch) || state.terminal()) {
      // a superseded or cancelled attempt still managed to subscribe on the broker: undo it,
      // and in particular never go from CLOSED back to ACTIVE
      return TransitionResult.of(state, epoch, Actions::releaseAssignment);
    }
    return TransitionResult.of(State.ACTIVE, epoch, Actions::markOpen);
  }

  /**
   * @param recoverable whether another attempt is worth making. The caller decides, because the
   *     same exception type means different things depending on which phase failed: a {@link
   *     TimeoutStreamException} from opening a connection is worth retrying, whereas the same
   *     exception from a candidate lookup that has exhausted its retry policy is the end of the
   *     road. {@link #recoverable(Throwable)} is the classifier for the assignment phase.
   */
  static TransitionResult onAssignmentFailed(
      State state, long epoch, long eventEpoch, Throwable cause, boolean recoverable) {
    if (isStale(epoch, eventEpoch) || state.terminal()) {
      return TransitionResult.noChange(state, epoch);
    }
    if (state == State.OPENING) {
      // initial subscription does not retry: the failure is reported to the caller of subscribe()
      return TransitionResult.noChange(State.CLOSED, epoch);
    }
    if (!recoverable) {
      return TransitionResult.of(
          State.CLOSED, epoch, a -> a.closeConsumerAfterStreamDeletion(cause));
    }
    long newEpoch = epoch + 1;
    return TransitionResult.of(
        State.RECOVERING, newEpoch, a -> a.scheduleAssignment(newEpoch, cause));
  }

  static TransitionResult onConnectionLost(State state, long epoch) {
    return onDisruption(state, epoch);
  }

  static TransitionResult onStreamUnavailable(State state, long epoch) {
    return onDisruption(state, epoch);
  }

  private static TransitionResult onDisruption(State state, long epoch) {
    if (state.terminal()) {
      return TransitionResult.noChange(state, epoch);
    }
    if (state == State.OPENING) {
      // the in-flight initial attempt will fail and surface to the caller of subscribe()
      return TransitionResult.noChange(state, epoch);
    }
    long newEpoch = epoch + 1;
    if (state == State.RECOVERING) {
      // supersede the in-flight attempt instead of being dropped by it
      return TransitionResult.of(State.RECOVERING, newEpoch, a -> a.dispatchAssignment(newEpoch));
    }
    return TransitionResult.of(
        State.RECOVERING,
        newEpoch,
        a -> {
          a.markRecovering();
          a.dispatchAssignment(newEpoch);
        });
  }

  static TransitionResult onCancelled(State state, long epoch) {
    if (state.terminal()) {
      return TransitionResult.noChange(state, epoch);
    }
    // bumping the epoch invalidates any in-flight attempt, whose success event then releases
    // whatever it established
    return TransitionResult.of(State.CLOSED, epoch + 1, Actions::releaseAssignment);
  }

  static TransitionResult onStreamDeleted(State state, long epoch, Throwable cause) {
    if (state.terminal()) {
      return TransitionResult.noChange(state, epoch);
    }
    return TransitionResult.of(
        State.CLOSED, epoch + 1, a -> a.closeConsumerAfterStreamDeletion(cause));
  }

  /**
   * Whether a failed assignment is worth another attempt, mirroring the classification the blocking
   * recovery loop performs today.
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

  private static boolean isStale(long epoch, long eventEpoch) {
    return eventEpoch != epoch;
  }
}

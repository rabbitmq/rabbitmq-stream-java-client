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

import static com.rabbitmq.stream.impl.SubscriptionStateMachine.State.ACTIVE;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.State.CLOSED;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.State.OPENING;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.State.RECOVERING;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onAssignmentFailed;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onAssignmentSucceeded;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onCancelled;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onConnectionLost;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onStreamDeleted;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.onStreamUnavailable;
import static com.rabbitmq.stream.impl.SubscriptionStateMachine.recoverable;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.impl.CoordinatorUtils.ClientClosedException;
import com.rabbitmq.stream.impl.SubscriptionStateMachine.Actions;
import com.rabbitmq.stream.impl.SubscriptionStateMachine.TransitionResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** No threads, no mocks, no clock: only the decision logic. */
public class SubscriptionStateMachineTest {

  static final Throwable CONNECTION_ERROR = new ConnectionStreamException("connection closed");
  static final Throwable STREAM_GONE = new StreamDoesNotExistException("stream");

  RecordingActions actions = new RecordingActions();

  @Test
  void assignmentSuccessMakesTheSubscriptionActive() {
    TransitionResult r = run(onAssignmentSucceeded(RECOVERING, 3, 3));
    assertThat(r.state()).isEqualTo(ACTIVE);
    assertThat(r.epoch()).isEqualTo(3);
    assertThat(actions.calls).containsExactly("markOpen");
  }

  @Test
  void staleAssignmentSuccessReleasesTheAssignmentAndLeavesTheStateAlone() {
    TransitionResult r = run(onAssignmentSucceeded(RECOVERING, 4, 3));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(4);
    assertThat(actions.calls).containsExactly("releaseAssignment");
  }

  @Test
  void assignmentSuccessNeverResurrectsAClosedSubscription() {
    // the leak this replaces: cancel() ran while an assignment was in flight, and the late
    // success left a live subscription on the broker owned by a closed consumer
    TransitionResult r = run(onAssignmentSucceeded(CLOSED, 5, 5));
    assertThat(r.state()).isEqualTo(CLOSED);
    assertThat(actions.calls).containsExactly("releaseAssignment");
  }

  @Test
  void connectionLossWhileActiveStartsRecoveryAndBumpsTheEpoch() {
    TransitionResult r = run(onConnectionLost(ACTIVE, 1));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(2);
    assertThat(actions.calls).containsExactly("markRecovering", "dispatchAssignment(2)");
  }

  @Test
  void connectionLossWhileRecoveringSupersedesTheInFlightAttempt() {
    // the compare-and-set this replaces dropped the new trigger, which is how a subscription
    // ended up stranded in RECOVERING with nothing scheduled
    TransitionResult r = run(onConnectionLost(RECOVERING, 7));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(8);
    assertThat(actions.calls).containsExactly("dispatchAssignment(8)");
  }

  @Test
  void metadataUpdateWhileRecoveringSupersedesTheInFlightAttempt() {
    TransitionResult r = run(onStreamUnavailable(RECOVERING, 2));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(3);
    assertThat(actions.calls).containsExactly("dispatchAssignment(3)");
  }

  @Test
  void twoTriggersInTheSameTickLeaveExactlyOneCurrentAttempt() {
    TransitionResult first = run(onConnectionLost(ACTIVE, 1));
    TransitionResult second = run(onStreamUnavailable(first.state(), first.epoch()));
    assertThat(second.epoch()).isEqualTo(3);
    // the attempt started by the first trigger is now stale and cleans up after itself
    RecordingActions late = new RecordingActions();
    TransitionResult r = onAssignmentSucceeded(second.state(), second.epoch(), first.epoch());
    r.applyEffect(late);
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(late.calls).containsExactly("releaseAssignment");
  }

  @Test
  void theReplacementConnectionDyingBeforeTheAssignmentCompletesStartsAFreshAttempt() {
    TransitionResult first = run(onConnectionLost(ACTIVE, 1));
    TransitionResult second = run(onConnectionLost(first.state(), first.epoch()));
    assertThat(second.state()).isEqualTo(RECOVERING);
    assertThat(second.epoch()).isEqualTo(3);
    // the failure of the superseded attempt must not disturb the current one
    RecordingActions stale = new RecordingActions();
    TransitionResult r =
        onAssignmentFailed(second.state(), second.epoch(), first.epoch(), CONNECTION_ERROR, true);
    r.applyEffect(stale);
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(3);
    assertThat(stale.calls).isEmpty();
  }

  @Test
  void recoverableAssignmentFailureSchedulesAnotherAttempt() {
    TransitionResult r = run(onAssignmentFailed(RECOVERING, 2, 2, CONNECTION_ERROR, true));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(3);
    assertThat(actions.calls).containsExactly("scheduleAssignment(3)");
  }

  @Test
  void unrecoverableAssignmentFailureClosesTheConsumer() {
    TransitionResult r = run(onAssignmentFailed(RECOVERING, 2, 2, STREAM_GONE, false));
    assertThat(r.state()).isEqualTo(CLOSED);
    assertThat(r.state().terminal()).isTrue();
    assertThat(actions.calls).containsExactly("closeConsumerAfterStreamDeletion");
  }

  @Test
  void exhaustedCandidateLookupIsTerminalEvenThoughItsExceptionLooksRefreshable() {
    Throwable lookupGaveUp = new TimeoutStreamException("candidate lookup gave up");
    assertThat(recoverable(lookupGaveUp)).isTrue();
    TransitionResult r = run(onAssignmentFailed(RECOVERING, 2, 2, lookupGaveUp, false));
    assertThat(r.state()).isEqualTo(CLOSED);
    assertThat(actions.calls).containsExactly("closeConsumerAfterStreamDeletion");
  }

  @Test
  void staleAssignmentFailureIsIgnored() {
    TransitionResult r = run(onAssignmentFailed(RECOVERING, 6, 5, CONNECTION_ERROR, true));
    assertThat(r.state()).isEqualTo(RECOVERING);
    assertThat(r.epoch()).isEqualTo(6);
    assertThat(actions.calls).isEmpty();
  }

  @Test
  void disruptionWhileOpeningDoesNotRecover() {
    // initial subscription does not retry: the in-flight attempt reports to subscribe()'s caller
    assertThat(run(onConnectionLost(OPENING, 1)).state()).isEqualTo(OPENING);
    assertThat(run(onStreamUnavailable(OPENING, 1)).state()).isEqualTo(OPENING);
    assertThat(actions.calls).isEmpty();
  }

  @Test
  void assignmentFailureWhileOpeningClosesWithoutRetrying() {
    TransitionResult r = run(onAssignmentFailed(OPENING, 1, 1, CONNECTION_ERROR, true));
    assertThat(r.state()).isEqualTo(CLOSED);
    assertThat(actions.calls).isEmpty();
  }

  @Test
  void cancellingDuringRecoveryClosesAndCleansUpTheLateAssignment() {
    TransitionResult recovering = run(onConnectionLost(ACTIVE, 1));
    RecordingActions onCancel = new RecordingActions();
    TransitionResult cancelled = onCancelled(recovering.state(), recovering.epoch());
    cancelled.applyEffect(onCancel);
    assertThat(cancelled.state()).isEqualTo(CLOSED);
    assertThat(cancelled.epoch()).isEqualTo(3);
    assertThat(onCancel.calls).containsExactly("releaseAssignment");

    RecordingActions late = new RecordingActions();
    TransitionResult r =
        onAssignmentSucceeded(cancelled.state(), cancelled.epoch(), recovering.epoch());
    r.applyEffect(late);
    assertThat(r.state()).isEqualTo(CLOSED);
    assertThat(late.calls).containsExactly("releaseAssignment");
  }

  @Test
  void streamDeletedMidRecoveryIsTerminal() {
    TransitionResult deleted = run(onStreamDeleted(RECOVERING, 4, STREAM_GONE));
    assertThat(deleted.state()).isEqualTo(CLOSED);
    assertThat(deleted.state().terminal()).isTrue();
    assertThat(actions.calls).containsExactly("closeConsumerAfterStreamDeletion");
  }

  @Test
  void everyEventOnAClosedSubscriptionIsANoOp() {
    List<TransitionResult> results =
        List.of(
            onConnectionLost(CLOSED, 9),
            onStreamUnavailable(CLOSED, 9),
            onCancelled(CLOSED, 9),
            onStreamDeleted(CLOSED, 9, STREAM_GONE),
            onAssignmentFailed(CLOSED, 9, 9, CONNECTION_ERROR, true));
    for (TransitionResult r : results) {
      r.applyEffect(actions);
      assertThat(r.state()).isEqualTo(CLOSED);
      assertThat(r.epoch()).isEqualTo(9);
    }
    assertThat(actions.calls).isEmpty();
  }

  @Test
  void failureClassificationMatchesTheBlockingLoopItReplaces() {
    assertThat(recoverable(new ConnectionStreamException("closed"))).isTrue();
    assertThat(recoverable(new ClientClosedException())).isTrue();
    assertThat(recoverable(new StreamNotAvailableException("stream"))).isTrue();
    assertThat(
            recoverable(
                new StreamException(
                    "already exists", Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS)))
        .isTrue();
    assertThat(recoverable(new StreamDoesNotExistException("stream"))).isFalse();
    assertThat(recoverable(new IllegalStateException("boom"))).isFalse();
    assertThat(recoverable(null)).isFalse();
  }

  private TransitionResult run(TransitionResult result) {
    result.applyEffect(this.actions);
    return result;
  }

  private static class RecordingActions implements Actions {

    private final List<String> calls = new ArrayList<>();

    @Override
    public void dispatchAssignment(long attemptEpoch) {
      this.calls.add("dispatchAssignment(" + attemptEpoch + ")");
    }

    @Override
    public void scheduleAssignment(long attemptEpoch, Throwable cause) {
      this.calls.add("scheduleAssignment(" + attemptEpoch + ")");
    }

    @Override
    public void markRecovering() {
      this.calls.add("markRecovering");
    }

    @Override
    public void markOpen() {
      this.calls.add("markOpen");
    }

    @Override
    public void closeConsumerAfterStreamDeletion(Throwable cause) {
      this.calls.add("closeConsumerAfterStreamDeletion");
    }

    @Override
    public void releaseAssignment() {
      this.calls.add("releaseAssignment");
    }
  }
}

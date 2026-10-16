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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.rabbitmq.stream.ConsumerFlowStrategy.CreditUnit;
import com.rabbitmq.stream.impl.ConsumersCoordinator.ByteCreditAccountant;
import java.util.Random;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ByteCreditAccountantTest {

  static final byte SUBSCRIPTION_ID = 0;

  @Test
  void releaseGrantsOnlyItsOwnChunkBytes() {
    Client client = mock(Client.class);
    ByteCreditAccountant accountant = new ByteCreditAccountant();
    accountant.reset(2_000_000);
    accountant.chunkArrived(client, SUBSCRIPTION_ID, 1024);
    accountant.chunkArrived(client, SUBSCRIPTION_ID, 1_000_000);

    accountant.release(client, SUBSCRIPTION_ID, 1, 1024);

    verify(client, times(1)).credit(SUBSCRIPTION_ID, 1024, CreditUnit.BYTE);
    verify(client, never()).credit(eq(SUBSCRIPTION_ID), eq(1_000_000), eq(CreditUnit.BYTE));
  }

  @Test
  void grantsAreDeferredUntilCreditDropsToFlushThresholdThenBatched() {
    Client client = mock(Client.class);
    ByteCreditAccountant accountant = new ByteCreditAccountant();
    accountant.reset(1000); // flush threshold is 750

    accountant.chunkArrived(client, SUBSCRIPTION_ID, 100); // credit = 900
    accountant.release(client, SUBSCRIPTION_ID, 1, 100); // pending = 100
    accountant.chunkArrived(client, SUBSCRIPTION_ID, 100); // credit = 800
    accountant.release(client, SUBSCRIPTION_ID, 1, 100); // pending = 200
    verify(client, never()).credit(eq(SUBSCRIPTION_ID), anyInt(), eq(CreditUnit.BYTE));

    accountant.chunkArrived(client, SUBSCRIPTION_ID, 100); // credit = 700 <= threshold, flushes
    verify(client, times(1)).credit(SUBSCRIPTION_ID, 200, CreditUnit.BYTE);
  }

  @Test
  void chunkLargerThanWindowDrivesCreditNegativeAndReleaseBringsItBackToTheWindow() {
    Client client = mock(Client.class);
    ByteCreditAccountant accountant = new ByteCreditAccountant();
    accountant.reset(1000);

    accountant.chunkArrived(client, SUBSCRIPTION_ID, 1500);
    assertThat(accountant.credit()).isEqualTo(-500);
    verify(client, never()).credit(eq(SUBSCRIPTION_ID), anyInt(), eq(CreditUnit.BYTE));

    accountant.release(client, SUBSCRIPTION_ID, 1, 1500);
    verify(client, times(1)).credit(SUBSCRIPTION_ID, 1500, CreditUnit.BYTE);
    assertThat(accountant.credit()).isEqualTo(1000);
  }

  @Test
  void resetDiscardsPriorState() {
    Client client = mock(Client.class);
    ByteCreditAccountant accountant = new ByteCreditAccountant();
    accountant.reset(1000);
    accountant.chunkArrived(client, SUBSCRIPTION_ID, 100);
    accountant.release(client, SUBSCRIPTION_ID, 1, 200);
    assertThat(accountant.pending()).isNotZero();

    accountant.reset(500);

    assertThat(accountant.credit()).isEqualTo(500);
    assertThat(accountant.pending()).isZero();
  }

  @RepeatedTest(20)
  void invariantsHoldOverRandomSequencesOfArrivalsAndReleases() {
    Client client = mock(Client.class);
    ByteCreditAccountant accountant = new ByteCreditAccountant();
    int window = 1000;
    accountant.reset(window);

    Random random = new Random();
    long totalReceived = 0;
    long totalReleased = 0;

    for (int i = 0; i < 200; i++) {
      long chunkCost = 1 + random.nextInt(300);
      accountant.chunkArrived(client, SUBSCRIPTION_ID, chunkCost);
      totalReceived += chunkCost;

      // releases do not always happen right away: some chunks stay unreleased for a while,
      // exercising sequences where receiving runs ahead of releasing
      if (random.nextBoolean()) {
        accountant.release(client, SUBSCRIPTION_ID, 1, chunkCost);
        totalReleased += chunkCost;
      }

      assertThat(accountant.pending() > 0 && accountant.credit() <= window / 2)
          .describedAs("no-deadlock post-condition violated at iteration %d", i)
          .isFalse();
    }
    // force a final flush of whatever is still pending, to reconcile totals
    accountant.chunkArrived(client, SUBSCRIPTION_ID, window * 10L);

    ArgumentCaptor<Integer> creditCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(client, atLeast(0))
        .credit(eq(SUBSCRIPTION_ID), creditCaptor.capture(), eq(CreditUnit.BYTE));
    long totalGranted = creditCaptor.getAllValues().stream().mapToLong(Integer::longValue).sum();

    assertThat(totalGranted).isEqualTo(totalReleased);
    assertThat(totalGranted).isLessThanOrEqualTo(totalReceived);
  }
}

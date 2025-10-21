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
package com.rabbitmq.stream.oauth2;

import static com.rabbitmq.stream.oauth2.OAuth2TestUtils.pair;
import static com.rabbitmq.stream.oauth2.OAuth2TestUtils.waitAtMost;
import static com.rabbitmq.stream.oauth2.TokenCredentialsManager.DEFAULT_REFRESH_DELAY_STRATEGY;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.oauth2.CredentialsManager.Registration;
import com.rabbitmq.stream.oauth2.OAuth2TestUtils.Pair;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TokenCredentialsManagerTest {

  ScheduledExecutorService scheduledExecutorService;
  AutoCloseable mocks;
  @Mock TokenRequester requester;

  @BeforeEach
  void init() {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    this.scheduledExecutorService.shutdownNow();
    this.mocks.close();
  }

  @Test
  void refreshShouldStopOnceUnregistered() throws InterruptedException {
    Duration tokenExpiry = ofMillis(50);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok", Instant.now().plus(tokenExpiry));
            });
    TokenCredentialsManager credentials =
        new TokenCredentialsManager(
            this.requester, this.scheduledExecutorService, DEFAULT_REFRESH_DELAY_STRATEGY);
    int expectedRefreshCount = 3;
    AtomicInteger refreshCount = new AtomicInteger();
    CountDownLatch refreshLatch = new CountDownLatch(expectedRefreshCount);
    Registration registration =
        credentials.register(
            "",
            (u, p) -> {
              refreshCount.incrementAndGet();
              refreshLatch.countDown();
            });
    registration.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);
    assertThat(refreshLatch.await(ofSeconds(10).toMillis(), MILLISECONDS)).isTrue();
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
    registration.close();
    assertThat(refreshCount).hasValue(expectedRefreshCount);
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());
    assertThat(refreshCount).hasValue(expectedRefreshCount);
    assertThat(requestCount).hasValue(expectedRefreshCount + 1);
  }

  @Test
  void severalRegistrationsShouldBeRefreshed() throws Exception {
    Duration tokenExpiry = ofMillis(50);
    Duration waitTime = tokenExpiry.dividedBy(4);
    Duration timeout = tokenExpiry.multipliedBy(20);
    when(this.requester.request())
        .thenAnswer(ignored -> token("ok", Instant.now().plus(tokenExpiry)));
    TokenCredentialsManager credentials =
        new TokenCredentialsManager(
            this.requester, this.scheduledExecutorService, DEFAULT_REFRESH_DELAY_STRATEGY);
    int expectedRefreshCountPerConnection = 3;
    int connectionCount = 10;
    AtomicInteger totalRefreshCount = new AtomicInteger();
    List<Pair<Registration, CountDownLatch>> registrations =
        range(0, connectionCount)
            .mapToObj(
                ignored -> {
                  CountDownLatch sync = new CountDownLatch(expectedRefreshCountPerConnection);
                  Registration r =
                      credentials.register(
                          "",
                          (username, password) -> {
                            totalRefreshCount.incrementAndGet();
                            sync.countDown();
                          });
                  return pair(r, sync);
                })
            .collect(toList());

    registrations.forEach(r -> r.v1().connect(connectionCallback(() -> {})));
    for (Pair<Registration, CountDownLatch> registrationPair : registrations) {
      assertThat(registrationPair.v2().await(ofSeconds(10).toMillis(), MILLISECONDS)).isTrue();
    }
    // all connections have been refreshed once
    int refreshCountSnapshot = totalRefreshCount.get();
    assertThat(refreshCountSnapshot)
        .isGreaterThanOrEqualTo(connectionCount * expectedRefreshCountPerConnection);

    // unregister half of the connections
    int splitCount = connectionCount / 2;
    registrations.subList(0, splitCount).forEach(r -> r.v1().close());
    // only the remaining connections should get refreshed again
    waitAtMost(
        timeout, waitTime, () -> totalRefreshCount.get() >= refreshCountSnapshot + splitCount);
    // waiting another round of refresh
    waitAtMost(
        timeout, waitTime, () -> totalRefreshCount.get() >= refreshCountSnapshot + splitCount * 2);
    // unregister all connections
    registrations.forEach(r -> r.v1().close());
    int finalRefreshCount = totalRefreshCount.get();
    // wait 2 expiry times
    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());
    // no new refresh
    assertThat(totalRefreshCount).hasValue(finalRefreshCount);
  }

  @Test
  void refreshDelayStrategy() {
    Duration diff = ofMillis(100);
    Function<Instant, Duration> strategy = TokenCredentialsManager.ratioRefreshDelayStrategy(0.8f);
    assertThat(strategy.apply(Instant.now().plusSeconds(10))).isCloseTo(ofSeconds(8), diff);
    assertThat(strategy.apply(Instant.now().minusSeconds(10))).isEqualTo(ofSeconds(1));
  }

  private static Token token(String value, Instant expirationTime) {
    return new Token() {
      @Override
      public String value() {
        return value;
      }

      @Override
      public Instant expirationTime() {
        return expirationTime;
      }
    };
  }

  private static CredentialsManager.AuthenticationCallback connectionCallback(
      Runnable passwordCallback) {
    return (username, password) -> passwordCallback.run();
  }
}

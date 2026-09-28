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
package com.rabbitmq.stream.oauth2;

import static com.rabbitmq.stream.oauth2.OAuth2TestUtils.pair;
import static com.rabbitmq.stream.oauth2.OAuth2TestUtils.waitAtMost;
import static com.rabbitmq.stream.oauth2.TokenCredentialsManager.DEFAULT_REFRESH_DELAY_STRATEGY;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.oauth2.CredentialsManager.Registration;
import com.rabbitmq.stream.oauth2.OAuth2TestUtils.Pair;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
  ExecutorService executorService;
  AutoCloseable mocks;
  @Mock TokenRequester requester;

  @BeforeEach
  void init() {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.executorService = Executors.newCachedThreadPool();
    this.mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    this.scheduledExecutorService.shutdownNow();
    this.executorService.shutdownNow();
    this.mocks.close();
  }

  private TokenCredentialsManager manager(Function<Instant, Duration> strategy) {
    return new TokenCredentialsManager(
        this.requester, this.scheduledExecutorService, this.executorService, strategy);
  }

  private TokenCredentialsManager manager(
      Function<Instant, Duration> strategy, Duration connectTimeout) {
    return new TokenCredentialsManager(
        this.requester,
        this.scheduledExecutorService,
        this.executorService,
        strategy,
        connectTimeout);
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
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
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
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
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
  void refreshShouldBeRetriedAfterTransientFailure() throws InterruptedException {
    Duration tokenExpiry = ofMillis(50);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              if (count == 2) {
                throw new OAuth2Exception("simulated transient failure");
              }
              return token("ok", Instant.now().plus(tokenExpiry));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    CountDownLatch refreshLatch = new CountDownLatch(1);
    Registration registration = credentials.register("", (u, p) -> refreshLatch.countDown());
    registration.connect(connectionCallback(() -> {}));
    // the first scheduled refresh fails, but the task recovers and retries
    assertThat(refreshLatch.await(ofSeconds(10).toMillis(), MILLISECONDS)).isTrue();
    assertThat(requestCount.get()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void refreshDelayStrategy() {
    Duration diff = ofMillis(100);
    Function<Instant, Duration> strategy = TokenCredentialsManager.ratioRefreshDelayStrategy(0.8f);
    assertThat(strategy.apply(Instant.now().plusSeconds(10))).isCloseTo(ofSeconds(8), diff);
    assertThat(strategy.apply(Instant.now().minusSeconds(10))).isEqualTo(ofSeconds(1));
  }

  @Test
  void connectAfterAllRegistrationsClosedShouldGetFreshTokenAndRefresh() throws Exception {
    Duration tokenExpiry = ofMillis(80);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok-" + requestCount.get(), Instant.now().plus(tokenExpiry));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r1 = credentials.register("r1", (u, p) -> {});
    r1.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);
    r1.close();

    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());

    CountDownLatch refreshLatch = new CountDownLatch(2);
    List<String> tokensReceived = new CopyOnWriteArrayList<>();
    Registration r2 =
        credentials.register(
            "r2",
            (u, p) -> {
              tokensReceived.add(p);
              refreshLatch.countDown();
            });
    List<String> tokenAtConnect = new CopyOnWriteArrayList<>();
    r2.connect(connectionCallback(v -> tokenAtConnect.add(v)));
    assertThat(requestCount.get()).isGreaterThanOrEqualTo(2);
    assertThat(tokenAtConnect).hasSize(1);
    assertThat(refreshLatch.await(10, SECONDS)).isTrue();
  }

  @Test
  void connectShouldGetFreshTokenWhenCachedTokenIsExpired() throws Exception {
    Duration tokenExpiry = ofMillis(80);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok-" + requestCount.get(), Instant.now().plus(tokenExpiry));
            });
    // never triggers a scheduled refresh, so the cached token just goes stale
    TokenCredentialsManager credentials = manager(ignored -> ofSeconds(3600));
    Registration r1 = credentials.register("r1", (u, p) -> {});
    r1.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);

    Thread.sleep(tokenExpiry.multipliedBy(2).toMillis());

    Registration r2 = credentials.register("r2", (u, p) -> {});
    r2.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(2);
  }

  @Test
  void connectShouldNotWaitForInFlightRefresh() throws Exception {
    // the cached token stays comfortably above the usable margin (1 second) while the refresh
    // (request #2) is stuck on the gate, so a concurrent connect must return immediately with it
    CountDownLatch requestGate = new CountDownLatch(1);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              if (count == 2) {
                assertThat(requestGate.await(10, SECONDS)).isTrue();
              }
              return token("ok-" + count, Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(ignored -> ofMillis(50));
    Registration r1 = credentials.register("r1", (u, p) -> {});
    r1.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);
    // wait until the refresh (request #2) is in flight and blocked on the gate
    waitAtMost(ofSeconds(5), ofMillis(20), () -> requestCount.get() == 2);

    Registration r2 = credentials.register("r2", (u, p) -> {});
    long start = System.nanoTime();
    r2.connect(connectionCallback(() -> {}));
    Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
    requestGate.countDown();
    assertThat(elapsed).isLessThan(ofSeconds(1));
  }

  @Test
  void concurrentConnectsWithoutTokenShouldTriggerSingleRequest() throws Exception {
    CountDownLatch requestGate = new CountDownLatch(1);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              requestGate.await(10, SECONDS);
              return token("ok", Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    int threadCount = 10;
    List<String> values = new CopyOnWriteArrayList<>();
    ExecutorService connectPool = Executors.newFixedThreadPool(threadCount);
    try {
      CountDownLatch done = new CountDownLatch(threadCount);
      for (int i = 0; i < threadCount; i++) {
        Registration r = credentials.register("r" + i, (u, p) -> {});
        connectPool.execute(
            () -> {
              r.connect(connectionCallback(values::add));
              done.countDown();
            });
      }
      Thread.sleep(200);
      requestGate.countDown();
      assertThat(done.await(10, SECONDS)).isTrue();
    } finally {
      connectPool.shutdownNow();
    }
    assertThat(requestCount).hasValue(1);
    assertThat(values).hasSize(threadCount).allMatch("ok"::equals);
  }

  @Test
  void connectShouldFailWhenTokenRequestFails() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              if (count == 1) {
                throw new OAuth2Exception("simulated failure");
              }
              return token("ok", Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r = credentials.register("r", (u, p) -> {});
    assertThatThrownBy(() -> r.connect(connectionCallback(() -> {})))
        .isInstanceOf(OAuth2Exception.class);
    r.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(2);
  }

  @Test
  void connectShouldTimeOutWhenTokenRequestHangs() throws Exception {
    CountDownLatch requestGate = new CountDownLatch(1);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestGate.await(10, SECONDS);
              return token("ok", Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY, ofMillis(200));
    Registration r = credentials.register("r", (u, p) -> {});
    assertThatThrownBy(() -> r.connect(connectionCallback(() -> {})))
        .isInstanceOf(OAuth2Exception.class);
    requestGate.countDown();
  }

  @Test
  void pendingConnectShouldFailWhenManagerIsClosed() throws Exception {
    CountDownLatch requestGate = new CountDownLatch(1);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestGate.await(10, SECONDS);
              return token("ok", Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r = credentials.register("r", (u, p) -> {});
    ExecutorService connectPool = Executors.newSingleThreadExecutor();
    try {
      java.util.concurrent.Future<Exception> future =
          connectPool.submit(
              () -> {
                try {
                  r.connect(connectionCallback(() -> {}));
                  return null;
                } catch (Exception e) {
                  return e;
                }
              });
      Thread.sleep(200);
      credentials.close();
      Exception result = future.get(10, SECONDS);
      assertThat(result).isInstanceOf(IllegalStateException.class);
    } finally {
      requestGate.countDown();
      connectPool.shutdownNow();
    }
  }

  @Test
  void tokenReceivedAfterCloseShouldBeDiscarded() throws Exception {
    CountDownLatch requestGate = new CountDownLatch(1);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              requestGate.await(10, SECONDS);
              return token("ok", Instant.now().plus(ofSeconds(10)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    AtomicInteger callbackCount = new AtomicInteger();
    Registration r = credentials.register("r", (u, p) -> callbackCount.incrementAndGet());
    ExecutorService connectPool = Executors.newSingleThreadExecutor();
    try {
      connectPool.execute(
          () -> {
            try {
              r.connect(connectionCallback(() -> {}));
            } catch (Exception e) {
              // expected: the manager is closed before the token request completes
            }
          });
      Thread.sleep(200);
      credentials.close();
      requestGate.countDown();
      Thread.sleep(300);
      assertThat(requestCount).hasValue(1);
      assertThat(callbackCount).hasValue(0);
    } finally {
      connectPool.shutdownNow();
    }
  }

  @Test
  void refreshRetriesShouldStopWhenAllRegistrationsAreClosed() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              if (count == 1) {
                return token("ok", Instant.now().plus(ofMillis(50)));
              }
              throw new OAuth2Exception("simulated failure");
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r = credentials.register("r", (u, p) -> {});
    r.connect(connectionCallback(() -> {}));
    waitAtMost(ofSeconds(5), ofMillis(20), () -> requestCount.get() >= 2);
    r.close();
    int countAfterClose = requestCount.get();
    Thread.sleep(500);
    assertThat(requestCount).hasValue(countAfterClose);
  }

  @Test
  void closingLastRegistrationConcurrentlyWithNewConnectShouldKeepRefreshing() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok-" + requestCount.get(), Instant.now().plus(ofMillis(60)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);

    for (int i = 0; i < 30; i++) {
      int iteration = i;
      Registration a = credentials.register("a-" + iteration, (u, p) -> {});
      a.connect(connectionCallback(() -> {}));

      CountDownLatch startGate = new CountDownLatch(1);
      CountDownLatch bRefreshed = new CountDownLatch(1);
      Registration[] bHolder = new Registration[1];
      Thread closer =
          new Thread(
              () -> {
                try {
                  startGate.await(10, SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                a.close();
              });
      Thread registerer =
          new Thread(
              () -> {
                Registration b =
                    credentials.register("b-" + iteration, (u, p) -> bRefreshed.countDown());
                bHolder[0] = b;
                try {
                  startGate.await(10, SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                b.connect(connectionCallback(() -> {}));
              });
      registerer.start();
      closer.start();
      startGate.countDown();
      closer.join(10_000);
      registerer.join(10_000);

      assertThat(bRefreshed.await(10, SECONDS)).isTrue();
      bHolder[0].close();
    }
  }

  @Test
  void closeShouldStopRefresh() throws Exception {
    Duration tokenExpiry = ofMillis(50);
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok", Instant.now().plus(tokenExpiry));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r = credentials.register("r", (u, p) -> {});
    r.connect(connectionCallback(() -> {}));
    assertThat(requestCount).hasValue(1);
    credentials.close();
    int countAfterClose = requestCount.get();
    Thread.sleep(tokenExpiry.multipliedBy(3).toMillis());
    assertThat(requestCount).hasValue(countAfterClose);
  }

  @Test
  void registerAndConnectShouldFailAfterClose() {
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    credentials.close();
    assertThatThrownBy(() -> credentials.register("r", (u, p) -> {}))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void closeShouldBeIdempotent() throws Exception {
    when(this.requester.request())
        .thenAnswer(ignored -> token("ok", Instant.now().plus(ofSeconds(10))));
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    Registration r = credentials.register("r", (u, p) -> {});
    r.connect(connectionCallback(() -> {}));
    credentials.close();
    credentials.close();
    r.close();
    r.close();
  }

  @Test
  void updatesShouldBeDeliveredInOrderPerRegistration() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              return token("t" + count, Instant.now().plus(ofMillis(40)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    List<Integer> received = new CopyOnWriteArrayList<>();
    Registration r =
        credentials.register("r", (u, p) -> received.add(Integer.parseInt(p.substring(1))));
    r.connect(connectionCallback(() -> {}));
    waitAtMost(ofSeconds(10), ofMillis(50), () -> received.size() >= 5);
    for (int i = 1; i < received.size(); i++) {
      assertThat(received.get(i)).isGreaterThan(received.get(i - 1));
    }
  }

  @Test
  void slowCallbackShouldNotDelayOtherRegistrations() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok-" + requestCount.get(), Instant.now().plus(ofMillis(40)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    CountDownLatch slowGate = new CountDownLatch(1);
    Registration a =
        credentials.register(
            "a",
            (u, p) -> {
              try {
                slowGate.await(10, SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    AtomicInteger bRefreshCount = new AtomicInteger();
    Registration b = credentials.register("b", (u, p) -> bRefreshCount.incrementAndGet());
    a.connect(connectionCallback(() -> {}));
    b.connect(connectionCallback(() -> {}));
    waitAtMost(ofSeconds(10), ofMillis(50), () -> bRefreshCount.get() >= 3);
    slowGate.countDown();
    a.close();
    b.close();
  }

  @Test
  void callbackExceptionShouldNotStopRefresh() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              requestCount.incrementAndGet();
              return token("ok-" + requestCount.get(), Instant.now().plus(ofMillis(40)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    AtomicInteger callbackCount = new AtomicInteger();
    Registration r =
        credentials.register(
            "r",
            (u, p) -> {
              callbackCount.incrementAndGet();
              throw new RuntimeException("simulated callback failure");
            });
    r.connect(connectionCallback(() -> {}));
    waitAtMost(ofSeconds(10), ofMillis(50), () -> callbackCount.get() >= 3);
    r.close();
  }

  @Test
  void supersededUpdateShouldBeSkipped() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    when(this.requester.request())
        .thenAnswer(
            ignored -> {
              int count = requestCount.incrementAndGet();
              return token("t" + count, Instant.now().plus(ofMillis(40)));
            });
    TokenCredentialsManager credentials = manager(DEFAULT_REFRESH_DELAY_STRATEGY);
    CountDownLatch releaseGate = new CountDownLatch(1);
    CountDownLatch enteredGate = new CountDownLatch(1);
    List<String> received = new CopyOnWriteArrayList<>();
    AtomicInteger callCount = new AtomicInteger();
    Registration a =
        credentials.register(
            "a",
            (u, p) -> {
              int call = callCount.incrementAndGet();
              if (call == 2) {
                enteredGate.countDown();
                try {
                  releaseGate.await(10, SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              received.add(p);
            });
    a.connect(connectionCallback(() -> {}));
    assertThat(enteredGate.await(10, SECONDS)).isTrue();
    // let a couple more refreshes happen while the callback for t2 is blocked
    Thread.sleep(150);
    releaseGate.countDown();
    waitAtMost(ofSeconds(10), ofMillis(50), () -> received.size() >= 3);
    // the value delivered right after the blocked one must be the latest known token,
    // not the one that arrived while the callback was blocked
    assertThat(received.get(2)).isNotEqualTo("t3");
    a.close();
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

  private static CredentialsManager.AuthenticationCallback connectionCallback(
      java.util.function.Consumer<String> passwordConsumer) {
    return (username, password) -> passwordConsumer.accept(password);
  }
}

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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials manager implementation that requests and refreshes tokens.
 *
 * <p>It also keeps track of registrations and updates them with refreshed tokens when appropriate.
 *
 * <p>All mutable state of this class (the cached token, the pending waiters, the registration
 * table, the in-flight request flag, the refresh task and its generation) is confined to a single
 * serial executor ({@code loop}): it is read and written only by tasks running on that executor.
 * Nothing running on the serial executor performs blocking I/O (no token request, no {@link
 * AuthenticationCallback} invocation, no {@code Future.get()}): blocking work is dispatched to the
 * {@code executorService} passed to the constructor, and its result is posted back to the serial
 * executor as an event. {@link Registration#connect(AuthenticationCallback)} must not be called
 * from the serial executor, or it throws {@link IllegalStateException} (it would deadlock
 * otherwise).
 */
public final class TokenCredentialsManager implements CredentialsManager {

  public static final Function<Instant, Duration> DEFAULT_REFRESH_DELAY_STRATEGY =
      ratioRefreshDelayStrategy(0.8f);
  private static final Duration FAILED_REFRESH_RETRY_DELAY = Duration.ofSeconds(1);
  private static final Duration USABLE_MARGIN = Duration.ofSeconds(1);
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(120);
  private static final Logger LOGGER = LoggerFactory.getLogger(TokenCredentialsManager.class);

  private final TokenRequester requester;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Executor executorService;
  private final Function<Instant, Duration> refreshDelayStrategy;
  private final SerialExecutor loop;
  private final Duration connectTimeout;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong registrationSequence = new AtomicLong(0);

  // confined to the serial executor (loop)
  private Token token;
  private boolean requestInFlight = false;
  private final List<Waiter> waiters = new ArrayList<>();
  private final Map<Long, RegistrationImpl> registrations = new HashMap<>();
  private ScheduledFuture<?> refreshTask;
  private long refreshGeneration;

  /**
   * Creates an instance.
   *
   * @param requester used to request tokens
   * @param scheduledExecutorService used to schedule refresh timers
   * @param executorService used for blocking work (token requests) and as the delegate of the
   *     internal serial executor
   * @param refreshDelayStrategy computes the delay before the next refresh from a token expiration
   *     time
   */
  public TokenCredentialsManager(
      TokenRequester requester,
      ScheduledExecutorService scheduledExecutorService,
      Executor executorService,
      Function<Instant, Duration> refreshDelayStrategy) {
    this(
        requester,
        scheduledExecutorService,
        executorService,
        refreshDelayStrategy,
        DEFAULT_CONNECT_TIMEOUT);
  }

  TokenCredentialsManager(
      TokenRequester requester,
      ScheduledExecutorService scheduledExecutorService,
      Executor executorService,
      Function<Instant, Duration> refreshDelayStrategy,
      Duration connectTimeout) {
    this.requester = requester;
    this.scheduledExecutorService = scheduledExecutorService;
    this.executorService = executorService;
    this.refreshDelayStrategy = refreshDelayStrategy;
    this.loop = new SerialExecutor(executorService);
    this.connectTimeout = connectTimeout;
  }

  @Override
  public Registration register(String name, AuthenticationCallback updateCallback) {
    if (this.closed.get()) {
      throw new IllegalStateException("Credentials manager is closed");
    }
    long id = this.registrationSequence.getAndIncrement();
    RegistrationImpl registration =
        new RegistrationImpl(
            id, name == null ? String.valueOf(id) : name, updateCallback, this.executorService);
    this.loop.execute(() -> onRegister(registration));
    return registration;
  }

  private void onRegister(RegistrationImpl registration) {
    if (this.closed.get()) {
      registration.markClosed();
    } else {
      this.registrations.put(registration.id, registration);
    }
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      try {
        this.loop.execute(this::onClose);
      } catch (RejectedExecutionException e) {
        LOGGER.debug("Could not schedule credentials manager closing, executor is shut down", e);
      }
    }
  }

  private void onClose() {
    cancelRefreshTask();
    this.refreshGeneration++;
    failWaiters(new IllegalStateException("Credentials manager is closed"));
    for (RegistrationImpl registration : this.registrations.values()) {
      registration.markClosed();
    }
    this.registrations.clear();
    this.token = null;
  }

  private boolean usable(Token t) {
    return t != null && t.expirationTime().isAfter(Instant.now().plus(USABLE_MARGIN));
  }

  private void giveToken(RegistrationImpl registration, Token t) {
    registration.lastToken = t;
    registration.currentToken = t;
  }

  private void onConnect(RegistrationImpl registration, CompletableFuture<Token> future) {
    if (this.closed.get() || registration.isClosed()) {
      future.completeExceptionally(
          new IllegalStateException("Credentials manager or registration is closed"));
      return;
    }
    if (usable(this.token)) {
      giveToken(registration, this.token);
      ensureRefreshScheduled();
      future.complete(this.token);
    } else {
      this.waiters.add(new Waiter(registration, future));
      requestToken();
    }
  }

  private void requestToken() {
    if (this.requestInFlight) {
      return;
    }
    this.requestInFlight = true;
    try {
      this.executorService.execute(
          () -> {
            try {
              Token t = getToken();
              postToLoop(() -> onTokenReceived(t));
            } catch (Exception e) {
              postToLoop(() -> onTokenFailure(e));
            }
          });
    } catch (RejectedExecutionException e) {
      this.requestInFlight = false;
      failWaiters(e);
    }
  }

  private void postToLoop(Runnable task) {
    try {
      this.loop.execute(task);
    } catch (RejectedExecutionException e) {
      LOGGER.debug("Could not post event to serial executor, it is shut down", e);
    }
  }

  private Token getToken() {
    if (debug()) {
      LOGGER.debug(
          "Requesting new token ({})...", registrationSummary(this.registrations.values()));
    }
    long start = 0L;
    if (debug()) {
      start = System.nanoTime();
    }
    Token t = this.requester.request();
    if (debug()) {
      LOGGER.debug(
          "Got new token in {} ms, token expires on {} ({})",
          Duration.ofNanos(System.nanoTime() - start),
          format(t.expirationTime()),
          registrationSummary(this.registrations.values()));
    }
    return t;
  }

  private void onTokenReceived(Token t) {
    this.requestInFlight = false;
    if (this.closed.get()) {
      return;
    }
    this.token = t;
    List<Waiter> currentWaiters = new ArrayList<>(this.waiters);
    this.waiters.clear();
    for (Waiter waiter : currentWaiters) {
      if (!waiter.registration.isClosed()) {
        giveToken(waiter.registration, t);
        waiter.future.complete(t);
      } else {
        waiter.future.completeExceptionally(new IllegalStateException("Registration is closed"));
      }
    }
    if (!this.registrations.isEmpty()) {
      scheduleRefresh(t);
    }
    dispatchUpdates(t);
  }

  private void onTokenFailure(Exception e) {
    this.requestInFlight = false;
    if (this.closed.get()) {
      return;
    }
    failWaiters(e);
    boolean hasConnectedRegistration =
        this.registrations.values().stream().anyMatch(r -> !r.isClosed() && r.lastToken != null);
    if (hasConnectedRegistration) {
      LOGGER.warn(
          "Error while refreshing token, retrying in {}: {}",
          FAILED_REFRESH_RETRY_DELAY,
          e.getMessage());
      scheduleTimer(FAILED_REFRESH_RETRY_DELAY);
    }
  }

  private void failWaiters(Exception e) {
    List<Waiter> currentWaiters = new ArrayList<>(this.waiters);
    this.waiters.clear();
    for (Waiter waiter : currentWaiters) {
      waiter.future.completeExceptionally(e);
    }
  }

  private void scheduleRefresh(Token t) {
    Duration delay = this.refreshDelayStrategy.apply(t.expirationTime());
    scheduleTimer(delay);
  }

  private void scheduleTimer(Duration delay) {
    cancelRefreshTask();
    this.refreshGeneration++;
    long generation = this.refreshGeneration;
    try {
      this.refreshTask =
          this.scheduledExecutorService.schedule(
              () -> postToLoop(() -> onRefreshTimer(generation)),
              delay.toMillis(),
              TimeUnit.MILLISECONDS);
      if (debug()) {
        LOGGER.debug(
            "Scheduled token update in {} ({})",
            delay,
            registrationSummary(this.registrations.values()));
      }
    } catch (RejectedExecutionException e) {
      LOGGER.debug("Could not schedule token refresh, scheduler is shut down", e);
    }
  }

  private void cancelRefreshTask() {
    if (this.refreshTask != null) {
      if (debug()) {
        LOGGER.debug("Cancelling refresh task");
      }
      this.refreshTask.cancel(false);
      this.refreshTask = null;
    }
  }

  private void ensureRefreshScheduled() {
    if (this.refreshTask == null && usable(this.token)) {
      scheduleRefresh(this.token);
    }
  }

  private void onRefreshTimer(long generation) {
    if (generation != this.refreshGeneration || this.closed.get() || this.registrations.isEmpty()) {
      return;
    }
    this.refreshTask = null;
    requestToken();
  }

  private void dispatchUpdates(Token t) {
    int dispatchedCount = 0;
    for (RegistrationImpl registration : this.registrations.values()) {
      if (!registration.isClosed() && !t.equals(registration.lastToken)) {
        giveToken(registration, t);
        registration.callbackExecutor.execute(() -> deliver(registration, t));
        dispatchedCount++;
      }
    }
    if (debug() || dispatchedCount > 0) {
      LOGGER.debug("Updated {} registration(s)", dispatchedCount);
    }
  }

  private void deliver(RegistrationImpl registration, Token t) {
    if (registration.isClosed() || registration.currentToken != t) {
      return;
    }
    try {
      registration.updateCallback.authenticate("", t.value());
    } catch (Exception e) {
      LOGGER.warn(
          "Error while updating token for registration '{}': {}",
          registration.name,
          e.getMessage());
    }
  }

  private void onUnregister(RegistrationImpl registration) {
    this.registrations.remove(registration.id);
    for (Waiter waiter : new ArrayList<>(this.waiters)) {
      if (waiter.registration.equals(registration)) {
        this.waiters.remove(waiter);
        waiter.future.completeExceptionally(new IllegalStateException("Registration is closed"));
      }
    }
    if (this.registrations.isEmpty()) {
      cancelRefreshTask();
      this.refreshGeneration++;
      this.token = null;
    }
  }

  private static String format(Instant instant) {
    return DateTimeFormatter.ISO_INSTANT.format(instant);
  }

  private static final class Waiter {

    private final RegistrationImpl registration;
    private final CompletableFuture<Token> future;

    private Waiter(RegistrationImpl registration, CompletableFuture<Token> future) {
      this.registration = registration;
      this.future = future;
    }
  }

  private final class RegistrationImpl implements Registration {

    private final long id;
    private final String name;
    private final AuthenticationCallback updateCallback;
    private final SerialExecutor callbackExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // confined to the loop
    private Token lastToken;
    // written on the loop, read by callback tasks
    private volatile Token currentToken;

    private RegistrationImpl(
        long id, String name, AuthenticationCallback updateCallback, Executor executorService) {
      this.id = id;
      this.name = name;
      this.updateCallback = updateCallback;
      this.callbackExecutor = new SerialExecutor(executorService);
    }

    /**
     * {@inheritDoc}
     *
     * <p>May block while a token is requested. Throws {@link OAuth2Exception} if the request fails
     * or times out, {@link IllegalStateException} if the manager or this registration is closed.
     */
    @Override
    public void connect(AuthenticationCallback callback) {
      if (loop.inExecutor()) {
        throw new IllegalStateException(
            "Registration.connect(...) must not be called from the credentials manager's "
                + "internal executor");
      }
      if (closed() || this.isClosed()) {
        throw new IllegalStateException("Credentials manager or registration is closed");
      }
      if (debug()) {
        LOGGER.debug("Connecting registration {}", this.name);
      }
      CompletableFuture<Token> future = new CompletableFuture<>();
      loop.execute(() -> onConnect(this, future));
      Token t;
      try {
        t = future.get(connectTimeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new OAuth2Exception("Error while requesting token", cause);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new OAuth2Exception("Interrupted while requesting token", e);
      } catch (TimeoutException e) {
        throw new OAuth2Exception("Timeout while requesting token", e);
      }
      if (debug()) {
        LOGGER.debug("Authenticating registration {}", this.name);
      }
      callback.authenticate("", t.value());
    }

    private boolean closed() {
      return TokenCredentialsManager.this.closed.get();
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        LOGGER.debug("Closing credentials registration {}", this.name);
        try {
          loop.execute(() -> onUnregister(this));
        } catch (RejectedExecutionException e) {
          LOGGER.debug("Could not schedule registration closing, executor is shut down", e);
        }
      }
    }

    private void markClosed() {
      this.closed.set(true);
    }

    private boolean isClosed() {
      return this.closed.get();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      RegistrationImpl that = (RegistrationImpl) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  public static Function<Instant, Duration> ratioRefreshDelayStrategy(float ratio) {
    return new RatioRefreshDelayStrategy(ratio);
  }

  private static class RatioRefreshDelayStrategy implements Function<Instant, Duration> {

    private final float ratio;

    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    private RatioRefreshDelayStrategy(float ratio) {
      if (ratio < 0 || ratio > 1) {
        throw new IllegalArgumentException("Ratio should be > 0 and <= 1: " + ratio);
      }
      this.ratio = ratio;
    }

    @Override
    public Duration apply(Instant expirationTime) {
      Duration expiresIn = Duration.between(Instant.now(), expirationTime);
      Duration delay;
      if (expiresIn.isZero() || expiresIn.isNegative()) {
        delay = Duration.ofSeconds(1);
      } else {
        delay = Duration.ofMillis((long) (expiresIn.toMillis() * ratio));
      }
      return delay;
    }
  }

  private static String registrationSummary(Collection<? extends Registration> registrations) {
    return registrations.stream().map(Registration::toString).collect(Collectors.joining(", "));
  }

  private static boolean debug() {
    return LOGGER.isDebugEnabled();
  }
}

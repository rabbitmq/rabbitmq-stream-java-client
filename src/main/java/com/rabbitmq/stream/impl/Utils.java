// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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

import static java.lang.String.format;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import io.netty.channel.ConnectTimeoutException;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Utils {

  @SuppressWarnings("rawtypes")
  private static final Consumer NO_OP_CONSUMER = o -> {};

  static final LongConsumer NO_OP_LONG_CONSUMER = someLong -> {};
  static final LongSupplier NO_OP_LONG_SUPPLIER = () -> 0;
  static final X509TrustManager TRUST_EVERYTHING_TRUST_MANAGER = new TrustEverythingTrustManager();
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final Map<Short, String> CONSTANT_LABELS;

  static final String SUBSCRIPTION_PROPERTY_SAC = "single-active-consumer";
  static final String SUBSCRIPTION_PROPERTY_SUPER_STREAM = "super-stream";
  static final String SUBSCRIPTION_PROPERTY_FILTER_PREFIX = "filter.";
  static final String SUBSCRIPTION_PROPERTY_MATCH_UNFILTERED = "match-unfiltered";

  static {
    Map<Short, String> labels = new HashMap<>();
    Arrays.stream(Constants.class.getDeclaredFields())
        .filter(f -> f.getName().startsWith("RESPONSE_CODE_") || f.getName().startsWith("CODE_"))
        .forEach(
            field -> {
              try {
                labels.put(
                    field.getShort(null),
                    field.getName().replace("RESPONSE_CODE_", "").replace("CODE_", ""));
              } catch (IllegalAccessException e) {
                LOGGER.info("Error while trying to access field Constants." + field.getName());
              }
            });
    CONSTANT_LABELS = Collections.unmodifiableMap(labels);
  }

  static final AddressResolver DEFAULT_ADDRESS_RESOLVER = address -> address;
  static final String DEFAULT_USERNAME = "guest";

  private Utils() {}

  @SuppressWarnings("unchecked")
  static <T> Consumer<T> noOpConsumer() {
    return (Consumer<T>) NO_OP_CONSUMER;
  }

  static Runnable makeIdempotent(Runnable action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return () -> {
      if (executed.compareAndSet(false, true)) {
        action.run();
      }
    };
  }

  static <T> Consumer<T> makeIdempotent(Consumer<T> action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return t -> {
      if (executed.compareAndSet(false, true)) {
        action.accept(t);
      }
    };
  }

  static String formatConstant(short value) {
    return value + " (" + CONSTANT_LABELS.getOrDefault(value, "UNKNOWN") + ")";
  }

  static boolean isSac(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return false;
    } else {
      return "true".equals(properties.get("single-active-consumer"));
    }
  }

  static boolean filteringEnabled(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return false;
    } else {
      return properties.keySet().stream()
          .anyMatch(k -> k.startsWith(SUBSCRIPTION_PROPERTY_FILTER_PREFIX));
    }
  }

  static short encodeRequestCode(Short code) {
    return code;
  }

  static short extractResponseCode(Short code) {
    return (short) (code & 0B0111_1111_1111_1111);
  }

  static short encodeResponseCode(Short code) {
    return (short) (code | 0B1000_0000_0000_0000);
  }

  static ClientFactory coordinatorClientFactory(StreamEnvironment environment) {
    String messageFormat =
        "%s. %s. "
            + "This may be due to the usage of a load balancer that makes topology discovery fail. "
            + "Use a custom AddressResolver or the --load-balancer flag if using StreamPerfTest. "
            + "See https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#understanding-connection-logic "
            + "and https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#with-a-load-balancer.";
    return context -> {
      ClientParameters parametersCopy = context.parameters().duplicate();
      Address address = new Address(parametersCopy.host(), parametersCopy.port());
      address = environment.addressResolver().resolve(address);
      parametersCopy.host(address.host()).port(address.port());

      if (context.key() == null) {
        throw new IllegalArgumentException("A key is necessary to create the client connection");
      }

      try {
        return Utils.connectToAdvertisedNodeClientFactory(
                context.key(), context1 -> new Client(context1.parameters()))
            .client(Utils.ClientFactoryContext.fromParameters(parametersCopy).key(context.key()));
      } catch (TimeoutStreamException e) {
        throw new TimeoutStreamException(
            format(messageFormat, e.getMessage(), e.getCause().getMessage(), e.getCause()));
      } catch (StreamException e) {
        if (e.getCause() != null
            && (e.getCause() instanceof UnknownHostException
                || e.getCause() instanceof ConnectTimeoutException)) {
          throw new StreamException(
              format(messageFormat, e.getMessage(), e.getCause().getMessage()), e.getCause());
        } else {
          throw e;
        }
      }
    };
  }

  static ClientFactory connectToAdvertisedNodeClientFactory(
      String expectedAdvertisedHostPort, ClientFactory clientFactory) {
    return connectToAdvertisedNodeClientFactory(
        expectedAdvertisedHostPort, clientFactory, ExactNodeRetryClientFactory.RETRY_INTERVAL);
  }

  static ClientFactory connectToAdvertisedNodeClientFactory(
      String expectedAdvertisedHostPort, ClientFactory clientFactory, Duration retryInterval) {
    return new ExactNodeRetryClientFactory(
        clientFactory,
        client -> {
          String currentKey = client.serverAdvertisedHost() + ":" + client.serverAdvertisedPort();
          boolean success = expectedAdvertisedHostPort.equals(currentKey);
          LOGGER.debug(
              "Expected client {}, got {}: {}",
              expectedAdvertisedHostPort,
              currentKey,
              success ? "success" : "failure");
          return success;
        },
        retryInterval);
  }

  static Runnable namedRunnable(Runnable task, String format, Object... args) {
    return new NamedRunnable(format(format, args), task);
  }

  static <T, R> Function<T, R> namedFunction(Function<T, R> task, String format, Object... args) {
    return new NamedFunction<>(format(format, args), task);
  }

  static <T> T callAndMaybeRetry(
      Callable<T> operation, Predicate<Exception> retryCondition, String format, Object... args) {
    return callAndMaybeRetry(
        operation,
        retryCondition,
        i -> i >= 3 ? BackOffDelayPolicy.TIMEOUT : Duration.ZERO,
        format,
        args);
  }

  static <T> T callAndMaybeRetry(
      Callable<T> operation,
      Predicate<Exception> retryCondition,
      BackOffDelayPolicy delayPolicy,
      String format,
      Object... args) {
    String description = format(format, args);
    int attempt = 0;
    Exception lastException = null;
    long startTime = System.nanoTime();
    boolean keepTrying = true;
    while (keepTrying) {
      try {
        attempt++;
        LOGGER.debug("Starting attempt #{} for operation '{}'", attempt, description);
        T result = operation.call();
        Duration operationDuration = Duration.ofNanos(System.nanoTime() - startTime);
        LOGGER.debug(
            "Operation '{}' completed in {} ms after {} attempt(s)",
            description,
            operationDuration.toMillis(),
            attempt);
        return result;
      } catch (Exception e) {
        lastException = e;
        if (retryCondition.test(e)) {
          LOGGER.debug("Operation '{}' failed, retrying...", description);
          Duration delay = delayPolicy.delay(attempt);
          if (BackOffDelayPolicy.TIMEOUT.equals(delay)) {
            keepTrying = false;
          } else if (!delay.isZero()) {
            try {
              Thread.sleep(delay.toMillis());
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              lastException = ex;
              keepTrying = false;
            }
          }
        } else {
          keepTrying = false;
        }
      }
    }
    String message =
        format(
            "Could not complete task '%s' after %d attempt(s) (reason: %s)",
            description, attempt, exceptionMessage(lastException));
    LOGGER.debug(message);
    if (lastException == null) {
      throw new StreamException(message);
    } else if (lastException instanceof RuntimeException) {
      throw (RuntimeException) lastException;
    } else {
      throw new StreamException(message, lastException);
    }
  }

  static String exceptionMessage(Exception e) {
    if (e == null) {
      return "unknown";
    } else if (e.getMessage() == null) {
      return e.getClass().getSimpleName();
    } else {
      return e.getMessage() + " [" + e.getClass().getSimpleName() + "]";
    }
  }

  interface ClientFactory {

    Client client(ClientFactoryContext context);
  }

  static class ExactNodeRetryClientFactory implements ClientFactory {

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private final ClientFactory delegate;
    private final Predicate<Client> condition;
    private final Duration retryInterval;

    ExactNodeRetryClientFactory(
        ClientFactory delegate, Predicate<Client> condition, Duration retryInterval) {
      this.delegate = delegate;
      this.condition = condition;
      this.retryInterval = retryInterval;
    }

    @Override
    public Client client(ClientFactoryContext context) {
      while (true) {
        Client client = this.delegate.client(context);
        if (condition.test(client)) {
          return client;
        } else {
          try {
            client.close();
          } catch (Exception e) {
            LOGGER.warn("Error while trying to close client", e);
          }
        }
        try {
          Thread.sleep(this.retryInterval.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
    }
  }

  static class ClientFactoryContext {

    private ClientParameters parameters;
    private String key;

    static ClientFactoryContext fromParameters(ClientParameters parameters) {
      return new ClientFactoryContext().parameters(parameters);
    }

    ClientParameters parameters() {
      return parameters;
    }

    ClientFactoryContext parameters(ClientParameters parameters) {
      this.parameters = parameters;
      return this;
    }

    String key() {
      return key;
    }

    ClientFactoryContext key(String key) {
      this.key = key;
      return this;
    }
  }

  private static class TrustEverythingTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  enum ClientConnectionType {
    CONSUMER,
    PRODUCER,
    LOCATOR
  }

  static Function<ClientConnectionType, String> defaultConnectionNamingStrategy(String prefix) {
    Map<ClientConnectionType, AtomicLong> sequences =
        new ConcurrentHashMap<>(ClientConnectionType.values().length);
    Map<ClientConnectionType, String> prefixes =
        new ConcurrentHashMap<>(ClientConnectionType.values().length);
    for (ClientConnectionType type : ClientConnectionType.values()) {
      sequences.put(type, new AtomicLong(0));
      prefixes.put(type, prefix + type.name().toLowerCase(Locale.ENGLISH) + "-");
    }
    return clientConnectionType ->
        prefixes.get(clientConnectionType) + sequences.get(clientConnectionType).getAndIncrement();
  }

  /*
  class to help testing SAC on super streams
   */
  static class CompositeConsumerUpdateListener implements ConsumerUpdateListener {

    private final List<ConsumerUpdateListener> delegates = new CopyOnWriteArrayList<>();

    @Override
    public OffsetSpecification update(Context context) {
      OffsetSpecification result = null;
      for (ConsumerUpdateListener delegate : delegates) {
        OffsetSpecification offsetSpecification = delegate.update(context);
        if (offsetSpecification != null) {
          result = offsetSpecification;
        }
      }
      return result;
    }

    void add(ConsumerUpdateListener delegate) {
      this.delegates.add(delegate);
    }

    CompositeConsumerUpdateListener duplicate() {
      CompositeConsumerUpdateListener duplica = new CompositeConsumerUpdateListener();
      for (ConsumerUpdateListener delegate : this.delegates) {
        duplica.add(delegate);
      }
      return duplica;
    }
  }

  static boolean offsetBefore(long x, long y) {
    return Long.compareUnsigned(x, y) < 0;
  }

  private static String currentVersion(String currentVersion) {
    // versions built from source: 3.7.0+rc.1.4.gedc5d96
    if (currentVersion.contains("+")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("+"));
    }
    // alpha (snapshot) versions: 3.7.0~alpha.449-1
    if (currentVersion.contains("~")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("~"));
    }
    // alpha (snapshot) versions: 3.7.1-alpha.40
    if (currentVersion.contains("-")) {
      currentVersion = currentVersion.substring(0, currentVersion.indexOf("-"));
    }
    return currentVersion;
  }

  /**
   * https://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
   */
  static int versionCompare(String str1, String str2) {
    String[] vals1 = str1.split("\\.");
    String[] vals2 = str2.split("\\.");
    int i = 0;
    // set index to first non-equal ordinal or length of shortest version string
    while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
      i++;
    }
    // compare first non-equal ordinal number
    if (i < vals1.length && i < vals2.length) {
      int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
      return Integer.signum(diff);
    }
    // the strings are equal or one string is a substring of the other
    // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
    return Integer.signum(vals1.length - vals2.length);
  }

  static boolean is3_11_OrMore(String brokerVersion) {
    return versionCompare(currentVersion(brokerVersion), "3.11.0") >= 0;
  }

  static StreamException convertCodeToException(
      short responseCode, String stream, Supplier<String> fallbackMessage) {
    if (responseCode == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
      return new StreamDoesNotExistException(stream);
    } else if (responseCode == Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE) {
      return new StreamNotAvailableException(stream);
    } else {
      return new StreamException(fallbackMessage.get(), responseCode);
    }
  }

  private static class NamedRunnable implements Runnable {

    private final String name;
    private final Runnable delegate;

    private NamedRunnable(String name, Runnable delegate) {
      this.name = name;
      this.delegate = delegate;
    }

    @Override
    public void run() {
      this.delegate.run();
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private static class NamedFunction<T, R> implements Function<T, R> {

    private final String name;
    private final Function<T, R> delegate;

    private NamedFunction(String name, Function<T, R> delegate) {
      this.name = name;
      this.delegate = delegate;
    }

    @Override
    public R apply(T t) {
      return this.delegate.apply(t);
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  static String quote(String value) {
    if (value == null) {
      return "null";
    } else {
      return "\"" + value + "\"";
    }
  }

  static String jsonField(String name, Number value) {
    return quote(name) + " : " + value.longValue();
  }

  static String jsonField(String name, String value) {
    return quote(name) + " : " + quote(value);
  }

  static class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory backingThreaFactory;

    private final String prefix;

    private final AtomicLong count = new AtomicLong(0);

    public NamedThreadFactory(String prefix) {
      this(Executors.defaultThreadFactory(), prefix);
    }

    public NamedThreadFactory(ThreadFactory backingThreadFactory, String prefix) {
      this.backingThreaFactory = backingThreadFactory;
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = this.backingThreaFactory.newThread(r);
      thread.setName(prefix + count.getAndIncrement());
      return thread;
    }
  }

  static final ExecutorServiceFactory NO_OP_EXECUTOR_SERVICE_FACTORY =
      new NoOpExecutorServiceFactory();

  static class NoOpExecutorServiceFactory implements ExecutorServiceFactory {

    private final ExecutorService executorService = new NoOpExecutorService();

    @Override
    public ExecutorService get() {
      return executorService;
    }

    @Override
    public void clientClosed(ExecutorService executorService) {}

    @Override
    public void close() {}
  }

  private static class NoOpExecutorService implements ExecutorService {

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return null;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return false;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return null;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return null;
    }

    @Override
    public Future<?> submit(Runnable task) {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
      return null;
    }

    @Override
    public void execute(Runnable command) {}
  }

  static class MutableBoolean {

    private boolean value;

    MutableBoolean(boolean initialValue) {
      this.value = initialValue;
    }

    void set(boolean value) {
      this.value = value;
    }

    boolean get() {
      return this.value;
    }
  }

  static <T> T lock(Lock lock, Supplier<T> action) {
    lock.lock();
    try {
      return action.get();
    } finally {
      lock.unlock();
    }
  }
}

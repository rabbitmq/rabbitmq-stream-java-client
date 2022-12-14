// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.ConsumerUpdateListener;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

  static final LongConsumer NO_OP_LONG_CONSUMER = someLong -> {};
  static final LongSupplier NO_OP_LONG_SUPPLIER = () -> 0;
  static final X509TrustManager TRUST_EVERYTHING_TRUST_MANAGER = new TrustEverythingTrustManager();
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final Map<Short, String> CONSTANT_LABELS;

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

  private Utils() {}

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
    return context -> {
      ClientParameters parametersCopy = context.parameters().duplicate();
      Address address = new Address(parametersCopy.host(), parametersCopy.port());
      address = environment.addressResolver().resolve(address);
      parametersCopy.host(address.host()).port(address.port());

      if (context.key() == null) {
        throw new IllegalArgumentException("A key is necessary to create the client connection");
      }

      return Utils.connectToAdvertisedNodeClientFactory(
              context.key(), context1 -> new Client(context1.parameters()))
          .client(Utils.ClientFactoryContext.fromParameters(parametersCopy).key(context.key()));
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
    return new NamedRunnable(String.format(format, args), task);
  }

  static <T, R> Function<T, R> namedFunction(Function<T, R> task, String format, Object... args) {
    return new NamedFunction<>(String.format(format, args), task);
  }

  static <T> T callAndMaybeRetry(
      Supplier<T> operation, Predicate<Exception> retryCondition, String format, Object... args) {
    return callAndMaybeRetry(operation, retryCondition, i -> Duration.ZERO, format, args);
  }

  static <T> T callAndMaybeRetry(
      Supplier<T> operation,
      Predicate<Exception> retryCondition,
      BackOffDelayPolicy delayPolicy,
      String format,
      Object... args) {
    String description = format(format, args);
    int attempt = 0;
    Exception lastException = null;
    while (attempt++ < 3) {
      try {
        return operation.get();
      } catch (Exception e) {
        lastException = e;
        if (retryCondition.test(e)) {
          LOGGER.debug("Operation '{}' failed, retrying...", description);
          Duration delay = delayPolicy.delay(attempt - 1);
          if (!delay.isZero()) {
            try {
              Thread.sleep(delay.toMillis());
            } catch (InterruptedException ex) {
              Thread.interrupted();
              lastException = ex;
              break;
            }
          }
        } else {
          break;
        }
      }
    }
    String message =
        format(
            "Could not complete task '%s' after %d attempt(s) (reason: {})",
            description, --attempt, exceptionMessage(lastException));
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
          Thread.interrupted();
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
}

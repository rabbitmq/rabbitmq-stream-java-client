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

import static io.vavr.Tuple.of;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import io.netty.channel.EventLoopGroup;
import io.vavr.Tuple2;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.assertj.core.api.AssertDelegateTarget;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  static final Duration DEFAULT_CONDITION_TIMEOUT = Duration.ofSeconds(10);

  private static final ConnectionFactory AMQP_CF = new ConnectionFactory();

  private TestUtils() {}

  public static Duration waitAtMost(CallableBooleanSupplier condition) throws Exception {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, condition, null);
  }

  public static Duration waitAtMost(
      CallableBooleanSupplier condition, String format, Object... args) throws Exception {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, condition, () -> String.format(format, args));
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(DEFAULT_CONDITION_TIMEOUT, condition, message);
  }

  public static Duration waitAtMost(Duration timeout, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost(timeout, condition, null);
  }

  public static Duration waitAtMost(int timeoutInSeconds, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  public static Duration waitAtMost(
      int timeoutInSeconds, CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(Duration.ofSeconds(timeoutInSeconds), condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout, CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(timeout, Duration.ofMillis(100), condition, message);
  }

  public static Duration waitAtMost(
      Duration timeout, Duration waitTime, CallableBooleanSupplier condition) throws Exception {
    return waitAtMost(timeout, waitTime, condition, null);
  }

  public static Duration waitAtMost(
      Duration timeout,
      Duration waitTime,
      CallableBooleanSupplier condition,
      Supplier<String> message)
      throws Exception {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    Duration waitedTime = Duration.ZERO;
    Exception exception = null;
    while (waitedTime.compareTo(timeout) <= 0) {
      Thread.sleep(waitTime.toMillis());
      waitedTime = waitedTime.plus(waitTime);
      try {
        if (condition.getAsBoolean()) {
          return waitedTime;
        }
        exception = null;
      } catch (Exception e) {
        exception = e;
      }
    }
    String msg;
    if (message == null) {
      msg = "Waited " + timeout.getSeconds() + " second(s), condition never got true";
    } else {
      msg = "Waited " + timeout.getSeconds() + " second(s), " + message.get();
    }
    if (exception == null) {
      fail(msg);
    } else {
      fail(msg, exception);
    }
    return waitedTime;
  }

  public static Address localhost() {
    return new Address("localhost", Client.DEFAULT_PORT);
  }

  static Address localhostTls() {
    return new Address("localhost", Client.DEFAULT_TLS_PORT);
  }

  static byte b(int value) {
    return (byte) value;
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf, int publishCount, String stream) {
    publishAndWaitForConfirms(cf, "message", publishCount, stream);
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf, String messagePrefix, int publishCount, String stream) {
    AtomicLong sequence = new AtomicLong(0);
    publishAndWaitForConfirms(
        cf,
        builder ->
            builder
                .addData(
                    (messagePrefix + sequence.getAndIncrement()).getBytes(StandardCharsets.UTF_8))
                .build(),
        publishCount,
        stream);
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf,
      Function<MessageBuilder, Message> messageFactory,
      int publishCount,
      String stream) {
    publishAndWaitForConfirms(cf, messageFactory, publishCount, stream, Duration.ofSeconds(60));
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf,
      Function<MessageBuilder, Message> messageFactory,
      int publishCount,
      String stream,
      Duration timeout) {
    CountDownLatch latchConfirm = new CountDownLatch(publishCount);
    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, correlationId) -> latchConfirm.countDown();

    Client client =
        cf.get(new Client.ClientParameters().publishConfirmListener(publishConfirmListener));

    client.declarePublisher(b(1), null, stream);
    int batchSize = 100;
    if (publishCount > batchSize) {
      List<Message> messages = new ArrayList<>(batchSize);
      for (int i = 1; i <= publishCount; i++) {
        Message message = messageFactory.apply(client.messageBuilder());
        messages.add(message);
        if (i % batchSize == 0 || i == publishCount) {
          client.publish(b(1), messages);
          messages.clear();
        }
      }
    } else {
      for (int i = 1; i <= publishCount; i++) {
        Message message = messageFactory.apply(client.messageBuilder());
        client.publish(b(1), Collections.singletonList(message));
      }
    }

    latchAssert(latchConfirm).completes(timeout);
  }

  static Consumer<Object> namedTask(TaskWithException task, String description) {
    return new Consumer<Object>() {

      @Override
      public void accept(Object o) {
        try {
          task.run(o);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString() {
        return description;
      }
    };
  }

  static <T> Consumer<T> namedConsumer(Consumer<T> delegate, String description) {
    return new Consumer<T>() {
      @Override
      public void accept(T t) {
        delegate.accept(t);
      }

      @Override
      public String toString() {
        return description;
      }
    };
  }

  static <T, U> BiConsumer<T, U> namedBiConsumer(BiConsumer<T, U> delegate, String description) {
    return new BiConsumer<T, U>() {
      @Override
      public void accept(T t, U s) {
        delegate.accept(t, s);
      }

      @Override
      public String toString() {
        return description;
      }
    };
  }

  static Answer<Void> answer(Runnable task) {
    return invocationOnMock -> {
      task.run();
      return null;
    };
  }

  static Answer<Void> answer(Consumer<InvocationOnMock> invocation) {
    return invocationOnMock -> {
      invocation.accept(invocationOnMock);
      return null;
    };
  }

  static <T> void doIfNotNull(T obj, Consumer<T> action) {
    if (obj != null) {
      action.accept(obj);
    }
  }

  static void declareSuperStreamTopology(Client client, String superStream, int partitions) {
    declareSuperStreamTopology(
        client,
        superStream,
        IntStream.range(0, partitions).mapToObj(String::valueOf).toArray(String[]::new));
  }

  static void declareSuperStreamTopology(Client client, String superStream, String... rks) {
    List<String> partitions =
        Arrays.stream(rks).map(rk -> superStream + "-" + rk).collect(toList());
    if (atLeastVersion("3.13.0", client.brokerVersion())) {
      client.createSuperStream(superStream, partitions, asList(rks), null);
    } else {
      try (Connection connection = connection();
          Channel ch = connection.createChannel()) {
        ch.exchangeDeclare(
            superStream,
            BuiltinExchangeType.DIRECT,
            true,
            false,
            Collections.singletonMap("x-super-stream", true));
        List<Tuple2<String, Integer>> bindings = new ArrayList<>(rks.length);
        for (int i = 0; i < rks.length; i++) {
          bindings.add(of(rks[i], i));
        }
        // shuffle the order to make sure we get in the correct order from the server
        Collections.shuffle(bindings);

        for (Tuple2<String, Integer> binding : bindings) {
          String routingKey = binding._1();
          String partitionName = superStream + "-" + routingKey;
          ch.queueDeclare(
              partitionName,
              true,
              false,
              false,
              Collections.singletonMap("x-queue-type", "stream"));
          ch.queueBind(
              partitionName,
              superStream,
              routingKey,
              Collections.singletonMap("x-stream-partition-order", binding._2()));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  static void deleteSuperStreamTopology(Client client, String superStream) {
    if (atLeastVersion("3.13.0", client.brokerVersion())) {
      client.deleteSuperStream(superStream);
    } else {
      try (Connection connection = connection();
          Channel ch = connection.createChannel()) {
        ch.exchangeDelete(superStream);
        for (String partition : client.partitions(superStream)) {
          ch.queueDelete(partition);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static String streamName(TestInfo info) {
    return streamName(info.getTestClass().get(), info.getTestMethod().get());
  }

  private static String streamName(ExtensionContext context) {
    return streamName(context.getTestInstance().get().getClass(), context.getTestMethod().get());
  }

  private static String streamName(Class<?> testClass, Method testMethod) {
    String uuid = UUID.randomUUID().toString();
    return format(
        "%s_%s%s",
        testClass.getSimpleName(), testMethod.getName(), uuid.substring(uuid.length() / 2));
  }

  static boolean tlsAvailable() {
    return Cli.rabbitmqctl("status").output().contains("stream/ssl");
  }

  static boolean isCluster() {
    String content = Cli.rabbitmqctl("eval 'nodes().'").output();
    return !content.replace("[", "").replace("]", "").trim().isEmpty();
  }

  static <T> void forEach(Collection<T> in, CallableIndexConsumer<T> consumer) throws Exception {
    int count = 0;
    for (T t : in) {
      consumer.accept(count++, t);
    }
  }

  static CountDownLatchAssert latchAssert(CountDownLatch latch) {
    return new CountDownLatchAssert(latch);
  }

  static CountDownLatchAssert latchAssert(AtomicReference<CountDownLatch> latchReference) {
    return new CountDownLatchAssert(latchReference.get());
  }

  static class ResponseConditions {

    static Condition<Response> ok() {
      return new Condition<>(Response::isOk, "OK");
    }

    static Condition<Response> ko() {
      return new Condition<>(response -> !response.isOk(), "KO");
    }

    static Condition<Response> responseCode(short expectedResponse) {
      return new Condition<>(
          response -> response.getResponseCode() == expectedResponse,
          "response code %s",
          Utils.formatConstant(expectedResponse));
    }
  }

  static class ExceptionConditions {

    static Condition<Throwable> responseCode(short expectedResponseCode) {
      String message = "code " + Utils.formatConstant(expectedResponseCode);
      return new Condition<>(
          throwable ->
              throwable instanceof StreamException
                  && ((StreamException) throwable).getCode() == expectedResponseCode,
          message);
    }
  }

  static Map<String, StreamMetadata> metadata(String stream, Broker leader, List<Broker> replicas) {
    return metadata(stream, leader, replicas, Constants.RESPONSE_CODE_OK);
  }

  static Map<String, StreamMetadata> metadata(
      String stream, Broker leader, List<Broker> replicas, short code) {
    return Collections.singletonMap(
        stream, new Client.StreamMetadata(stream, code, leader, replicas));
  }

  static Map<String, StreamMetadata> metadata(Broker leader, List<Broker> replicas) {
    return metadata("stream", leader, replicas);
  }

  static String currentVersion(String currentVersion) {
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

  static boolean beforeMessageContainers(String currentVersion) {
    return Utils.versionCompare(currentVersion(currentVersion), "3.13.0") < 0;
  }

  static boolean afterMessageContainers(String currentVersion) {
    return Utils.versionCompare(currentVersion(currentVersion), "3.13.0") >= 0;
  }

  static boolean atLeastVersion(String expectedVersion, String currentVersion) {
    if (currentVersion.contains("alpha-stream")) {
      return true;
    }
    try {
      currentVersion = currentVersion(currentVersion);
      return "0.0.0".equals(currentVersion)
          || Utils.versionCompare(currentVersion, expectedVersion) >= 0;
    } catch (RuntimeException e) {
      LoggerFactory.getLogger(TestUtils.class)
          .warn("Unable to parse broker version {}", currentVersion, e);
      throw e;
    }
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfFilteringNotSupportedCondition.class)
  @interface DisabledIfFilteringNotSupported {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfRabbitMqCtlNotSetCondition.class)
  @interface DisabledIfRabbitMqCtlNotSet {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfMqttNotEnabledCondition.class)
  @interface DisabledIfMqttNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfStompNotEnabledCondition.class)
  @interface DisabledIfStompNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfAmqp10NotEnabledCondition.class)
  @interface DisabledIfAmqp10NotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfAuthMechanismSslNotEnabledCondition.class)
  @interface DisabledIfAuthMechanismSslNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfOauth2AuthBackendNotEnabledCondition.class)
  @interface DisabledIfOauth2AuthBackendNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfTlsNotEnabledCondition.class)
  public @interface DisabledIfTlsNotEnabled {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfNotClusterCondition.class)
  @interface DisabledIfNotCluster {}

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(AnnotationBrokerVersionAtLeastCondition.class)
  public @interface BrokerVersionAtLeast {

    BrokerVersion value();
  }

  interface TaskWithException {

    void run(Object context) throws Exception;
  }

  interface CallableIndexConsumer<T> {

    void accept(int index, T t) throws Exception;
  }

  public interface CallableConsumer<T> {

    void accept(T t) throws Exception;
  }

  static <T> java.util.function.Consumer<T> wrap(CallableConsumer<T> action) {
    return t -> {
      try {
        action.accept(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @FunctionalInterface
  public interface CallableBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  interface RunnableWithException {

    void run() throws Exception;
  }

  public static class CountDownLatchReferenceConditions {

    public static Condition<AtomicReference<CountDownLatch>> completed() {
      return new Condition<>(
          latch -> CountDownLatchConditions.latchCondition(latch.get(), DEFAULT_CONDITION_TIMEOUT),
          "completed in %d ms",
          DEFAULT_CONDITION_TIMEOUT.toMillis());
    }
  }

  public static class CountDownLatchConditions {

    public static Condition<CountDownLatch> completed() {
      return completed(Duration.ofSeconds(10));
    }

    static Condition<CountDownLatch> completed(int timeoutInSeconds) {
      return completed(Duration.ofSeconds(timeoutInSeconds));
    }

    static Condition<CountDownLatch> completed(Duration timeout) {
      return new Condition<>(
          latch -> latchCondition(latch, timeout), "completed in %d ms", timeout.toMillis());
    }

    private static boolean latchCondition(CountDownLatch latch, Duration timeout) {
      try {
        return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }
  }

  public static class StreamTestInfrastructureExtension
      implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
        ExtensionContext.Namespace.create(StreamTestInfrastructureExtension.class);

    private static ExtensionContext.Store store(ExtensionContext extensionContext) {
      return extensionContext.getRoot().getStore(NAMESPACE);
    }

    static EventLoopGroup eventLoopGroup(ExtensionContext context) {
      return (EventLoopGroup) store(context).get("nettyEventLoopGroup");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
      store(context).put("nettyEventLoopGroup", Utils.eventLoopGroup());
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      Field field = field(context.getTestInstance().get().getClass(), "eventLoopGroup");
      if (field != null) {
        field.setAccessible(true);
        field.set(context.getTestInstance().get(), eventLoopGroup(context));
      }

      String brokerVersion = null;
      field = field(context.getTestInstance().get().getClass(), "stream");
      if (field != null) {
        field.setAccessible(true);
        String stream = streamName(context);
        field.set(context.getTestInstance().get(), stream);
        Client client =
            new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
        brokerVersion = currentVersion(client.brokerVersion());
        Client.Response response = client.create(stream);
        assertThat(response.isOk()).isTrue();
        store(context.getRoot()).put("filteringSupported", client.filteringSupported());
        client.close();
        store(context).put("testMethodStream", stream);
      }

      for (Field declaredField : context.getTestInstance().get().getClass().getDeclaredFields()) {
        if (declaredField.getType().equals(ClientFactory.class)) {
          declaredField.setAccessible(true);
          ClientFactory clientFactory = new ClientFactory(eventLoopGroup(context));
          declaredField.set(context.getTestInstance().get(), clientFactory);
          store(context).put("testClientFactory", clientFactory);
          break;
        }
      }

      field = field(context.getTestInstance().get().getClass(), "brokerVersion");
      if (field != null) {
        if (brokerVersion == null) {
          brokerVersion =
              context.getRoot().getStore(Namespace.GLOBAL).get("brokerVersion", String.class);
        }
        if (brokerVersion == null) {
          Client client =
              new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
          brokerVersion = currentVersion(client.brokerVersion());
        }
        context.getRoot().getStore(Namespace.GLOBAL).put("brokerVersion", brokerVersion);
        field.setAccessible(true);
        field.set(context.getTestInstance().get(), brokerVersion);
      }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      ClientFactory clientFactory = (ClientFactory) store(context).get("testClientFactory");
      if (clientFactory != null) {
        clientFactory.close();
      }

      try {
        Field streamField = context.getTestInstance().get().getClass().getDeclaredField("stream");
        streamField.setAccessible(true);
        String stream = (String) streamField.get(context.getTestInstance().get());
        Client client =
            new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
        Client.Response response = client.delete(stream);
        assertThat(response.isOk()).isTrue();
        client.close();
        store(context).remove("testMethodStream");
      } catch (NoSuchFieldException e) {

      }
    }

    @Override
    public void afterAll(ExtensionContext context) {
      EventLoopGroup eventLoopGroup = eventLoopGroup(context);
      ExecutorServiceCloseableResourceWrapper wrapper =
          context
              .getRoot()
              .getStore(Namespace.GLOBAL)
              .getOrComputeIfAbsent(ExecutorServiceCloseableResourceWrapper.class);
      wrapper.executorService.submit(
          () -> {
            try {
              eventLoopGroup.shutdownGracefully(0, 0, SECONDS).get(10, SECONDS);
            } catch (InterruptedException e) {
              LOGGER.debug("Error while asynchronously closing Netty event loop group", e);
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              LOGGER.warn("Error while asynchronously closing Netty event loop group", e);
            }
          });
    }

    private static Field field(Class<?> cls, String name) {
      Field field = null;
      while (field == null && cls != null) {
        try {
          field = cls.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
          cls = cls.getSuperclass();
        }
      }
      return field;
    }

    private static class ExecutorServiceCloseableResourceWrapper implements AutoCloseable {

      private final ExecutorService executorService;

      private ExecutorServiceCloseableResourceWrapper() {
        ThreadFactory tf = ThreadUtils.threadFactory("closing-resource-");
        this.executorService = Executors.newCachedThreadPool(tf);
      }

      @Override
      public void close() {
        this.executorService.shutdownNow();
      }
    }
  }

  public static class ClientFactory {

    private final EventLoopGroup eventLoopGroup;
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();

    public ClientFactory(EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
    }

    public Client get() {
      return get(new Client.ClientParameters());
    }

    public Client get(Client.ClientParameters parameters) {
      Client client = new Client(parameters.eventLoopGroup(eventLoopGroup));
      clients.add(client);
      return client;
    }

    private void close() {
      for (Client c : clients) {
        c.close();
      }
    }
  }

  static class DisabledIfFilteringNotSupportedCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      Boolean filteringSupported =
          StreamTestInfrastructureExtension.store(context).get("filteringSupported", Boolean.class);
      if (filteringSupported == null) {
        EventLoopGroup eventLoop = StreamTestInfrastructureExtension.eventLoopGroup(context);
        try (Client client = new Client(new ClientParameters().eventLoopGroup(eventLoop))) {
          filteringSupported = client.filteringSupported();
          StreamTestInfrastructureExtension.store(context)
              .put("filteringSupported", filteringSupported);
        }
      }

      if (filteringSupported) {
        return ConditionEvaluationResult.enabled("filtering is supported");
      } else {
        return ConditionEvaluationResult.disabled("filtering is not supported");
      }
    }
  }

  static class DisabledIfRabbitMqCtlNotSetCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (Cli.rabbitmqctlCommand() == null) {
        return ConditionEvaluationResult.disabled("rabbitmqctl.bin system property not set");
      } else {
        return ConditionEvaluationResult.enabled("rabbitmqctl.bin system property is set");
      }
    }
  }

  abstract static class DisabledIfPluginNotEnabledCondition implements ExecutionCondition {

    private final String pluginLabel;
    private final Predicate<String> condition;

    DisabledIfPluginNotEnabledCondition(String pluginLabel, Predicate<String> condition) {
      this.pluginLabel = pluginLabel;
      this.condition = condition;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (Cli.rabbitmqctlCommand() == null) {
        return ConditionEvaluationResult.disabled(
            format(
                "rabbitmqctl.bin system property not set, cannot check if %s plugin is enabled",
                pluginLabel));
      } else {
        try {
          String output = Cli.rabbitmqctl("status").output();
          if (condition.test(output)) {
            return ConditionEvaluationResult.enabled(format("%s plugin enabled", pluginLabel));
          } else {
            return ConditionEvaluationResult.disabled(format("%s plugin disabled", pluginLabel));
          }
        } catch (Exception e) {
          return ConditionEvaluationResult.disabled(
              format("Error while trying to detect %s plugin: " + e.getMessage(), pluginLabel));
        }
      }
    }
  }

  static class DisabledIfMqttNotEnabledCondition extends DisabledIfPluginNotEnabledCondition {

    DisabledIfMqttNotEnabledCondition() {
      super(
          "MQTT", output -> output.contains("rabbitmq_mqtt") && output.contains("protocol: mqtt"));
    }
  }

  static class DisabledIfStompNotEnabledCondition extends DisabledIfPluginNotEnabledCondition {

    DisabledIfStompNotEnabledCondition() {
      super(
          "STOMP",
          output -> output.contains("rabbitmq_stomp") && output.contains("protocol: stomp"));
    }
  }

  static class DisabledIfAuthMechanismSslNotEnabledCondition
      extends DisabledIfPluginNotEnabledCondition {

    DisabledIfAuthMechanismSslNotEnabledCondition() {
      super(
          "X509 authentication mechanism",
          output -> output.contains("rabbitmq_auth_mechanism_ssl"));
    }
  }

  static class DisabledIfAmqp10NotEnabledCondition extends DisabledIfPluginNotEnabledCondition {

    DisabledIfAmqp10NotEnabledCondition() {
      super(
          "AMQP 1.0", output -> output.contains("rabbitmq_amqp1_0") && output.contains("AMQP 1.0"));
    }
  }

  static class DisabledIfOauth2AuthBackendNotEnabledCondition
      extends DisabledIfPluginNotEnabledCondition {

    DisabledIfOauth2AuthBackendNotEnabledCondition() {
      super(
          "OAuth2 authentication backend",
          output -> output.contains("rabbitmq_auth_backend_oauth2"));
    }
  }

  static class DisabledIfTlsNotEnabledCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (tlsAvailable()) {
        return ConditionEvaluationResult.enabled("TLS is enabled");
      } else {
        return ConditionEvaluationResult.disabled("TLS is disabled");
      }
    }
  }

  static class DisabledIfNotClusterCondition implements ExecutionCondition {

    private static final String KEY = "isCluster";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      ExtensionContext.Store store = context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL);
      boolean isCluster = store.getOrComputeIfAbsent(KEY, k -> isCluster(), Boolean.class);
      if (isCluster) {
        return ConditionEvaluationResult.enabled("Multi-node cluster");
      } else {
        return ConditionEvaluationResult.disabled("Not a multi-node cluster");
      }
    }
  }

  private static class BaseBrokerVersionAtLeastCondition implements ExecutionCondition {

    private final Function<ExtensionContext, String> versionProvider;

    private BaseBrokerVersionAtLeastCondition(Function<ExtensionContext, String> versionProvider) {
      this.versionProvider = versionProvider;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (!context.getTestMethod().isPresent()) {
        return ConditionEvaluationResult.enabled("Apply only to methods");
      }
      String expectedVersion = versionProvider.apply(context);
      if (expectedVersion == null) {
        return ConditionEvaluationResult.enabled("No broker version requirement");
      } else {
        String brokerVersion =
            context
                .getRoot()
                .getStore(Namespace.GLOBAL)
                .getOrComputeIfAbsent(
                    "brokerVersion",
                    k -> {
                      EventLoopGroup eventLoopGroup =
                          StreamTestInfrastructureExtension.eventLoopGroup(context);
                      if (eventLoopGroup == null) {
                        throw new IllegalStateException(
                            "The event loop group must be in the test context to use "
                                + BrokerVersionAtLeast.class.getSimpleName()
                                + ", use the "
                                + StreamTestInfrastructureExtension.class.getSimpleName()
                                + " extension in the test");
                      }
                      try (Client client =
                          new Client(new ClientParameters().eventLoopGroup(eventLoopGroup))) {
                        return client.brokerVersion();
                      }
                    },
                    String.class);

        if (atLeastVersion(expectedVersion, brokerVersion)) {
          return ConditionEvaluationResult.enabled(
              "Broker version requirement met, expected "
                  + expectedVersion
                  + ", actual "
                  + brokerVersion);
        } else {
          return ConditionEvaluationResult.disabled(
              "Broker version requirement not met, expected "
                  + expectedVersion
                  + ", actual "
                  + brokerVersion);
        }
      }
    }
  }

  private static class AnnotationBrokerVersionAtLeastCondition
      extends BaseBrokerVersionAtLeastCondition {

    private AnnotationBrokerVersionAtLeastCondition() {
      super(
          context -> {
            BrokerVersionAtLeast annotation =
                context.getElement().get().getAnnotation(BrokerVersionAtLeast.class);
            return annotation == null ? null : annotation.value().toString();
          });
    }
  }

  static class BrokerVersionAtLeast311Condition extends BaseBrokerVersionAtLeastCondition {

    private BrokerVersionAtLeast311Condition() {
      super(context -> "3.11.0");
    }
  }

  static class CountDownLatchAssert implements AssertDelegateTarget {

    private static final Duration TIMEOUT = DEFAULT_CONDITION_TIMEOUT;

    private final CountDownLatch latch;

    CountDownLatchAssert(CountDownLatch latch) {
      this.latch = latch;
    }

    void completes() {
      completes(TIMEOUT);
    }

    void completes(int timeoutInSeconds) {
      completes(Duration.ofSeconds(timeoutInSeconds));
    }

    void completes(Duration timeout) {
      try {
        assertThat(latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }

    void doesNotComplete() {
      doesNotComplete(TIMEOUT);
    }

    void doesNotComplete(int timeoutInSeconds) {
      doesNotComplete(Duration.ofSeconds(timeoutInSeconds));
    }

    void doesNotComplete(Duration timeout) {
      try {
        assertThat(latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)).isFalse();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException(e);
      }
    }
  }

  static Level newLoggerLevel(Class<?> c, Level level) {
    org.apache.logging.log4j.Logger logger = LogManager.getLogger(c);
    Level initialLevel = logger.getLevel();
    Configurator.setLevel(c.getName(), level);
    return initialLevel;
  }

  static void waitMs(long waitTime) {
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static String jsonPrettyPrint(String in) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    JsonElement element = JsonParser.parseString(in);
    return gson.toJson(element);
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Tag("single-active-consumer")
  public @interface SingleActiveConsumer {}

  public enum BrokerVersion {
    RABBITMQ_3_11_0("3.11.0"),
    RABBITMQ_3_11_7("3.11.7"),
    RABBITMQ_3_11_9("3.11.9"),
    RABBITMQ_3_11_11("3.11.11"),
    RABBITMQ_3_11_14("3.11.14"),
    RABBITMQ_3_13_0("3.13.0"),
    RABBITMQ_4_0_0("4.0.0"),
    RABBITMQ_4_1_2("4.1.2"),
    RABBITMQ_4_1_4("4.1.4"),
    RABBITMQ_4_2_0("4.2.0"),
    RABBITMQ_4_3_0("4.3.0");

    final String value;

    BrokerVersion(String value) {
      this.value = value;
    }

    String version() {
      return this.value;
    }

    @Override
    public String toString() {
      return this.value;
    }
  }

  static Client.ChunkListener credit() {
    return (client, subscriptionId, offset, messageCount, dataSize) -> {
      client.credit(subscriptionId, 1);
      return null;
    };
  }

  static void waitUntilStable(LongSupplier value) {
    int sameValueCount = 0;
    Duration timeout = Duration.ofSeconds(10);
    Duration waitTime = Duration.ofMillis(100);
    long waitedTime = 0;
    long lastValue = -1;
    while (sameValueCount < 10) {
      if (value.getAsLong() == lastValue) {
        sameValueCount++;
      } else {
        lastValue = value.getAsLong();
      }
      try {
        Thread.sleep(waitTime.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      waitedTime += waitTime.toMillis();
      if (waitedTime > timeout.toMillis()) {
        Assertions.fail("Did not stabilize after %d seconds", timeout.getSeconds());
      }
    }
  }

  static void repeatIfFailure(RunnableWithException test) throws Exception {
    int executionCount = 0;
    Throwable lastException = null;
    while (executionCount < 5) {
      try {
        test.run();
        return;
      } catch (Exception | AssertionError e) {
        executionCount++;
        lastException = e;
      }
    }
    if (lastException instanceof Error) {
      throw new RuntimeException(lastException);
    } else {
      throw (Exception) lastException;
    }
  }

  private static Connection connection() throws IOException, TimeoutException {
    return AMQP_CF.newConnection();
  }

  public static Sync sync() {
    return sync(1);
  }

  public static Sync sync(int count) {
    return new Sync(count);
  }

  public static class Sync {

    private final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

    private Sync(int count) {
      this.latch.set(new CountDownLatch(count));
    }

    public void down() {
      this.latch.get().countDown();
    }

    void down(int count) {
      IntStream.range(0, count).forEach(ignored -> this.latch.get().countDown());
    }

    boolean await(Duration timeout) {
      try {
        return this.latch.get().await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
    }

    long currentCount() {
      return this.latch.get().getCount();
    }

    void reset(int count) {
      this.latch.set(new CountDownLatch(count));
    }

    void reset() {
      this.reset(1);
    }

    boolean hasCompleted() {
      return this.latch.get().getCount() == 0;
    }
  }

  static Collection<Thread> threads() {
    return Thread.getAllStackTraces().keySet();
  }

  public static int randomNetworkPort() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }
}

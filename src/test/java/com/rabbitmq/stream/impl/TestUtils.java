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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import ch.qos.logback.classic.Level;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.assertj.core.api.AssertDelegateTarget;
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
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  private TestUtils() {}

  public static Duration waitAtMost(CallableBooleanSupplier condition) throws Exception {
    return waitAtMost(10, condition, null);
  }

  public static Duration waitAtMost(CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    return waitAtMost(10, condition, message);
  }

  public static Duration waitAtMost(Duration timeout, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost((int) timeout.getSeconds(), condition, null);
  }

  public static Duration waitAtMost(int timeoutInSeconds, CallableBooleanSupplier condition)
      throws Exception {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  public static Duration waitAtMost(
      int timeoutInSeconds, CallableBooleanSupplier condition, Supplier<String> message)
      throws Exception {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    int waitTime = 100;
    int waitedTime = 0;
    int timeoutInMs = timeoutInSeconds * 1000;
    Exception exception = null;
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      waitedTime += waitTime;
      try {
        if (condition.getAsBoolean()) {
          return Duration.ofMillis(waitedTime);
        }
        exception = null;
      } catch (Exception e) {
        exception = e;
      }
    }
    String msg;
    if (message == null) {
      msg = "Waited " + timeoutInSeconds + " second(s), condition never got true";
    } else {
      msg = "Waited " + timeoutInSeconds + " second(s), " + message.get();
    }
    if (exception == null) {
      fail(msg);
    } else {
      fail(msg, exception);
    }
    return Duration.ofMillis(waitedTime);
  }

  static Address localhost() {
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
    CountDownLatch latchConfirm = new CountDownLatch(publishCount);
    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, correlationId) -> latchConfirm.countDown();

    Client client =
        cf.get(new Client.ClientParameters().publishConfirmListener(publishConfirmListener));

    client.declarePublisher(b(1), null, stream);
    for (int i = 1; i <= publishCount; i++) {
      Message message = messageFactory.apply(client.messageBuilder());
      client.publish(b(1), Collections.singletonList(message));
    }

    latchAssert(latchConfirm).completes(Duration.ofSeconds(60));
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

  static void declareSuperStreamTopology(Connection connection, String superStream, int partitions)
      throws Exception {
    declareSuperStreamTopology(
        connection,
        superStream,
        IntStream.range(0, partitions).mapToObj(String::valueOf).toArray(String[]::new));
  }

  static void declareSuperStreamTopology(Connection connection, String superStream, String... rks)
      throws Exception {
    try (Channel ch = connection.createChannel()) {
      ch.exchangeDeclare(
          superStream,
          BuiltinExchangeType.DIRECT,
          true,
          false,
          Collections.singletonMap("x-super-stream", true));

      List<Tuple2<String, Integer>> bindings = new ArrayList<>(rks.length);
      for (int i = 0; i < rks.length; i++) {
        bindings.add(Tuple.of(rks[i], i));
      }
      // shuffle the order to make sure we get in the correct order from the server
      Collections.shuffle(bindings);

      for (Tuple2<String, Integer> binding : bindings) {
        String routingKey = binding._1();
        String partitionName = superStream + "-" + routingKey;
        ch.queueDeclare(
            partitionName, true, false, false, Collections.singletonMap("x-queue-type", "stream"));
        ch.queueBind(
            partitionName,
            superStream,
            routingKey,
            Collections.singletonMap("x-stream-partition-order", binding._2()));
      }
    }
  }

  static void deleteSuperStreamTopology(Connection connection, String superStream, int partitions)
      throws Exception {
    deleteSuperStreamTopology(
        connection,
        superStream,
        IntStream.range(0, partitions).mapToObj(String::valueOf).toArray(String[]::new));
  }

  static void deleteSuperStreamTopology(
      Connection connection, String superStream, String... routingKeys) throws Exception {
    try (Channel ch = connection.createChannel()) {
      ch.exchangeDelete(superStream);
      for (String routingKey : routingKeys) {
        String partitionName = superStream + "-" + routingKey;
        ch.queueDelete(partitionName);
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
    return String.format(
        "%s_%s%s",
        testClass.getSimpleName(), testMethod.getName(), uuid.substring(uuid.length() / 2));
  }

  static boolean tlsAvailable() {
    if (Host.rabbitmqctlCommand() == null) {
      throw new IllegalStateException(
          "rabbitmqctl.bin system property not set, cannot check if TLS is enabled");
    } else {
      try {
        Process process = Host.rabbitmqctl("status");
        String output = capture(process.getInputStream());
        return output.contains("stream/ssl");
      } catch (Exception e) {
        throw new RuntimeException("Error while trying to detect TLS: " + e.getMessage());
      }
    }
  }

  private static String capture(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    StringBuilder buff = new StringBuilder();
    while ((line = br.readLine()) != null) {
      buff.append(line).append("\n");
    }
    return buff.toString();
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
  @ExtendWith(DisabledIfTlsNotEnabledCondition.class)
  public @interface DisabledIfTlsNotEnabled {}

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

  static class CountDownLatchConditions {

    static Condition<CountDownLatch> completed() {
      return completed(Duration.ofSeconds(10));
    }

    static Condition<CountDownLatch> completed(int timeoutInSeconds) {
      return completed(Duration.ofSeconds(timeoutInSeconds));
    }

    static Condition<CountDownLatch> completed(Duration timeout) {
      return new Condition<>(
          latch -> {
            try {
              return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              Thread.interrupted();
              throw new RuntimeException(e);
            }
          },
          "completed in %d ms",
          timeout.toMillis());
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
      store(context).put("nettyEventLoopGroup", new NioEventLoopGroup());
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      try {
        Field streamField =
            context.getTestInstance().get().getClass().getDeclaredField("eventLoopGroup");
        streamField.setAccessible(true);
        streamField.set(context.getTestInstance().get(), eventLoopGroup(context));
      } catch (NoSuchFieldException e) {

      }
      try {
        Field streamField = context.getTestInstance().get().getClass().getDeclaredField("stream");
        streamField.setAccessible(true);
        String stream = streamName(context);
        streamField.set(context.getTestInstance().get(), stream);
        Client client =
            new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
        Client.Response response = client.create(stream);
        assertThat(response.isOk()).isTrue();
        client.close();
        store(context).put("testMethodStream", stream);
      } catch (NoSuchFieldException e) {

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
              eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
            } catch (InterruptedException e) {
              // happens at the end of the test suite
              LOGGER.debug("Error while asynchronously closing Netty event loop group", e);
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              LOGGER.warn("Error while asynchronously closing Netty event loop group", e);
            }
          });
    }

    private static class ExecutorServiceCloseableResourceWrapper implements CloseableResource {

      private final ExecutorService executorService;

      private ExecutorServiceCloseableResourceWrapper() {
        this.executorService = Executors.newCachedThreadPool();
      }

      @Override
      public void close() throws Throwable {
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

  static class DisabledIfRabbitMqCtlNotSetCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (Host.rabbitmqctlCommand() == null) {
        return ConditionEvaluationResult.disabled("rabbitmqctl.bin system property not set");
      } else {
        return ConditionEvaluationResult.enabled("rabbitmqctl.bin system property is set");
      }
    }
  }

  static class DisabledIfMqttNotEnabledCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (Host.rabbitmqctlCommand() == null) {
        return ConditionEvaluationResult.disabled(
            "rabbitmqctl.bin system property not set, cannot check if MQTT plugin is enabled");
      } else {
        try {
          Process process = Host.rabbitmqctl("status");
          String output = capture(process.getInputStream());
          if (output.contains("rabbitmq_mqtt") && output.contains("protocol: mqtt")) {
            return ConditionEvaluationResult.enabled("MQTT plugin enabled");
          } else {
            return ConditionEvaluationResult.disabled("MQTT plugin disabled");
          }
        } catch (Exception e) {
          return ConditionEvaluationResult.disabled(
              "Error while trying to detect MQTT plugin: " + e.getMessage());
        }
      }
    }
  }

  static class DisabledIfStompNotEnabledCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      if (Host.rabbitmqctlCommand() == null) {
        return ConditionEvaluationResult.disabled(
            "rabbitmqctl.bin system property not set, cannot check if STOMP plugin is enabled");
      } else {
        try {
          Process process = Host.rabbitmqctl("status");
          String output = capture(process.getInputStream());
          if (output.contains("rabbitmq_stomp") && output.contains("protocol: stomp")) {
            return ConditionEvaluationResult.enabled("STOMP plugin enabled");
          } else {
            return ConditionEvaluationResult.disabled("STOMP plugin disabled");
          }
        } catch (Exception e) {
          return ConditionEvaluationResult.disabled(
              "Error while trying to detect STOMP plugin: " + e.getMessage());
        }
      }
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

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

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
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(c);
    Level initialLevel = logger.getEffectiveLevel();
    logger.setLevel(level);
    return initialLevel;
  }

  static void waitMs(long waitTime) {
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Tag("single-active-consumer")
  public @interface SingleActiveConsumer {}

  public enum BrokerVersion {
    RABBITMQ_3_11("3.11.0");

    final String value;

    BrokerVersion(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }
  }
}

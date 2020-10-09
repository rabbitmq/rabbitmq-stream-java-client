// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.annotation.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.assertj.core.api.AssertDelegateTarget;
import org.junit.jupiter.api.extension.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

final class TestUtils {

  private TestUtils() {}

  static Duration waitAtMost(int timeoutInSeconds, BooleanSupplier condition)
      throws InterruptedException {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  static Duration waitAtMost(
      int timeoutInSeconds, BooleanSupplier condition, Supplier<String> message)
      throws InterruptedException {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    int waitTime = 100;
    int waitedTime = 0;
    int timeoutInMs = timeoutInSeconds * 1000;
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      waitedTime += waitTime;
      if (condition.getAsBoolean()) {
        return Duration.ofMillis(waitedTime);
      }
    }
    if (message == null) {
      fail("Waited " + timeoutInSeconds + " second(s), condition never got true");
    } else {
      fail("Waited " + timeoutInSeconds + " second(s), " + message.get());
    }
    return Duration.ofMillis(waitedTime);
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf, int publishCount, String stream) {
    publishAndWaitForConfirms(cf, "message", publishCount, stream);
  }

  static void publishAndWaitForConfirms(
      TestUtils.ClientFactory cf, String messagePrefix, int publishCount, String stream) {
    CountDownLatch latchConfirm = new CountDownLatch(publishCount);
    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, correlationId) -> latchConfirm.countDown();

    Client client =
        cf.get(new Client.ClientParameters().publishConfirmListener(publishConfirmListener));

    for (int i = 1; i <= publishCount; i++) {
      client.publish(
          stream,
          (byte) 1,
          Collections.singletonList(
              client
                  .messageBuilder()
                  .addData((messagePrefix + i).getBytes(StandardCharsets.UTF_8))
                  .build()));
    }

    try {
      assertThat(latchConfirm.await(60, SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  static Consumer<Object> namedTask(TaskWithException task, String description) {
    return new Consumer<Object>() {

      @Override
      public void accept(Object o) {
        try {
          task.run(o);
        } catch (Exception e) {
          throw new RuntimeException();
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

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ExtendWith(DisabledIfRabbitMqCtlNotSetCondition.class)
  @interface DisabledIfRabbitMqCtlNotSet {}

  interface TaskWithException {

    void run(Object context) throws Exception;
  }

  static class StreamTestInfrastructureExtension
      implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
        ExtensionContext.Namespace.create(StreamTestInfrastructureExtension.class);

    private static ExtensionContext.Store store(ExtensionContext extensionContext) {
      return extensionContext.getRoot().getStore(NAMESPACE);
    }

    private static EventLoopGroup eventLoopGroup(ExtensionContext context) {
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
        String stream = UUID.randomUUID().toString();
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
    public void afterAll(ExtensionContext context) throws Exception {
      EventLoopGroup eventLoopGroup = eventLoopGroup(context);
      eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }
  }

  static class ClientFactory {

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

  static <T> void forEach(Collection<T> in, CallableIndexConsumer<T> consumer) throws Exception {
    int count = 0;
    for (T t : in) {
      consumer.accept(count++, t);
    }
  }

  interface CallableIndexConsumer<T> {

    void accept(int index, T t) throws Exception;
  }

  interface CallableConsumer<T> {

    void accept(T t) throws Exception;
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

  static CountDownLatchAssert latchAssert(CountDownLatch latch) {
    return new CountDownLatchAssert(latch);
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
}

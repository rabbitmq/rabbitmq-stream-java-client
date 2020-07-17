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

package com.rabbitmq.stream;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apiguardian.api.API;
import org.junit.jupiter.api.extension.*;

import java.lang.annotation.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apiguardian.api.API.Status.STABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {

    public static void waitAtMost(int timeoutInSeconds, BooleanSupplier condition) throws InterruptedException {
        if (condition.getAsBoolean()) {
            return;
        }
        int waitTime = 100;
        int waitedTime = 0;
        int timeoutInMs = timeoutInSeconds * 1000;
        while (waitedTime <= timeoutInMs) {
            Thread.sleep(waitTime);
            if (condition.getAsBoolean()) {
                return;
            }
            waitedTime += waitTime;
        }
        fail("Waited " + timeoutInSeconds + " second(s), condition never got true");
    }

    static void publishAndWaitForConfirms(TestUtils.ClientFactory cf, int publishCount, String stream) {
        publishAndWaitForConfirms(cf, "message", publishCount, stream);
    }

    static void publishAndWaitForConfirms(TestUtils.ClientFactory cf, String messagePrefix, int publishCount, String stream) {
        CountDownLatch latchConfirm = new CountDownLatch(publishCount);
        Client.PublishConfirmListener publishConfirmListener = correlationId -> latchConfirm.countDown();

        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishConfirmListener));

        for (int i = 1; i <= publishCount; i++) {
            client.publish(stream, Collections.singletonList(client.messageBuilder().addData((messagePrefix + i).getBytes(StandardCharsets.UTF_8)).build()));
        }

        try {
            assertThat(latchConfirm.await(60, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @ExtendWith(DisabledIfRabbitMqCtlNotSetCondition.class)
    @API(status = STABLE, since = "5.1")
    public @interface DisabledIfRabbitMqCtlNotSet {


    }

    static class StreamTestInfrastructureExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

        private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(StreamTestInfrastructureExtension.class);

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
                Field streamField = context.getTestInstance().get().getClass().getDeclaredField("eventLoopGroup");
                streamField.setAccessible(true);
                streamField.set(context.getTestInstance().get(), eventLoopGroup(context));
            } catch (NoSuchFieldException e) {

            }
            try {
                Field streamField = context.getTestInstance().get().getClass().getDeclaredField("stream");
                streamField.setAccessible(true);
                String stream = UUID.randomUUID().toString();
                streamField.set(context.getTestInstance().get(), stream);
                Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
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
                Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context)));
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
}

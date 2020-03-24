// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class ClientTest {

    static EventLoopGroup eventLoopGroup;

    String target;
    int credit = 10;
    Set<Client> clients = ConcurrentHashMap.newKeySet();

    static void publishAndWaitForConfirms(int publishCount, String target) {
        long start = System.currentTimeMillis();
        CountDownLatch latchConfirm = new CountDownLatch(publishCount);
        Client.ConfirmListener confirmListener = correlationId -> latchConfirm.countDown();

        Client client = new Client(new Client.ClientParameters()
                .confirmListener(confirmListener).eventLoopGroup(eventLoopGroup));

        for (int i = 1; i <= publishCount; i++) {
            client.publish(target, ("message" + i).getBytes(StandardCharsets.UTF_8));
        }

        try {
            assertThat(latchConfirm.await(60, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        System.out.println("Loaded in " + (System.currentTimeMillis() - start));
        client.close();
    }

    private static void waitAtMost(int timeoutInSeconds, BooleanSupplier condition) throws InterruptedException {
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

    @BeforeAll
    static void initSuite() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void tearDownSuite() throws Exception {
        eventLoopGroup.shutdownGracefully().get(10, SECONDS);
    }

    @BeforeEach
    void init() {
        target = UUID.randomUUID().toString();
        Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup));
        Client.Response response = client.create(target);
        assertThat(response.isOk()).isTrue();
        client.close();
    }

    @AfterEach
    void tearDown() {
        Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup));
        Client.Response response = client.delete(target);
        assertThat(response.isOk()).isTrue();
        client.close();

        for (Client c : clients) {
            c.close();
        }
    }

    @ValueSource(ints = {1, 2, 3, 4, 5})
    @ParameterizedTest
    void metadataExistingTargets(int targetCount) throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();

        Client targetClient = client();
        String[] targets = IntStream.range(0, targetCount).mapToObj(i -> {
            String t = UUID.randomUUID().toString();
            targetClient.create(t);
            return t;
        }).toArray(String[]::new);

        Client client = client();
        Map<String, Client.TargetMetadata> metadata = client.metadata(targets);
        assertThat(metadata).hasSize(targetCount).containsKeys(targets);
        asList(targets).forEach(t -> {
            Client.TargetMetadata targetMetadata = metadata.get(t);
            assertThat(targetMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            assertThat(targetMetadata.getTarget()).isEqualTo(t);
            assertThat(targetMetadata.getLeader().getHost()).isEqualTo(hostname);
            assertThat(targetMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
            assertThat(targetMetadata.getReplicas()).isEmpty();
        });
        asList(targets).forEach(t -> targetClient.delete(t));
    }

    @Test
    void metadataOneNonExistingTarget() {
        Client client = client();
        String nonExistingTarget = UUID.randomUUID().toString();
        Map<String, Client.TargetMetadata> metadata = client.metadata(nonExistingTarget);
        assertThat(metadata).hasSize(1).containsKey(nonExistingTarget);
        Client.TargetMetadata targetMetadata = metadata.get(nonExistingTarget);
        assertThat(targetMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_TARGET_DOES_NOT_EXIST);
        assertThat(targetMetadata.getTarget()).isEqualTo(nonExistingTarget);
        assertThat(targetMetadata.getLeader()).isNull();
        assertThat(targetMetadata.getReplicas()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
            "1,1",
            "2,1",
            "5,1",
            "1,2",
            "2,2",
            "5,2",
            "1,3",
            "2,3",
            "5,3",
    })
    void metadataExistingNonExistingTargets(int existingCount, int nonExistingCount) throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();
        Client targetClient = client();
        List<String> existingTargets = IntStream.range(0, existingCount).mapToObj(i -> {
            String t = UUID.randomUUID().toString();
            targetClient.create(t);
            return t;
        }).collect(Collectors.toList());

        List<String> nonExistingTargets = IntStream.range(0, nonExistingCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());

        List<String> allTargets = new ArrayList<>(existingCount + nonExistingCount);
        allTargets.addAll(existingTargets);
        allTargets.addAll(nonExistingTargets);
        Collections.shuffle(allTargets);

        String[] targets = allTargets.toArray(new String[]{});


        Client client = client();
        Map<String, Client.TargetMetadata> metadata = client.metadata(targets);
        assertThat(metadata).hasSize(targets.length).containsKeys(targets);
        metadata.keySet().forEach(t -> {
            Client.TargetMetadata targetMetadata = metadata.get(t);
            assertThat(targetMetadata.getTarget()).isEqualTo(t);
            if (existingTargets.contains(t)) {
                assertThat(targetMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
                assertThat(targetMetadata.getLeader().getHost()).isEqualTo(hostname);
                assertThat(targetMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
                assertThat(targetMetadata.getReplicas()).isEmpty();
            } else {
                assertThat(targetMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_TARGET_DOES_NOT_EXIST);
                assertThat(targetMetadata.getTarget()).isEqualTo(t);
                assertThat(targetMetadata.getLeader()).isNull();
                assertThat(targetMetadata.getReplicas()).isEmpty();
            }
        });
        existingTargets.forEach(t -> targetClient.delete(t));
    }

    @Test
    void subscriptionListenerIsCalledWhenTargetIsDeleted() throws Exception {
        class TestParameters {
            final boolean sameClient;
            final int subscriptionCount;

            TestParameters(boolean sameClient, int subscriptionCount) {
                this.sameClient = sameClient;
                this.subscriptionCount = subscriptionCount;
            }
        }

        TestParameters[] testParameters = new TestParameters[]{
                new TestParameters(true, 1),
                new TestParameters(false, 1),
                new TestParameters(true, 5),
                new TestParameters(false, 5),
        };

        for (TestParameters testParameter : testParameters) {
            String t = UUID.randomUUID().toString();
            CountDownLatch subscriptionListenerLatch = new CountDownLatch(testParameter.subscriptionCount);
            List<Integer> cancelledSubscriptions = new CopyOnWriteArrayList<>();
            Client subscriptionClient = client(new Client.ClientParameters().subscriptionListener((subscriptionId, deletedTarget, reason) -> {
                if (t.equals(deletedTarget) && reason == Constants.RESPONSE_CODE_TARGET_DELETED) {
                    cancelledSubscriptions.add(subscriptionId);
                    subscriptionListenerLatch.countDown();
                }
            }));

            Client createDeleteClient;
            if (testParameter.sameClient) {
                createDeleteClient = subscriptionClient;
            } else {
                createDeleteClient = client();
            }

            createDeleteClient.create(t);

            IntStream.range(0, testParameter.subscriptionCount).forEach(i -> subscriptionClient.subscribe(i, t, 0, 10));

            createDeleteClient.delete(t);

            assertThat(subscriptionListenerLatch.await(5, SECONDS)).isTrue();
            assertThat(cancelledSubscriptions).hasSize(testParameter.subscriptionCount).containsAll(
                    IntStream.range(0, testParameter.subscriptionCount).boxed().collect(Collectors.toList())
            );
        }

    }

    @Test
    void severalSubscriptionsInSameConnection() throws Exception {
        int messageCount = 1000;
        Client publisher = client();

        ConcurrentMap<Integer, CountDownLatch> latches = new ConcurrentHashMap<>(2);
        latches.put(1, new CountDownLatch(messageCount * 2));
        latches.put(2, new CountDownLatch(messageCount));

        ConcurrentMap<Integer, AtomicInteger> messageCounts = new ConcurrentHashMap<>(2);
        Client consumer = client(new Client.ClientParameters().recordListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        consumer.subscribe(1, target, 0, messageCount * 100);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(target, ("" + i).getBytes()));

        waitAtMost(5, () -> messageCounts.computeIfAbsent(1, k -> new AtomicInteger(0)).get() == messageCount);

        consumer.subscribe(2, target, 0, messageCount * 100);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(target, ("" + i).getBytes()));

        assertThat(latches.get(1).await(5, SECONDS)).isTrue();
        assertThat(latches.get(2).await(5, SECONDS)).isTrue();
    }

    @Test
    void subscriptionToNonExistingTargetShouldReturnError() {
        String nonExistingTarget = UUID.randomUUID().toString();
        Client.Response response = client().subscribe(1, nonExistingTarget, 0, 10);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_TARGET_DOES_NOT_EXIST);
    }

    @Test
    void unsubscribeShouldNotReceiveMoreMessageAfterUnsubscribe() throws Exception {
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger receivedMessageCount = new AtomicInteger(0);
        Client client = client(new Client.ClientParameters().recordListener((correlationId, offset, message) -> {
            receivedMessageCount.incrementAndGet();
            latch.countDown();
        }));
        Client.Response response = client.subscribe(1, target, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();
        IntStream.range(0, messageCount).forEach(i -> client.publish(target, ("" + i).getBytes()));
        assertThat(latch.await(10, SECONDS)).isTrue();
        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        CountDownLatch latch2 = new CountDownLatch(messageCount);
        Client client2 = client(new Client.ClientParameters().recordListener((correlationId, offset, message) -> latch2.countDown()));
        client2.subscribe(1, target, 0, messageCount * 100);
        IntStream.range(0, messageCount).forEach(i -> client.publish(target, ("" + i).getBytes()));
        assertThat(latch2.await(10, SECONDS)).isTrue();
        Thread.sleep(1000L);
        assertThat(receivedMessageCount).hasValue(messageCount);
    }

    @Test
    void unsubscribeTwoSubscriptionsOneIsCancelled() throws Exception {
        int messageCount = 10;
        ConcurrentMap<Integer, CountDownLatch> latches = new ConcurrentHashMap<>(2);
        latches.put(1, new CountDownLatch(messageCount));
        latches.put(2, new CountDownLatch(messageCount * 2));
        ConcurrentMap<Integer, AtomicInteger> messageCounts = new ConcurrentHashMap<>(2);
        Client client = client(new Client.ClientParameters().recordListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        Client.Response response = client.subscribe(1, target, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();
        response = client.subscribe(2, target, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(target, ("" + i).getBytes()));
        assertThat(latches.get(1).await(10, SECONDS)).isTrue();

        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(target, ("" + i).getBytes()));
        assertThat(latches.get(2).await(10, SECONDS)).isTrue();
        assertThat(messageCounts.get(2)).hasValue(messageCount * 2);
        assertThat(messageCounts.get(1)).hasValue(messageCount);
    }

    @Test
    void unsubscribeNonExistingSubscriptionShouldReturnError() {
        Client client = client();
        Client.Response response = client.subscribe(1, target, 0, 10);
        assertThat(response.isOk()).isTrue();

        response = client.unsubscribe(42);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST);
    }

    @Test
    void subscriptionWithAlreadyExistingSubscriptionIdShouldReturnError() {
        Client client = client();
        Client.Response response = client.subscribe(1, target, 0, 20);
        assertThat(response.isOk()).isTrue();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

        response = client.subscribe(1, target, 0, 20);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS);
    }

    @Test
    void publishConfirm() throws Exception {
        int publishCount = 1000;

        Set<Long> confirmedIds = ConcurrentHashMap.newKeySet(publishCount);
        CountDownLatch latchConfirm = new CountDownLatch(1);
        Client.ConfirmListener confirmListener = correlationId -> {
            confirmedIds.add(correlationId);
            if (confirmedIds.size() == publishCount) {
                latchConfirm.countDown();
            }
        };

        Client client = client(new Client.ClientParameters().confirmListener(confirmListener));

        Set<Long> correlationIds = new HashSet<>(publishCount);
        for (int i = 1; i <= publishCount; i++) {
            long sequenceId = client.publish(target, ("message" + i).getBytes(StandardCharsets.UTF_8));
            correlationIds.add(sequenceId);
        }

        assertThat(latchConfirm.await(60, SECONDS)).isTrue();
        correlationIds.removeAll(confirmedIds);
        assertThat(correlationIds).isEmpty();
    }

    @Test
    public void publishConsumeComplexMessages() throws Exception {
        Client publisher = client();

        int publishCount = 1000;
        for (int i = 1; i <= publishCount; i++) {
            byte[] body = ("message" + i).getBytes(StandardCharsets.UTF_8);
            Map<String, Object> applicationProperties = Collections.singletonMap("id", i);
            Message message = new Message() {
                @Override
                public byte[] getBodyAsBinary() {
                    return body;
                }

                @Override
                public Object getBody() {
                    return null;
                }

                @Override
                public Properties getProperties() {
                    return null;
                }

                @Override
                public Map<String, Object> getApplicationProperties() {
                    return applicationProperties;
                }
            };
            publisher.publish(target, message);
        }

        CountDownLatch latch = new CountDownLatch(publishCount);
        Set<Message> messages = ConcurrentHashMap.newKeySet(publishCount);
        Client.ChunkListener chunkListener = (client, correlationId, offset, recordCount, dataSize) -> client.credit(correlationId, 1);
        Client.RecordListener recordListener = (correlationId, offset, message) -> {
            messages.add(message);
            latch.countDown();
        };
        Client consumer = client(new Client.ClientParameters().recordListener(recordListener).chunkListener(chunkListener));
        consumer.subscribe(1, target, 0, credit);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(messages).hasSize(publishCount);
        messages.stream().forEach(message -> {
            assertThat(message.getApplicationProperties()).hasSize(1);
            Integer id = (Integer) message.getApplicationProperties().get("id");
            assertThat(message.getBodyAsBinary()).isEqualTo(("message" + id).getBytes(StandardCharsets.UTF_8));
        });
        assertThat(consumer.unsubscribe(1).isOk()).isTrue();
    }

    @Test
    void consume() throws Exception {
        int publishCount = 100000;
        int correlationId = 42;
        publishAndWaitForConfirms(publishCount, target);
        MetricRegistry metrics = new MetricRegistry();
        Meter consumed = metrics.meter("consumed");

        CountDownLatch latch = new CountDownLatch(publishCount);

        AtomicInteger receivedCorrelationId = new AtomicInteger();
        Client.ChunkListener chunkListener = (client, corr, offset, recordCountInChunk, dataSize) -> {
            receivedCorrelationId.set(corr);
            client.credit(correlationId, 1);
        };

        Client.RecordListener recordListener = (corr, offset, message) -> {
            consumed.mark();
            latch.countDown();
        };

        Client client = client(new Client.ClientParameters()
                .chunkListener(chunkListener)
                .recordListener(recordListener));
        client.subscribe(correlationId, target, 0, credit);

        assertThat(latch.await(60, SECONDS)).isTrue();
        assertThat(receivedCorrelationId).hasValue(correlationId);
        client.close();
    }

    @Test
    void publishAndConsume() throws Exception {
        int publishCount = 1000000;
        MetricRegistry metrics = new MetricRegistry();
        Meter consumed = metrics.meter("consumed");
        Meter published = metrics.meter("published");
        Histogram chunkSize = metrics.histogram("chunk.size");

        CountDownLatch consumedLatch = new CountDownLatch(publishCount);
        Client.ChunkListener chunkListener = (client, correlationId, offset, recordCount, dataSize) -> {
            chunkSize.update(recordCount);
            if (consumedLatch.getCount() != 0) {
                client.credit(correlationId, 1);
            }
        };

        Client.RecordListener recordListener = (corr, offset, data) -> {
            consumed.mark();
            consumedLatch.countDown();
        };

        Client client = client(new Client.ClientParameters()
                .chunkListener(chunkListener)
                .recordListener(recordListener));
        client.subscribe(1, target, 0, credit);

        CountDownLatch confirmedLatch = new CountDownLatch(publishCount);
        new Thread(() -> {
            Client publisher = client(new Client.ClientParameters()
                    .confirmListener(correlationId -> confirmedLatch.countDown())
            );
            int messageId = 0;
            while (confirmedLatch.getCount() != 0) {
                messageId++;
                publisher.publish(target, ("message" + messageId).getBytes(StandardCharsets.UTF_8));
                published.mark();
            }
        }).start();

        assertThat(confirmedLatch.await(15, SECONDS)).isTrue();
        assertThat(consumedLatch.await(15, SECONDS)).isTrue();
        client.unsubscribe(1);
    }

    @Test
    void deleteNonExistingTargetShouldReturnError() {
        String nonExistingTarget = UUID.randomUUID().toString();
        Client.Response response = client().delete(nonExistingTarget);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_TARGET_DOES_NOT_EXIST);
    }

    @Test
    void createAlreadyExistingTargetShouldReturnError() {
        Client.Response response = client().create(target);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_TARGET_ALREADY_EXISTS);
    }

    @Test
    void filterSmallerOffsets() throws Exception {
        int messageCount = 50000;
        publishAndWaitForConfirms(messageCount, target);
        for (int i = 0; i < 10; i++) {
            Map<Integer, Long> firstOffsets = new ConcurrentHashMap<>();
            Map<Integer, CountDownLatch> latches = new ConcurrentHashMap<>();
            latches.put(1, new CountDownLatch(1));
            latches.put(2, new CountDownLatch(1));
            Client client = new Client(new Client.ClientParameters().recordListener((subscriptionId, offset, message) -> {
                if (firstOffsets.get(subscriptionId) == null) {
                    firstOffsets.put(subscriptionId, offset);
                }
                if (offset == messageCount - 1) {
                    latches.get(subscriptionId).countDown();
                }
            }).chunkListener((client1, subscriptionId, offset, recordCount, dataSize) -> client1.credit(subscriptionId, 1))
                    .eventLoopGroup(eventLoopGroup));
            client.subscribe(1, target, 50, 10);
            client.subscribe(2, target, 100, 10);

            assertThat(latches.get(1).await(10, SECONDS)).isTrue();
            assertThat(latches.get(2).await(10, SECONDS)).isTrue();
            assertThat(firstOffsets.get(1)).isEqualTo(50);
            assertThat(firstOffsets.get(2)).isEqualTo(100);
            client.close();
        }
    }

    @Test
    void publishToNonExistingTargetTriggersPublishErrorListener() throws Exception {
        int messageCount = 1000;
        AtomicInteger confirms = new AtomicInteger(0);
        Set<Short> responseCodes = ConcurrentHashMap.newKeySet(1);
        Set<Long> publishingIdErrors = ConcurrentHashMap.newKeySet(messageCount);
        CountDownLatch latch = new CountDownLatch(messageCount);
        Client client = client(new Client.ClientParameters()
                .confirmListener(publishingId -> confirms.incrementAndGet())
                .publishErrorListener((publishingId, responseCode) -> {
                    publishingIdErrors.add(publishingId);
                    responseCodes.add(responseCode);
                    latch.countDown();
                })
        );

        String nonExistingTarget = UUID.randomUUID().toString();
        Set<Long> publishingIds = ConcurrentHashMap.newKeySet(messageCount);
        IntStream.range(0, messageCount).forEach(i -> publishingIds.add(client.publish(nonExistingTarget, ("" + i).getBytes())));

        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(responseCodes).hasSize(1).contains(Constants.RESPONSE_CODE_TARGET_DOES_NOT_EXIST);
        assertThat(publishingIdErrors).hasSameSizeAs(publishingIds).hasSameElementsAs(publishingIdErrors);
    }

    @Test
    void shouldReachTailWhenPublisherStopWhileConsumerIsBehind() throws Exception {
        int messageCount = 100000;
        int messageLimit = messageCount * 2;
        AtomicLong lastConfirmed = new AtomicLong();
        CountDownLatch consumerStartLatch = new CountDownLatch(messageCount);
        Client publisher = client(new Client.ClientParameters()
                .confirmListener(publishingId -> {
                    lastConfirmed.set(publishingId);
                    consumerStartLatch.countDown();
                }));

        CountDownLatch consumedMessagesLatch = new CountDownLatch(messageLimit);
        AtomicReference<String> lastConsumedMessage = new AtomicReference<>();
        Client consumer = client(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, recordCount, dataSize) -> client.credit(0, 1))
                .recordListener((subscriptionId, offset, message) -> {
                    lastConsumedMessage.set(new String(message.getBodyAsBinary()));
                    consumedMessagesLatch.countDown();
                }));

        AtomicBoolean publisherHasStopped = new AtomicBoolean(false);
        new Thread(() -> {
            int publishedMessageCount = 0;
            while (true) {
                publisher.publish(target, String.valueOf(publishedMessageCount).getBytes());
                if (++publishedMessageCount == messageLimit) {
                    break;
                }
            }
            publisherHasStopped.set(true);
        }).start();

        assertThat(consumerStartLatch.await(10, SECONDS)).isTrue();

        assertThat(consumer.subscribe(0, target, 0, 10).isOk()).isTrue();
        assertThat(consumedMessagesLatch.await(10, SECONDS)).isTrue();
        assertThat(publisherHasStopped).isTrue();
        assertThat(lastConfirmed).hasValue(messageLimit - 1);
        assertThat(lastConsumedMessage).hasValue(String.valueOf(messageLimit - 1));
    }

    @Test
    void getSaslMechanismsShouldReturnDefaultSupportedMechanisms() {
        Client client = client();
        List<String> mechanisms = client.getSaslMechanisms();
        assertThat(mechanisms).hasSize(2).contains("PLAIN", "AMQPLAIN");
    }

    @Test
    void authenticateShouldPassWithValidCredentials() {
        Client client = client();
        client.authenticate();
    }

    @Test
    void authenticateWithJdkSaslConfiguration() {
        Client client = client(new Client.ClientParameters().saslConfiguration(new JdkSaslConfiguration(
                new DefaultUsernamePasswordCredentialsProvider("guest", "guest"), () -> "localhost"
        )));
        client.authenticate();
    }

    @Test
    void authenticateShouldFailWhenUsingBadCredentials() {
        Client client = client(new Client.ClientParameters().username("bad").password("bad"));
        try {
            client.authenticate();
        } catch (AuthenticationFailureException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE)));
        }
    }

    @Test
    void authenticateShouldFailWhenUsingUnsupportedSaslMechanism() {
        Client client = client(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
            @Override
            public String getName() {
                return "FANCY-SASL";
            }

            @Override
            public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
                return new byte[0];
            }
        }));
        try {
            client.authenticate();
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED)));
        }
    }

    @Test
    void authenticateShouldFailWhenSendingGarbageToSaslChallenge() {
        Client client = client(new Client.ClientParameters().saslConfiguration(mechanisms -> new SaslMechanism() {
            @Override
            public String getName() {
                return PlainSaslMechanism.INSTANCE.getName();
            }

            @Override
            public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
                return "blabla".getBytes(StandardCharsets.UTF_8);
            }
        }));
        try {
            client.authenticate();
        } catch (ClientException e) {
            assertThat(e.getMessage().contains(String.valueOf(Constants.RESPONSE_CODE_SASL_ERROR)));
        }
    }

    Client client() {
        return client(new Client.ClientParameters());
    }

    Client client(Client.ClientParameters parameters) {
        Client client = new Client(parameters.eventLoopGroup(eventLoopGroup));
        clients.add(client);
        return client;
    }
}

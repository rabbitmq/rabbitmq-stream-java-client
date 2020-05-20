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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.rabbitmq.stream.TestUtils.waitAtMost;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class ClientTest {

    static EventLoopGroup eventLoopGroup;

    static final Charset UTF8 = StandardCharsets.UTF_8;

    String stream;
    int credit = 10;
    Set<Client> clients = ConcurrentHashMap.newKeySet();

    static void publishAndWaitForConfirms(int publishCount, String stream) {
        long start = System.currentTimeMillis();
        CountDownLatch latchConfirm = new CountDownLatch(publishCount);
        Client.ConfirmListener confirmListener = correlationId -> latchConfirm.countDown();

        Client client = new Client(new Client.ClientParameters()
                .confirmListener(confirmListener).eventLoopGroup(eventLoopGroup));

        for (int i = 1; i <= publishCount; i++) {
            client.publish(stream, ("message" + i).getBytes(StandardCharsets.UTF_8));
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

    @BeforeAll
    static void initSuite() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void tearDownSuite() throws Exception {
        eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }

    static boolean await(CountDownLatch latch, Duration timeout) {
        try {
            return latch.await(timeout.getSeconds(), SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void init() {
        stream = UUID.randomUUID().toString();
        Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup));
        Client.Response response = client.create(stream);
        assertThat(response.isOk()).isTrue();
        client.close();
    }

    @AfterEach
    void tearDown() {
        Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup));
        Client.Response response = client.delete(stream);
        assertThat(response.isOk()).isTrue();
        client.close();

        for (Client c : clients) {
            c.close();
        }
    }

    @ValueSource(ints = {1, 2, 3, 4, 5})
    @ParameterizedTest
    void metadataExistingStreams(int streamCount) throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();

        Client streamClient = client();
        String[] streams = IntStream.range(0, streamCount).mapToObj(i -> {
            String t = UUID.randomUUID().toString();
            streamClient.create(t);
            return t;
        }).toArray(String[]::new);

        Client client = client();
        Map<String, Client.StreamMetadata> metadata = client.metadata(streams);
        assertThat(metadata).hasSize(streamCount).containsKeys(streams);
        asList(streams).forEach(t -> {
            Client.StreamMetadata streamMetadata = metadata.get(t);
            assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            assertThat(streamMetadata.getStream()).isEqualTo(t);
            assertThat(streamMetadata.getLeader().getHost()).isEqualTo(hostname);
            assertThat(streamMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
            assertThat(streamMetadata.getReplicas()).isEmpty();
        });
        asList(streams).forEach(t -> streamClient.delete(t));
    }

    @Test
    void metadataOneNonExistingStream() {
        Client client = client();
        String nonExistingStream = UUID.randomUUID().toString();
        Map<String, Client.StreamMetadata> metadata = client.metadata(nonExistingStream);
        assertThat(metadata).hasSize(1).containsKey(nonExistingStream);
        Client.StreamMetadata streamMetadata = metadata.get(nonExistingStream);
        assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        assertThat(streamMetadata.getStream()).isEqualTo(nonExistingStream);
        assertThat(streamMetadata.getLeader()).isNull();
        assertThat(streamMetadata.getReplicas()).isEmpty();
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
    void metadataExistingNonExistingStreams(int existingCount, int nonExistingCount) throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();
        Client streamClient = client();
        List<String> existingStreams = IntStream.range(0, existingCount).mapToObj(i -> {
            String t = UUID.randomUUID().toString();
            streamClient.create(t);
            return t;
        }).collect(Collectors.toList());

        List<String> nonExistingStreams = IntStream.range(0, nonExistingCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());

        List<String> allStreams = new ArrayList<>(existingCount + nonExistingCount);
        allStreams.addAll(existingStreams);
        allStreams.addAll(nonExistingStreams);
        Collections.shuffle(allStreams);

        String[] streams = allStreams.toArray(new String[]{});


        Client client = client();
        Map<String, Client.StreamMetadata> metadata = client.metadata(streams);
        assertThat(metadata).hasSize(streams.length).containsKeys(streams);
        metadata.keySet().forEach(t -> {
            Client.StreamMetadata streamMetadata = metadata.get(t);
            assertThat(streamMetadata.getStream()).isEqualTo(t);
            if (existingStreams.contains(t)) {
                assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
                assertThat(streamMetadata.getLeader().getHost()).isEqualTo(hostname);
                assertThat(streamMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
                assertThat(streamMetadata.getReplicas()).isEmpty();
            } else {
                assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
                assertThat(streamMetadata.getStream()).isEqualTo(t);
                assertThat(streamMetadata.getLeader()).isNull();
                assertThat(streamMetadata.getReplicas()).isEmpty();
            }
        });
        existingStreams.forEach(t -> streamClient.delete(t));
    }

    @Test
    void subscriptionListenerIsCalledWhenStreamIsDeleted() throws Exception {
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
            Client subscriptionClient = client(new Client.ClientParameters().subscriptionListener((subscriptionId, deletedStream, reason) -> {
                if (t.equals(deletedStream) && reason == Constants.RESPONSE_CODE_STREAM_DELETED) {
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
        Client consumer = client(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        consumer.subscribe(1, stream, 0, messageCount * 2);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("" + i).getBytes()));

        waitAtMost(5, () -> messageCounts.computeIfAbsent(1, k -> new AtomicInteger(0)).get() == messageCount);

        consumer.subscribe(2, stream, 0, messageCount * 2);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("" + i).getBytes()));

        assertThat(latches.get(1).await(5, SECONDS)).isTrue();
        assertThat(latches.get(2).await(5, SECONDS)).isTrue();
    }

    @Test
    void subscriptionToNonExistingStreamShouldReturnError() {
        String nonExistingStream = UUID.randomUUID().toString();
        Client.Response response = client().subscribe(1, nonExistingStream, 0, 10);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }

    @Test
    void subscriptionToNonStreamQueueShouldReturnError() throws Exception {
        String nonStreamQueue = UUID.randomUUID().toString();
        ConnectionFactory cf = new ConnectionFactory();
        try (Connection amqpConnection = cf.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.queueDeclare(nonStreamQueue, false, true, false, null);

            Client.Response response = client().subscribe(1, nonStreamQueue, 0, 10);
            assertThat(response.isOk()).isFalse();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        }
    }


    @Test
    void unsubscribeShouldNotReceiveMoreMessageAfterUnsubscribe() throws Exception {
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger receivedMessageCount = new AtomicInteger(0);
        Client client = client(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            receivedMessageCount.incrementAndGet();
            latch.countDown();
        }));
        Client.Response response = client.subscribe(1, stream, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();
        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latch.await(10, SECONDS)).isTrue();
        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        CountDownLatch latch2 = new CountDownLatch(messageCount);
        Client client2 = client(new Client.ClientParameters().messageListener((correlationId, offset, message) -> latch2.countDown()));
        client2.subscribe(1, stream, 0, messageCount * 100);
        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
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
        Client client = client(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        Client.Response response = client.subscribe(1, stream, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();
        response = client.subscribe(2, stream, 0, messageCount * 100);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latches.get(1).await(10, SECONDS)).isTrue();

        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latches.get(2).await(10, SECONDS)).isTrue();
        assertThat(messageCounts.get(2)).hasValue(messageCount * 2);
        assertThat(messageCounts.get(1)).hasValue(messageCount);

        client.unsubscribe(2);
    }

    @Test
    void unsubscribeNonExistingSubscriptionShouldReturnError() {
        Client client = client();
        Client.Response response = client.subscribe(1, stream, 0, 10);
        assertThat(response.isOk()).isTrue();

        response = client.unsubscribe(42);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST);
    }

    @Test
    void subscriptionWithAlreadyExistingSubscriptionIdShouldReturnError() {
        Client client = client();
        Client.Response response = client.subscribe(1, stream, 0, 20);
        assertThat(response.isOk()).isTrue();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

        response = client.subscribe(1, stream, 0, 20);
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
            long sequenceId = client.publish(stream, ("message" + i).getBytes(StandardCharsets.UTF_8));
            correlationIds.add(sequenceId);
        }

        assertThat(latchConfirm.await(60, SECONDS)).isTrue();
        correlationIds.removeAll(confirmedIds);
        assertThat(correlationIds).isEmpty();
    }

    void publishConsumeComplexMessage(Client publisher, Codec codec, Function<Integer, Message> messageFactory) {
        int publishCount = 1000;
        for (int i = 1; i <= publishCount; i++) {
            publisher.publish(stream, messageFactory.apply(i));
        }

        CountDownLatch latch = new CountDownLatch(publishCount);
        Set<Message> messages = ConcurrentHashMap.newKeySet(publishCount);
        Client.ChunkListener chunkListener = (client, correlationId, offset, messageCount, dataSize) -> client.credit(correlationId, 1);
        Client.MessageListener messageListener = (correlationId, offset, message) -> {
            messages.add(message);
            latch.countDown();
        };
        Client consumer = client(new Client.ClientParameters()
                .codec(codec)
                .messageListener(messageListener).chunkListener(chunkListener));
        consumer.subscribe(1, stream, 0, credit);
        assertThat(await(latch, Duration.ofSeconds(10))).isTrue();
        assertThat(messages).hasSize(publishCount);
        messages.stream().forEach(message -> {
            assertThat(message.getApplicationProperties()).hasSize(1);
            Integer id = (Integer) message.getApplicationProperties().get("id");
            assertThat(message.getBodyAsBinary()).isEqualTo(("message" + id).getBytes(StandardCharsets.UTF_8));
        });
        assertThat(consumer.unsubscribe(1).isOk()).isTrue();
    }

    @Test
    void publishConsumeComplexMessagesWithMessageImplementationAndSwiftMqCodec() {
        Codec codec = new SwiftMqCodec();
        Client publisher = client(new Client.ClientParameters().codec(codec));
        publishConsumeComplexMessage(publisher, codec, i -> {
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

                @Override
                public Map<String, Object> getMessageAnnotations() {
                    return null;
                }
            };
            return message;
        });
    }

    @Test
    void publishConsumeComplexMessageWithMessageBuilderAndSwiftMqCodec() {
        Codec codec = new SwiftMqCodec();
        Client publisher = client(new Client.ClientParameters().codec(codec));
        publishConsumeComplexMessage(publisher, codec, i -> publisher.messageBuilder().applicationProperties().entry("id", i)
                .messageBuilder().addData(("message" + i).getBytes(StandardCharsets.UTF_8)).build());
    }

    @Test
    void publishConsumeComplexMessageWithMessageBuilderAndQpidProtonCodec() {
        Codec codec = new QpidProtonCodec();
        Client publisher = client(new Client.ClientParameters().codec(codec));
        publishConsumeComplexMessage(publisher, codec, i -> publisher.messageBuilder().applicationProperties().entry("id", i)
                .messageBuilder().addData(("message" + i).getBytes(StandardCharsets.UTF_8)).build());
    }

    @Test
    void publishConsumeWithSimpleCodec() throws Exception {
        int messageCount = 1000;
        Codec codec = new SimpleCodec();
        Client publisher = client(new Client.ClientParameters().codec(codec));
        IntStream.range(0, 1000).forEach(i -> publisher.publish(stream, String.valueOf(i).getBytes(UTF8)));

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Set<String> messageBodies = ConcurrentHashMap.newKeySet(messageCount);
        Client consumer = client(new Client.ClientParameters()
                .codec(codec)
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    messageBodies.add(new String(message.getBodyAsBinary()));
                    consumeLatch.countDown();
                })
        );

        consumer.subscribe(1, stream, 0, 10);
        assertThat(consumeLatch.await(10, SECONDS)).isTrue();
        IntStream.range(0, messageCount).forEach(i -> assertThat(messageBodies).contains(String.valueOf(i)));
    }

    @Test
    void batchPublishing() throws Exception {
        int batchCount = 500;
        int batchSize = 10;
        int payloadSize = 20;
        int messageCount = batchCount * batchSize;
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Client publisher = client(new Client.ClientParameters()
                .confirmListener(publishingId -> publishLatch.countDown())
        );
        AtomicInteger publishingSequence = new AtomicInteger(0);
        IntStream.range(0, batchCount).forEach(batchIndex -> {
            publisher.publishBinary(stream, IntStream.range(0, batchSize)
                    .mapToObj(i -> {
                        int sequence = publishingSequence.getAndIncrement();
                        ByteBuffer b = ByteBuffer.allocate(payloadSize);
                        b.putInt(sequence);
                        return b.array();
                    })
                    .collect(Collectors.toList()));
        });

        assertThat(publishLatch.await(10, SECONDS)).isTrue();

        Set<Integer> sizes = ConcurrentHashMap.newKeySet(1);
        Set<Integer> sequences = ConcurrentHashMap.newKeySet(messageCount);
        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Client consumer = client(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    ByteBuffer bb = ByteBuffer.wrap(message.getBodyAsBinary());
                    sizes.add(message.getBodyAsBinary().length);
                    sequences.add(bb.getInt());
                    consumeLatch.countDown();
                }));

        consumer.subscribe(1, stream, 0, 10);
        assertThat(consumeLatch.await(10, SECONDS)).isTrue();
        assertThat(sizes).hasSize(1).containsOnly(payloadSize);
        assertThat(sequences).hasSize(messageCount);
        IntStream.range(0, messageCount + 1).forEach(value -> assertThat(sequences).contains(value));
    }

    @Test
    void consume() throws Exception {
        int publishCount = 100000;
        int correlationId = 42;
        publishAndWaitForConfirms(publishCount, stream);
        MetricRegistry metrics = new MetricRegistry();
        Meter consumed = metrics.meter("consumed");

        CountDownLatch latch = new CountDownLatch(publishCount);

        AtomicInteger receivedCorrelationId = new AtomicInteger();
        Client.ChunkListener chunkListener = (client, corr, offset, messageCountInChunk, dataSize) -> {
            receivedCorrelationId.set(corr);
            client.credit(correlationId, 1);
        };

        Client.MessageListener messageListener = (corr, offset, message) -> {
            consumed.mark();
            latch.countDown();
        };

        Client client = client(new Client.ClientParameters()
                .chunkListener(chunkListener)
                .messageListener(messageListener));
        client.subscribe(correlationId, stream, 0, credit);

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
        Client.ChunkListener chunkListener = (client, correlationId, offset, messageCount, dataSize) -> {
            chunkSize.update(messageCount);
            if (consumedLatch.getCount() != 0) {
                client.credit(correlationId, 1);
            }
        };

        Client.MessageListener messageListener = (corr, offset, data) -> {
            consumed.mark();
            consumedLatch.countDown();
        };

        Client client = client(new Client.ClientParameters()
                .chunkListener(chunkListener)
                .messageListener(messageListener));
        client.subscribe(1, stream, 0, credit);

        CountDownLatch confirmedLatch = new CountDownLatch(publishCount);
        new Thread(() -> {
            Client publisher = client(new Client.ClientParameters()
                    .confirmListener(correlationId -> confirmedLatch.countDown())
            );
            int messageId = 0;
            while (messageId < publishCount) {
                messageId++;
                publisher.publish(stream, ("message" + messageId).getBytes(StandardCharsets.UTF_8));
                published.mark();
            }
        }).start();

        assertThat(confirmedLatch.await(15, SECONDS)).isTrue();
        assertThat(consumedLatch.await(15, SECONDS)).isTrue();
        client.unsubscribe(1);
    }

    @Test
    void consumeFromTail() throws Exception {
        int messageCount = 10000;
        CountDownLatch firstWaveLatch = new CountDownLatch(messageCount);
        CountDownLatch secondWaveLatch = new CountDownLatch(messageCount * 2);
        Client publisher = client(new Client.ClientParameters()
                .confirmListener(publishingId -> {
                    firstWaveLatch.countDown();
                    secondWaveLatch.countDown();
                }));
        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("first wave " + i).getBytes(StandardCharsets.UTF_8)));
        assertThat(firstWaveLatch.await(10, SECONDS)).isTrue();

        CountDownLatch consumedLatch = new CountDownLatch(messageCount);

        Set<String> consumed = ConcurrentHashMap.newKeySet();
        Client consumer = new Client(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    consumed.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
                    consumedLatch.countDown();
                }));

        consumer.subscribe(1, stream, -1, 10);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("second wave " + i).getBytes(StandardCharsets.UTF_8)));

        assertThat(consumedLatch.await(10, SECONDS)).isTrue();
        assertThat(consumed).hasSize(messageCount);
        consumed.stream().forEach(v -> assertThat(v).startsWith("second wave").doesNotStartWith("first wave"));
    }

    @Test
    void deleteNonExistingStreamShouldReturnError() {
        String nonExistingStream = UUID.randomUUID().toString();
        Client.Response response = client().delete(nonExistingStream);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }

    @Test
    void deleteNonStreamQueueShouldReturnError() throws Exception {
        String nonStreamQueue = UUID.randomUUID().toString();
        ConnectionFactory cf = new ConnectionFactory();
        try (Connection amqpConnection = cf.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.queueDeclare(nonStreamQueue, false, true, false, null);
            Client.Response response = client().delete(nonStreamQueue);
            assertThat(response.isOk()).isFalse();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        }
    }

    @Test
    void createAlreadyExistingStreamShouldReturnError() {
        Client.Response response = client().create(stream);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_ALREADY_EXISTS);
    }

    @Test
    void retention() throws Exception {
        int messageCount = 1000;
        int payloadSize = 1000;
        class TestConfig {
            final Supplier<Map<String, String>> arguments;
            final Predicate<Long> firstMessageIdAssertion;

            TestConfig(Supplier<Map<String, String>> arguments, Predicate<Long> firstMessageIdAssertion) {
                this.arguments = arguments;
                this.firstMessageIdAssertion = firstMessageIdAssertion;
            }
        }

        TestConfig[] configurations = new TestConfig[]{
                new TestConfig(
                        () -> new Client.StreamParametersBuilder()
                                .maxLengthBytes(messageCount * payloadSize / 10)
                                .maxSegmentSizeBytes(messageCount * payloadSize / 20)
                                .build(),
                        firstMessageId -> firstMessageId > 0),
                new TestConfig(
                        () -> new Client.StreamParametersBuilder()
                                .maxLengthBytes(ByteCapacity.B(messageCount * payloadSize / 10))
                                .maxSegmentSizeBytes(ByteCapacity.B(messageCount * payloadSize / 20))
                                .build(),
                        firstMessageId -> firstMessageId > 0),
                new TestConfig(() -> Collections.emptyMap(), firstMessageId -> firstMessageId == 0)
        };

        for (TestConfig configuration : configurations) {
            String testStream = UUID.randomUUID().toString();
            CountDownLatch publishingLatch = new CountDownLatch(messageCount);
            Client publisher = client(new Client.ClientParameters()
                    .confirmListener(publishingId -> publishingLatch.countDown())
            );

            Map<String, String> arguments = configuration.arguments.get();
            try {
                publisher.create(testStream, arguments);
                AtomicLong publishSequence = new AtomicLong(0);
                byte[] payload = new byte[payloadSize];
                IntStream.range(0, messageCount).forEach(i -> publisher.publish(testStream,
                        publisher.messageBuilder().properties().messageId(publishSequence.getAndIncrement())
                                .messageBuilder().addData(payload).build())
                );
                assertThat(publishingLatch.await(10, SECONDS)).isTrue();

                CountDownLatch consumingLatch = new CountDownLatch(1);
                AtomicLong firstMessageId = new AtomicLong(-1);
                Client consumer = client(new Client.ClientParameters()
                        .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                        .messageListener((subscriptionId, offset, message) -> {
                            long messageId = message.getProperties().getMessageIdAsLong();
                            firstMessageId.compareAndSet(-1, messageId);
                            if (messageId == publishSequence.get() - 1) {
                                consumingLatch.countDown();
                            }
                        }));

                consumer.subscribe(1, testStream, 0, 10);
                assertThat(consumingLatch.await(10, SECONDS)).isTrue();
                consumer.unsubscribe(1);
                assertThat(configuration.firstMessageIdAssertion.test(firstMessageId.get())).isTrue();
            } finally {
                publisher.delete(testStream);
            }
        }
    }

    @Test
    void filterSmallerOffsets() throws Exception {
        int messageCount = 50000;
        publishAndWaitForConfirms(messageCount, stream);
        for (int i = 0; i < 10; i++) {
            Map<Integer, Long> firstOffsets = new ConcurrentHashMap<>();
            Map<Integer, CountDownLatch> latches = new ConcurrentHashMap<>();
            latches.put(1, new CountDownLatch(1));
            latches.put(2, new CountDownLatch(1));
            Client client = new Client(new Client.ClientParameters().messageListener((subscriptionId, offset, message) -> {
                if (firstOffsets.get(subscriptionId) == null) {
                    firstOffsets.put(subscriptionId, offset);
                }
                if (offset == messageCount - 1) {
                    latches.get(subscriptionId).countDown();
                }
            }).chunkListener((client1, subscriptionId, offset, msgCount, dataSize) -> client1.credit(subscriptionId, 1))
                    .eventLoopGroup(eventLoopGroup));
            client.subscribe(1, stream, 50, 10);
            client.subscribe(2, stream, 100, 10);

            assertThat(latches.get(1).await(10, SECONDS)).isTrue();
            assertThat(latches.get(2).await(10, SECONDS)).isTrue();
            assertThat(firstOffsets.get(1)).isEqualTo(50);
            assertThat(firstOffsets.get(2)).isEqualTo(100);
            client.close();
        }
    }

    @Test
    void publishToNonExistingStreamTriggersPublishErrorListener() throws Exception {
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

        String nonExistingStream = UUID.randomUUID().toString();
        Set<Long> publishingIds = ConcurrentHashMap.newKeySet(messageCount);
        IntStream.range(0, messageCount).forEach(i -> publishingIds.add(client.publish(nonExistingStream, ("" + i).getBytes())));

        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(responseCodes).hasSize(1).contains(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        assertThat(publishingIdErrors).hasSameSizeAs(publishingIds).hasSameElementsAs(publishingIdErrors);
    }

    @Test
    void publishToNonStreamQueueTriggersPublishErrorListener() throws Exception {
        String nonStreamQueue = UUID.randomUUID().toString();
        ConnectionFactory cf = new ConnectionFactory();
        try (Connection amqpConnection = cf.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.queueDeclare(nonStreamQueue, false, true, false, null);

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

            Set<Long> publishingIds = ConcurrentHashMap.newKeySet(messageCount);
            IntStream.range(0, messageCount).forEach(i -> publishingIds.add(client.publish(nonStreamQueue, ("" + i).getBytes())));

            assertThat(latch.await(10, SECONDS)).isTrue();
            assertThat(responseCodes).hasSize(1).contains(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
            assertThat(publishingIdErrors).hasSameSizeAs(publishingIds).hasSameElementsAs(publishingIdErrors);
        }
    }

    @Test
    void declareAmqpStreamQueueAndUseItAsStream() throws Exception {
        int messageCount = 10000;
        String q = UUID.randomUUID().toString();
        ConnectionFactory cf = new ConnectionFactory();
        try (Connection amqpConnection = cf.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.queueDeclare(q, true, false, false, Collections.singletonMap("x-queue-type", "stream"));
            CountDownLatch publishedLatch = new CountDownLatch(messageCount);
            CountDownLatch consumedLatch = new CountDownLatch(messageCount);
            Client client = client(new Client.ClientParameters()
                    .confirmListener(publishingId -> publishedLatch.countDown())
                    .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                    .messageListener((subscriptionId, offset, message) -> consumedLatch.countDown())
            );

            IntStream.range(0, messageCount).forEach(i -> client.publish(q, "hello".getBytes(StandardCharsets.UTF_8)));
            assertThat(publishedLatch.await(10, SECONDS)).isTrue();

            client.subscribe(1, q, 0, 10);
            assertThat(consumedLatch.await(10, SECONDS)).isTrue();
        }
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
                .chunkListener((client, subscriptionId, offset, msgCount, dataSize) -> client.credit(0, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    lastConsumedMessage.set(new String(message.getBodyAsBinary()));
                    consumedMessagesLatch.countDown();
                }));

        AtomicBoolean publisherHasStopped = new AtomicBoolean(false);
        new Thread(() -> {
            int publishedMessageCount = 0;
            while (true) {
                publisher.publish(stream, String.valueOf(publishedMessageCount).getBytes());
                if (++publishedMessageCount == messageLimit) {
                    break;
                }
            }
            publisherHasStopped.set(true);
        }).start();

        assertThat(consumerStartLatch.await(10, SECONDS)).isTrue();

        assertThat(consumer.subscribe(0, stream, 0, 10).isOk()).isTrue();
        assertThat(consumedMessagesLatch.await(10, SECONDS)).isTrue();
        assertThat(publisherHasStopped).isTrue();
        assertThat(lastConfirmed).hasValue(messageLimit - 1);
        assertThat(lastConsumedMessage).hasValue(String.valueOf(messageLimit - 1));
    }

    @Test
    void serverShouldSendCloseWhenSendingGarbage() throws Exception {
        Client client = client();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        dataOutputStream.writeInt(4);
        dataOutputStream.writeShort(30000); // command ID
        dataOutputStream.writeShort(Constants.VERSION_0);
        client.send(out.toByteArray());
        waitAtMost(10, () -> client.isOpen() == false);
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

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

import com.rabbitmq.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;

class DefaultClientSubscriptions implements ClientSubscriptions {

    static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;

    static final Duration METADATA_UPDATE_DEFAULT_INITIAL_DELAY = Duration.ofSeconds(5);
    static final Duration METADATA_UPDATE_DEFAULT_RETRY_DELAY = Duration.ofSeconds(1);
    static final Duration METADATA_UPDATE_DEFAULT_RETRY_TIMEOUT = Duration.ofSeconds(60);
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientSubscriptions.class);
    private final Random random = new Random();
    private final AtomicLong globalSubscriptionIdSequence = new AtomicLong(0);
    private final StreamEnvironment environment;
    private final Map<String, ClientSubscriptionPool> clientSubscriptionPools = new ConcurrentHashMap<>();
    private final Map<Long, StreamSubscription> streamSubscriptionRegistry = new ConcurrentHashMap<>();
    private final Function<Client.ClientParameters, Client> clientFactory;
    volatile Duration metadataUpdateInitialDelay = METADATA_UPDATE_DEFAULT_INITIAL_DELAY;
    volatile Duration metadataUpdateRetryDelay = METADATA_UPDATE_DEFAULT_RETRY_DELAY;
    volatile Duration metadataUpdateRetryTimeout = METADATA_UPDATE_DEFAULT_RETRY_TIMEOUT;


    DefaultClientSubscriptions(StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
        this.environment = environment;
        this.clientFactory = clientFactory;
    }

    DefaultClientSubscriptions(StreamEnvironment environment) {
        this(environment, cp -> new Client(cp));
    }

    private static String keyForClientSubscriptionState(Client.Broker broker) {
        // FIXME make sure this is a reasonable key for brokers
        return broker.getHost() + ":" + broker.getPort();
    }

    @Override
    public long subscribe(StreamConsumer consumer, String stream, OffsetSpecification offsetSpecification, MessageHandler messageHandler) {
        // FIXME fail immediately if there's no locator (can provide a supplier that does not retry)
        List<Client.Broker> candidates = findBrokersForStream(stream);
        Client.Broker newNode = pickBroker(candidates);

        long streamSubscriptionId = globalSubscriptionIdSequence.getAndIncrement();
        // create stream subscription to track final and changing state of this very subscription
        // we keep this instance when we move the subscription from a client to another one
        StreamSubscription streamSubscription = new StreamSubscription(streamSubscriptionId, consumer, stream, messageHandler);

        String key = keyForClientSubscriptionState(newNode);

        ClientSubscriptionPool clientSubscriptionPool = clientSubscriptionPools.computeIfAbsent(key, s -> new ClientSubscriptionPool(
                key,
                environment
                        .clientParametersCopy()
                        .host(newNode.getHost())
                        .port(newNode.getPort())
        ));

        clientSubscriptionPool.add(streamSubscription, offsetSpecification);
        streamSubscriptionRegistry.put(streamSubscription.id, streamSubscription);
        return streamSubscriptionId;
    }

    private Client locator() {
        return environment.locator();
    }

    // package protected for testing
    List<Client.Broker> findBrokersForStream(String stream) {
        // FIXME make sure locator is not null (retry)
        Map<String, Client.StreamMetadata> metadata = locator().metadata(stream);
        if (metadata.size() == 0 || metadata.get(stream) == null) {
            // this is not supposed to happen
            throw new StreamDoesNotExistException(stream);
        }

        Client.StreamMetadata streamMetadata = metadata.get(stream);
        if (!streamMetadata.isResponseOk()) {
            if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
                throw new StreamDoesNotExistException(stream);
            } else {
                throw new IllegalStateException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
            }
        }

        List<Client.Broker> replicas = streamMetadata.getReplicas();
        if ((replicas == null || replicas.isEmpty()) && streamMetadata.getLeader() == null) {
            throw new IllegalStateException("No node available to consume from stream " + stream);
        }

        List<Client.Broker> brokers;
        if (replicas == null || replicas.isEmpty()) {
            brokers = Collections.singletonList(streamMetadata.getLeader());
            LOGGER.debug("Consuming from {} on leader node {}", stream, streamMetadata.getLeader());
        } else {
            LOGGER.debug("Replicas for consuming from {}: {}", stream, replicas);
            brokers = new ArrayList<>(replicas);
        }

        LOGGER.debug("Candidates to consume from {}: {}", stream, brokers);

        return brokers;
    }

    private Client.Broker pickBroker(List<Client.Broker> brokers) {
        if (brokers.isEmpty()) {
            return null;
        } else if (brokers.size() == 1) {
            return brokers.get(0);
        } else {
            return brokers.get(random.nextInt(brokers.size()));
        }
    }

    @Override
    public void unsubscribe(long id) {
        // TODO consider closing the corresponding subscriptions state if there's no more subscription in it
        StreamSubscription streamSubscription = this.streamSubscriptionRegistry.get(id);
        if (streamSubscription != null) {
            streamSubscription.cancel();
        }
    }

    @Override
    public void close() {
        for (ClientSubscriptionPool subscriptionPool : this.clientSubscriptionPools.values()) {
            subscriptionPool.close();
        }

    }

    private static class StreamSubscription {

        private final long id;
        private final String stream;
        private final MessageHandler messageHandler;
        private final StreamConsumer consumer;
        private volatile long offset;
        private volatile int subscriptionIdInClient;
        private volatile SubscriptionState subscriptionState;

        private StreamSubscription(long id, StreamConsumer consumer, String stream, MessageHandler messageHandler) {
            this.id = id;
            this.consumer = consumer;
            this.stream = stream;
            this.messageHandler = messageHandler;
        }

        private void cancel() {
            this.subscriptionState.remove(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamSubscription that = (StreamSubscription) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private class ClientSubscriptionPool {

        private final List<SubscriptionState> states = new CopyOnWriteArrayList<>();
        private final String name;
        private final Client.ClientParameters clientParameters;
        private final java.util.function.Consumer<SubscriptionState> cleanCallback;

        private ClientSubscriptionPool(String name, Client.ClientParameters clientParameters) {
            this.name = name;
            this.clientParameters = clientParameters;
            this.cleanCallback = subscriptionState -> {
                synchronized (ClientSubscriptionPool.this) {
                    if (subscriptionState.isEmpty()) {
                        subscriptionState.close();
                        states.remove(subscriptionState);
                        LOGGER.debug("Closed subscription state on {}, {} remaining", name, states.size());
                        if (states.isEmpty()) {
                            clientSubscriptionPools.remove(name);
                            LOGGER.debug("Closed client subscription pool on {} because it was empty", name);
                        }
                    }

                }
            };
            LOGGER.debug("Creating client subscription pool on {}", name);
            states.add(new SubscriptionState(name, clientParameters, cleanCallback));
        }

        synchronized void add(StreamSubscription streamSubscription, OffsetSpecification offsetSpecification) {
            boolean added = false;
            for (SubscriptionState state : states) {
                if (!state.isFull()) {
                    state.add(streamSubscription, offsetSpecification);
                    added = true;
                    break;
                }
            }
            if (!added) {
                LOGGER.debug("Creating subscription state on {}, this is #{}", name, states.size() + 1);
                SubscriptionState state = new SubscriptionState(name, clientParameters, cleanCallback);
                states.add(state);
                state.add(streamSubscription, offsetSpecification);
            }
        }

        synchronized void close() {
            Iterator<SubscriptionState> iterator = states.iterator();
            while (iterator.hasNext()) {
                SubscriptionState state = iterator.next();
                state.close();
            }
            states.clear();
        }
    }

    private class SubscriptionState {

        private final Client client;
        private final java.util.function.Consumer<SubscriptionState> cleanCallback;
        private final Map<String, Set<StreamSubscription>> streamToStreamSubscriptions = new ConcurrentHashMap<>();
        private final Set<Long> globalStreamSubscriptionIds = ConcurrentHashMap.newKeySet();
        // the 3 following data structures track the subscriptions, they must remain consistent
        private volatile List<StreamSubscription> streamSubscriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);

        private SubscriptionState(String name, Client.ClientParameters clientParameters,
                                  java.util.function.Consumer<SubscriptionState> cleanCallback) {
            LOGGER.debug("creating subscription state on {}", name);
            IntStream.range(0, MAX_SUBSCRIPTIONS_PER_CLIENT).forEach(i -> streamSubscriptions.add(null));
            this.cleanCallback = cleanCallback;
            this.client = clientFactory.apply(clientParameters
                    .clientProperty("name", "rabbitmq-stream-consumer")
                    .chunkListener((client, subscriptionId, offset, messageCount, dataSize) -> client.credit(subscriptionId, 1))
                    .creditNotification((subscriptionId, responseCode) -> LOGGER.debug("Received credit notification for subscription {}: {}", subscriptionId, responseCode))
                    .messageListener((subscriptionId, offset, message) -> {
                        StreamSubscription streamSubscription = streamSubscriptions.get(subscriptionId);
                        if (streamSubscription != null) {
                            streamSubscription.offset = offset;
                            streamSubscription.messageHandler.handle(offset, message);
                            // FIXME set offset here as well, best effort to avoid duplicates
                        } else {
                            LOGGER.warn("Could not find stream subscription {}", subscriptionId);
                        }
                    })
                    .shutdownListener(shutdownContext -> {
                        if (shutdownContext.isShutdownUnexpected()) {
                            clientSubscriptionPools.remove(name);
                            LOGGER.debug("Unexpected shutdown notification on subscription client {}, scheduling consumers re-assignment", name);
                            environment.scheduledExecutorService().schedule(() -> {
                                for (Map.Entry<String, Set<StreamSubscription>> entry : streamToStreamSubscriptions.entrySet()) {
                                    String stream = entry.getKey();
                                    LOGGER.debug("Re-assigning consumers to stream {} after disconnection");
                                    assignConsumersToStream(
                                            entry.getValue(), stream,
                                            attempt -> environment.recoveryBackOffDelayPolicy().delay(attempt + 1), // already waited once
                                            Duration.ZERO
                                    );
                                }
                            }, environment.recoveryBackOffDelayPolicy().delay(0).toMillis(), TimeUnit.MILLISECONDS);
                        }
                    })
                    .metadataListener((stream, code) -> {
                        LOGGER.debug("Received metadata notification for {}, stream is likely to have become unavailable",
                                stream);

                        Set<StreamSubscription> affectedSubscriptions;
                        synchronized (SubscriptionState.this) {
                            Set<StreamSubscription> subscriptions = streamToStreamSubscriptions.remove(stream);
                            if (subscriptions != null && !subscriptions.isEmpty()) {
                                List<StreamSubscription> newSubscriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
                                for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                                    newSubscriptions.add(streamSubscriptions.get(i));
                                }
                                for (StreamSubscription subscription : subscriptions) {
                                    newSubscriptions.set(subscription.subscriptionIdInClient, null);
                                    globalStreamSubscriptionIds.remove(subscription.id);
                                }
                                this.streamSubscriptions = newSubscriptions;
                            }
                            affectedSubscriptions = subscriptions;
                        }
                        if (affectedSubscriptions != null && !affectedSubscriptions.isEmpty()) {
                            // scheduling consumer re-assignment, to give the system some time to recover
                            environment.scheduledExecutorService().schedule(() -> {
                                LOGGER.debug("Trying to move {} subscription(s) (stream {})", affectedSubscriptions.size(), stream);
                                assignConsumersToStream(
                                        affectedSubscriptions, stream,
                                        attempt -> attempt == 0 ? Duration.ZERO : metadataUpdateRetryDelay,
                                        metadataUpdateRetryTimeout
                                );
                            }, metadataUpdateInitialDelay.toMillis(), TimeUnit.MILLISECONDS);
                        }
                    }));
        }

        private void assignConsumersToStream(Collection<StreamSubscription> subscriptions, String stream,
                                             Function<Integer, Duration> delayPolicy, Duration retryTimeout) {
            Runnable consumersClosingCallback = () -> {
                for (StreamSubscription affectedSubscription : subscriptions) {
                    try {
                        affectedSubscription.consumer.closeAfterStreamDeletion();
                        streamSubscriptionRegistry.remove(affectedSubscription.id);
                    } catch (Exception e) {
                        LOGGER.debug("Error while closing consumer", e.getMessage());
                    }
                }
            };

            AsyncRetry.asyncRetry(() -> findBrokersForStream(stream))
                    .description("Candidate lookup to consume from " + stream)
                    .scheduler(environment.scheduledExecutorService())
                    .retry(ex -> !(ex instanceof StreamDoesNotExistException))
                    .delayPolicy(delayPolicy)
                    .timeout(retryTimeout)
                    .build()
                    .thenAccept(candidates -> {
                        if (candidates == null) {
                            consumersClosingCallback.run();
                        } else {
                            for (StreamSubscription affectedSubscription : subscriptions) {
                                try {
                                    Client.Broker broker = pickBroker(candidates);
                                    LOGGER.debug("Using {} to resume consuming from {}", broker, stream);
                                    String key = keyForClientSubscriptionState(broker);
                                    // FIXME in case the broker is no longer there, we may have to deal with an error here
                                    // we could renew the list of candidates for the stream
                                    ClientSubscriptionPool subscriptionPool = clientSubscriptionPools.computeIfAbsent(key, s -> new ClientSubscriptionPool(
                                            key,
                                            environment
                                                    .clientParametersCopy()
                                                    .host(broker.getHost())
                                                    .port(broker.getPort())
                                    ));
                                    if (affectedSubscription.consumer.isOpen()) {
                                        synchronized (affectedSubscription.consumer) {
                                            if (affectedSubscription.consumer.isOpen()) {
                                                subscriptionPool.add(affectedSubscription, OffsetSpecification.offset(affectedSubscription.offset));
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    LOGGER.warn("Error while re-assigning subscription from stream {}", stream, e.getMessage());
                                }
                            }
                        }
                    }).exceptionally(ex -> {
                consumersClosingCallback.run();
                return null;
            });
        }

        synchronized void add(StreamSubscription streamSubscription, OffsetSpecification offsetSpecification) {
            int subscriptionId = 0;
            for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                if (streamSubscriptions.get(i) == null) {
                    subscriptionId = i;
                    break;
                }
            }

            List<StreamSubscription> previousSubscriptions = this.streamSubscriptions;

            LOGGER.debug("Subscribing to {}", streamSubscription.stream);
            try {
                // updating data structures before subscribing
                // (to make sure they are up-to-date in case message would arrive super fast)
                streamSubscription.subscriptionIdInClient = subscriptionId;
                streamSubscription.subscriptionState = this;
                streamToStreamSubscriptions.computeIfAbsent(streamSubscription.stream, s -> ConcurrentHashMap.newKeySet())
                        .add(streamSubscription);
                globalStreamSubscriptionIds.add(streamSubscription.id);
                this.streamSubscriptions = update(previousSubscriptions, subscriptionId, streamSubscription);
                // FIXME consider using fewer initial credits
                Client.Response subscribeResponse = client.subscribe(subscriptionId, streamSubscription.stream, offsetSpecification, 10);
                if (!subscribeResponse.isOk()) {
                    String message = "Subscription to stream " + streamSubscription.stream + " failed with code " + subscribeResponse.getResponseCode();
                    LOGGER.debug(message);
                    throw new StreamException(message);
                }
            } catch (RuntimeException e) {
                streamSubscription.subscriptionIdInClient = -1;
                streamSubscription.subscriptionState = null;
                this.streamSubscriptions = previousSubscriptions;
                streamToStreamSubscriptions.computeIfAbsent(streamSubscription.stream, s -> ConcurrentHashMap.newKeySet())
                        .remove(streamSubscription);
                globalStreamSubscriptionIds.remove(streamSubscription.id);
                throw e;
            }

            LOGGER.debug("Subscribed to {}", streamSubscription.stream);
        }

        synchronized void remove(StreamSubscription streamSubscription) {
            int subscriptionIdInClient = streamSubscription.subscriptionIdInClient;
            Client.Response unsubscribeResponse = client.unsubscribe(subscriptionIdInClient);
            if (!unsubscribeResponse.isOk()) {
                LOGGER.warn("Unexpected response code when unsubscribing from {}: {} (subscription ID {})",
                        streamSubscription.stream, unsubscribeResponse.getResponseCode(), subscriptionIdInClient);
            }
            this.streamSubscriptions = update(this.streamSubscriptions, subscriptionIdInClient, null);
            globalStreamSubscriptionIds.remove(streamSubscription.id);
            streamToStreamSubscriptions.compute(streamSubscription.stream, (stream, subscriptionsForThisStream) -> {
                if (subscriptionsForThisStream == null || subscriptionsForThisStream.isEmpty()) {
                    // should not happen
                    return null;
                } else {
                    subscriptionsForThisStream.remove(streamSubscription);
                    return subscriptionsForThisStream.isEmpty() ? null : subscriptionsForThisStream;
                }
            });
            this.cleanCallback.accept(this);
        }

        private List<StreamSubscription> update(List<StreamSubscription> original, int index, StreamSubscription newValue) {
            List<StreamSubscription> newSubcriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
            for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                newSubcriptions.add(i == index ?
                        newValue : original.get(i)
                );
            }
            return newSubcriptions;
        }

        boolean isFull() {
            return this.globalStreamSubscriptionIds.size() == MAX_SUBSCRIPTIONS_PER_CLIENT;
        }

        boolean isEmpty() {
            return this.globalStreamSubscriptionIds.isEmpty();
        }

        void close() {
            this.client.close();
        }
    }
}

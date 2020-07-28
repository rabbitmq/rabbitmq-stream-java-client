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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

class StreamEnvironment implements Environment {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironment.class);

    private final Random random = new Random();

    private final EventLoopGroup eventLoopGroup;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean privateScheduleExecutorService;
    private final Client.ClientParameters clientParametersPrototype;
    private final List<Address> addresses;
    private final Map<String, Client> publishingClientPool = new ConcurrentHashMap<>();
    private final Map<String, Client> consumingClientPool = new ConcurrentHashMap<>();
    private final Map<String, ClientSubscriptionState> clientSubscriptionStates = new ConcurrentHashMap<>();
    private final List<Producer> producers = new CopyOnWriteArrayList<>();
    private final Codec codec;
    private final RecoveryBackOffDelayPolicy recoveryBackOffDelayPolicy;
    private volatile Client locator;

    StreamEnvironment(ScheduledExecutorService scheduledExecutorService, Client.ClientParameters clientParametersPrototype,
                      List<URI> uris, RecoveryBackOffDelayPolicy recoveryBackOffDelayPolicy) {

        this.recoveryBackOffDelayPolicy = recoveryBackOffDelayPolicy;
        clientParametersPrototype = maybeSetUpClientParametersFromUris(uris, clientParametersPrototype);

        if (uris.isEmpty()) {
            this.addresses = Collections.singletonList(
                    new Address(clientParametersPrototype.host, clientParametersPrototype.port)
            );
        } else {
            this.addresses = uris.stream()
                    .map(uriItem -> new Address(uriItem))
                    .collect(Collectors.toList());
        }

        if (clientParametersPrototype.eventLoopGroup == null) {
            this.eventLoopGroup = new NioEventLoopGroup();
            this.clientParametersPrototype = clientParametersPrototype
                    .duplicate().eventLoopGroup(this.eventLoopGroup);
        } else {
            this.eventLoopGroup = null;
            this.clientParametersPrototype = clientParametersPrototype
                    .duplicate().eventLoopGroup(clientParametersPrototype.eventLoopGroup);
        }
        if (scheduledExecutorService == null) {
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            this.privateScheduleExecutorService = true;
        } else {
            this.scheduledExecutorService = scheduledExecutorService;
            this.privateScheduleExecutorService = false;
        }


        // FIXME plug shutdown listener to reconnect in case of disconnection
        // use the addresses array to reconnect to another node
        AtomicReference<Client.ShutdownListener> shutdownListenerReference = new AtomicReference<>();
        Client.ShutdownListener shutdownListener = shutdownContext -> {
            if (shutdownContext.isShutdownUnexpected()) {
                this.scheduledExecutorService.execute(() -> {
                    try {
                        LOGGER.debug("Unexpected locator disconnection, trying to reconnect");
                        this.locator = null;
                        Client newLocator = null;
                        Client.ClientParameters newLocatorParameters = this.clientParametersPrototype.duplicate()
                                .shutdownListener(shutdownListenerReference.get());

                        int attempts = 0;
                        while (newLocator == null) {
                            Address address = addresses.size() == 1 ? addresses.get(0) :
                                    addresses.get(random.nextInt(addresses.size()));
                            LOGGER.debug("Trying to reconnect locator on {}", address);
                            try {
                                Thread.sleep(this.recoveryBackOffDelayPolicy.delay(attempts++).toMillis());
                                newLocator = new Client(newLocatorParameters
                                        .host(address.host)
                                        .port(address.port)
                                );
                                LOGGER.debug("Locator connected on {}", address);
                            } catch (Exception e) {
                                LOGGER.debug("Error while trying to connect locator on {}, retrying possibly somewhere else", address);
                            }
                        }
                        this.locator = newLocator;
                    } catch (Exception e) {
                        LOGGER.debug("Error while reconnecting locator", e);
                    }
                });
            }
        };
        shutdownListenerReference.set(shutdownListener);
        Client.ClientParameters locatorParameters = clientParametersPrototype
                .duplicate()
                .shutdownListener(shutdownListenerReference.get());
        this.locator = new Client(locatorParameters);
        this.codec = locator.codec();
    }

    private static String uriDecode(String s) {
        try {
            // URLDecode decodes '+' to a space, as for
            // form encoding.  So protect plus signs.
            return URLDecoder.decode(s.replace("+", "%2B"), "US-ASCII");
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    Client.ClientParameters maybeSetUpClientParametersFromUris(List<URI> uris, Client.ClientParameters clientParametersPrototype) {
        if (uris.isEmpty()) {
            return clientParametersPrototype;
        } else {
            URI uri = uris.get(0);
            clientParametersPrototype = clientParametersPrototype.duplicate();
            String host = uri.getHost();
            if (host != null) {
                clientParametersPrototype.host(host);
            }

            int port = uri.getPort();
            if (port != -1) {
                clientParametersPrototype.port(port);
            }

            String userInfo = uri.getRawUserInfo();
            if (userInfo != null) {
                String[] userPassword = userInfo.split(":");
                if (userPassword.length > 2) {
                    throw new IllegalArgumentException("Bad user info in URI " + userInfo);
                }

                clientParametersPrototype.username(uriDecode(userPassword[0]));
                if (userPassword.length == 2) {
                    clientParametersPrototype.password(uriDecode(userPassword[1]));
                }
            }

            String path = uri.getRawPath();
            if (path != null && path.length() > 0) {
                if (path.indexOf('/', 1) != -1) {
                    throw new IllegalArgumentException("Multiple segments in path of URI: " + path);
                }
                clientParametersPrototype.virtualHost(uriDecode(uri.getPath().substring(1)));
            }
            return clientParametersPrototype;
        }
    }

    @Override
    public StreamCreator streamCreator() {
        return new StreamStreamCreator(this);
    }

    @Override
    public void deleteStream(String stream) {
        Client.Response response = this.locator().delete(stream);
        if (!response.isOk()) {
            throw new StreamException("Error while deleting stream " + stream, response.getResponseCode());
        }
    }

    @Override
    public ProducerBuilder producerBuilder() {
        return new StreamProducerBuilder(this);
    }

    void addProducer(Producer producer) {
        this.producers.add(producer);
    }

    @Override
    public ConsumerBuilder consumerBuilder() {
        return new StreamConsumerBuilder(this);
    }

    @Override
    public void close() {
        if (privateScheduleExecutorService) {
            this.scheduledExecutorService.shutdownNow();
        }

        for (Producer producer : producers) {
            try {
                producer.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing producer, moving on to the next publisher", e);
            }
        }


        for (Client client : publishingClientPool.values()) {
            try {
                client.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing client, moving on to the next client", e);
            }
        }

        // FIXME close consumers

        try {
            if (this.locator != null) {
                this.locator.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error while closing locator client", e);
        }

        try {
            if (this.eventLoopGroup != null && (!this.eventLoopGroup.isShuttingDown() || !this.eventLoopGroup.isShutdown())) {
                LOGGER.debug("Closing Netty event loop group");
                this.eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.info("Event loop group closing has been interrupted");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.info("Event loop group closing failed", e);
        } catch (TimeoutException e) {
            LOGGER.info("Could not close event loop group in 10 seconds");
        }

    }

    protected ScheduledExecutorService getScheduledExecutorService() {
        return this.scheduledExecutorService;
    }

    Client getClientForPublisher(String stream) {
        Map<String, Client.StreamMetadata> metadata = locator().metadata(stream);
        if (metadata.size() == 0 || metadata.get(stream) == null) {
            throw new IllegalArgumentException("Stream does not exist: " + stream);
        }

        Client.StreamMetadata streamMetadata = metadata.get(stream);
        if (!streamMetadata.isResponseOk()) {
            throw new IllegalArgumentException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
        }

        Client.Broker leader = streamMetadata.getLeader();
        if (leader == null) {
            throw new IllegalStateException("Not leader available for stream " + stream);
        }
        LOGGER.info("Using client on {}:{} to publish to {}", leader.getHost(), leader.getPort(), stream);

        // FIXME make sure this is a reasonable key for brokers
        String key = leader.getHost() + ":" + leader.getPort();

        return publishingClientPool.computeIfAbsent(key, s -> {
            // FIXME add shutdown listener to client for publisher
            // this should notify the affected publishers/consumers
            return new Client(
                    clientParametersPrototype.duplicate()
                            .host(leader.getHost())
                            .port(leader.getPort())
            );
        });
    }

    Runnable registerConsumer(String stream, OffsetSpecification offsetSpecification, MessageHandler messageHandler) {
        Map<String, Client.StreamMetadata> metadata = locator().metadata(stream);
        if (metadata.size() == 0 || metadata.get(stream) == null) {
            throw new IllegalArgumentException("Stream does not exist: " + stream);
        }

        Client.StreamMetadata streamMetadata = metadata.get(stream);
        if (!streamMetadata.isResponseOk()) {
            throw new IllegalArgumentException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
        }

        List<Client.Broker> replicas = streamMetadata.getReplicas();
        if ((replicas == null || replicas.isEmpty()) && streamMetadata.getLeader() == null) {
            throw new IllegalStateException("Not node available to consume from stream " + stream);
        }

        Client.Broker broker;
        if (replicas == null || replicas.isEmpty()) {
            broker = streamMetadata.getLeader();
            LOGGER.debug("Consuming from {} on leader node {}", stream, broker);
        } else if (replicas.size() == 1) {
            broker = replicas.get(0);
        } else {
            LOGGER.debug("Replicas for consuming from {}: {}", stream, replicas);
            broker = replicas.get(random.nextInt(replicas.size()));
        }

        LOGGER.debug("Consuming from {} on node {}", stream, broker);

        // FIXME make sure this is a reasonable key for brokers
        String key = broker.getHost() + ":" + broker.getPort();

        synchronized (this) {
            // FIXME both client and subscription state could be in the same class
            ClientSubscriptionState clientSubscriptionState = clientSubscriptionStates.computeIfAbsent(key, s -> new ClientSubscriptionState());


            Client consumingClient = consumingClientPool.computeIfAbsent(key, s -> {
                // FIXME add shutdown listener to client for consumers
                // this should notify the affected publishers/consumers
                return new Client(
                        clientParametersPrototype.duplicate()
                                .host(broker.getHost())
                                .port(broker.getPort())
                                .chunkListener((client, subscriptionId, offset, messageCount, dataSize) -> client.credit(subscriptionId, 1))
                                .messageListener(clientSubscriptionState.getMessageListener())
                );
            });

            clientSubscriptionState.setClient(consumingClient);

            Runnable closingConsumerCallback = clientSubscriptionState.addHandler(stream, offsetSpecification, messageHandler);
            return closingConsumerCallback;
        }
    }

    Client locator() {
        if (this.locator == null) {
            throw new StreamException("No connection available");
        }
        return this.locator;
    }

    Codec codec() {
        return this.codec;
    }

    private static final class ClientSubscriptionState {

        private final AtomicInteger subscriptionSequence = new AtomicInteger();
        private final Map<Integer, MessageHandler> handlers = new ConcurrentHashMap<>();
        private final Client.MessageListener messageListener = (subscriptionId, offset, message) -> handlers.get(subscriptionId).handle(offset, message);
        private final AtomicReference<Client> client = new AtomicReference<>();

        Runnable addHandler(String stream, OffsetSpecification offsetSpecification, MessageHandler messageHandler) {
            int subscriptionId = subscriptionSequence.getAndIncrement();
            handlers.put(subscriptionId, messageHandler);
            Client.Response subscribeResponse = client.get().subscribe(subscriptionId, stream, offsetSpecification, 10);
            if (!subscribeResponse.isOk()) {
                throw new StreamException("Subscription to stream " + stream + " failed with code " + subscribeResponse.getResponseCode());
            }
            // FIXME if no more handlers in the map, consider closing the client
            return () -> {
                Client.Response unsubscribeResponse = client.get().unsubscribe(subscriptionId);
                handlers.remove(subscriptionId);
                if (!unsubscribeResponse.isOk()) {
                    throw new StreamException("Unsubscription to stream " + stream + " failed with code " + unsubscribeResponse.getResponseCode());
                }
            };
        }

        Client.MessageListener getMessageListener() {
            return messageListener;
        }

        void setClient(Client client) {
            this.client.set(client);
        }
    }

    private static final class Address {

        private final String host;
        private final int port;

        private Address(URI uri) {
            this(
                    uri.getHost() == null ? "localhost" : uri.getHost(),
                    uri.getPort() == -1 ? Client.DEFAULT_PORT : uri.getPort()
            );
        }

        private Address(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "Address{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }
}

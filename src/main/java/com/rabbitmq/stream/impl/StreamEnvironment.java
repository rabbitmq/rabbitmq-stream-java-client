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

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

class StreamEnvironment implements Environment {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironment.class);

    private final EventLoopGroup eventLoopGroup;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean privateScheduleExecutorService;
    private final Client.ClientParameters clientParametersPrototype;
    private final List<Address> addresses;
    private final Client locator;
    private final Map<String, Client> clientPool = new ConcurrentHashMap<>();
    private final List<Producer> producers = new CopyOnWriteArrayList<>();

    StreamEnvironment(ScheduledExecutorService scheduledExecutorService, Client.ClientParameters clientParametersPrototype,
                      List<URI> uris) {

        clientParametersPrototype = maybeSetUpClientParametersFromUris(uris, clientParametersPrototype);

        this.addresses = uris.stream().map(uriItem -> new Address(
                uriItem.getHost() == null ? "localhost" : uriItem.getHost(),
                uriItem.getPort() == -1 ? Client.DEFAULT_PORT : uriItem.getPort())
        ).collect(Collectors.toList());

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
        this.locator = new Client(clientParametersPrototype.duplicate());
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
    public ProducerBuilder producerBuilder() {
        return new StreamProducerBuilder(this);
    }

    void addProducer(Producer producer) {
        this.producers.add(producer);
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


        for (Client client : clientPool.values()) {
            try {
                client.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing client, moving on to the next client", e);
            }
        }

        try {
            this.locator.close();
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

    public Client getClientForPublisher(String stream) {
        Map<String, Client.StreamMetadata> metadata = this.locator.metadata(stream);
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

        return clientPool.computeIfAbsent(key, s -> {
            // FIXME add shutdown listener to client for publisher
            // this should notify the affected publishers/consumers
            return new Client(
                    clientParametersPrototype.duplicate()
                            .host(leader.getHost())
                            .port(leader.getPort())
            );
        });
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

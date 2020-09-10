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

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducersCoordinator {

  static final int MAX_PRODUCERS_PER_CLIENT = 256;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);
  private final StreamEnvironment environment;
  private final AtomicLong globalPublisherIdSequence = new AtomicLong(0);
  private final Map<Long, ProducerTracker> producerRegistry = new ConcurrentHashMap<>();
  private final Function<Client.ClientParameters, Client> clientFactory;
  private final Map<String, ManagerPool> pools = new ConcurrentHashMap<>();

  ProducersCoordinator(StreamEnvironment environment) {
    this(environment, cp -> new Client(cp));
  }

  ProducersCoordinator(
      StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.environment.clientParametersCopy();
  }

  private static String keyForManagerPool(Client.Broker broker) {
    // FIXME make sure this is a reasonable key for brokers
    return broker.getHost() + ":" + broker.getPort();
  }

  Runnable registerProducer(StreamProducer producer, String stream) {
    Client.Broker brokerForProducer = getBrokerForProducer(stream);

    String key = keyForManagerPool(brokerForProducer);
    ManagerPool pool =
        pools.computeIfAbsent(
            key,
            s ->
                new ManagerPool(
                    key,
                    environment
                        .clientParametersCopy()
                        .host(brokerForProducer.getHost())
                        .port(brokerForProducer.getPort())));

    long globalProducerId = globalPublisherIdSequence.getAndIncrement();
    ProducerTracker producerTracker = new ProducerTracker(globalProducerId, stream, producer);
    pool.add(producerTracker);

    // FIXME do we actually need the global producer ID (it's used for cleaning)?
    producerRegistry.put(globalProducerId, producerTracker);

    return () -> {
      producerTracker.clientProducersManager.unregister(producerTracker);
      producerRegistry.remove(globalProducerId);
    };
  }

  private Client.Broker getBrokerForProducer(String stream) {
    Map<String, Client.StreamMetadata> metadata = this.environment.locator().metadata(stream);
    if (metadata.size() == 0 || metadata.get(stream) == null) {
      throw new StreamDoesNotExistException(stream);
    }

    Client.StreamMetadata streamMetadata = metadata.get(stream);
    if (!streamMetadata.isResponseOk()) {
      if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
        throw new StreamDoesNotExistException(stream);
      } else {
        throw new IllegalStateException(
            "Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
      }
    }

    Client.Broker leader = streamMetadata.getLeader();
    if (leader == null) {
      throw new IllegalStateException("Not leader available for stream " + stream);
    }
    LOGGER.debug(
        "Using client on {}:{} to publish to {}", leader.getHost(), leader.getPort(), stream);

    return leader;
  }

  void close() {
    for (ManagerPool pool : pools.values()) {
      pool.close();
    }
    pools.clear();
  }

  int poolSize() {
    return pools.size();
  }

  int clientCount() {
    return pools.values().stream()
        .map(pool -> pool.managers.size())
        .reduce(0, (acc, size) -> acc + size);
  }

  private static class ProducerTracker {

    private final long id;
    private final String stream;
    private final StreamProducer producer;
    private volatile byte publisherId;
    private volatile ClientProducersManager clientProducersManager;

    private ProducerTracker(long id, String stream, StreamProducer producer) {
      this.id = id;
      this.stream = stream;
      this.producer = producer;
    }
  }

  private class ManagerPool {

    private final List<ClientProducersManager> managers = new CopyOnWriteArrayList<>();
    private final String name;
    private final Client.ClientParameters clientParameters;

    private ManagerPool(String name, Client.ClientParameters clientParameters) {
      this.name = name;
      this.clientParameters = clientParameters;
      this.managers.add(new ClientProducersManager(this, clientFactory, clientParameters));
    }

    private synchronized void add(ProducerTracker producerTracker) {
      boolean added = false;
      // FIXME deal with state unavailability (state may be closing because of connection closing)
      // try all of them until it succeeds, throw exception if failure
      for (ClientProducersManager manager : this.managers) {
        if (!manager.isFull()) {
          manager.register(producerTracker);
          added = true;
          break;
        }
      }
      if (!added) {
        LOGGER.debug(
            "Creating producers tracker on {}, this is subscription state #{}",
            name,
            managers.size() + 1);
        ClientProducersManager manager =
            new ClientProducersManager(this, clientFactory, clientParameters);
        this.managers.add(manager);
        manager.register(producerTracker);
      }
    }

    synchronized void maybeDisposeManager(ClientProducersManager manager) {
      if (manager.isEmpty()) {
        manager.close();
        this.remove(manager);
      }
    }

    private synchronized void remove(ClientProducersManager manager) {
      this.managers.remove(manager);
      if (this.managers.isEmpty()) {
        pools.remove(this.name);
      }
    }

    synchronized void close() {
      for (ClientProducersManager manager : managers) {
        manager.close();
      }
      managers.clear();
    }
  }

  private class ClientProducersManager {

    private final ConcurrentMap<Byte, ProducerTracker> producers =
        new ConcurrentHashMap<>(MAX_PRODUCERS_PER_CLIENT);
    private final Map<String, Set<ProducerTracker>> streamToProducerTrackers =
        new ConcurrentHashMap<>();
    private final Client client;
    private final ManagerPool owner;

    private ClientProducersManager(
        ManagerPool owner,
        Function<Client.ClientParameters, Client> cf,
        Client.ClientParameters clientParameters) {
      this.owner = owner;
      AtomicReference<Client> ref = new AtomicReference<>();
      this.client =
          cf.apply(
              clientParameters
                  .publishConfirmListener(
                      (publisherId, publishingId) -> {
                        ProducerTracker producerTracker = producers.get(publisherId);
                        if (producerTracker == null) {
                          LOGGER.warn("Received publish confirm for unknown producer: {}");
                        } else {
                          producerTracker.producer.confirm(publishingId);
                        }
                      })
                  .publishErrorListener(
                      (publisherId, publishingId, errorCode) -> {
                        ProducerTracker producerTracker = producers.get(publisherId);
                        if (producerTracker == null) {
                          LOGGER.warn("Received publish error for unknown producer: {}");
                        } else {
                          producerTracker.producer.error(publishingId, errorCode);
                        }
                      })
                  .shutdownListener(
                      shutdownContext -> {
                        owner.remove(this);
                        if (shutdownContext.isShutdownUnexpected()) {
                          LOGGER.debug(
                              "Recovering {} producers after unexpected connection termination",
                              producers.size());
                          producers.forEach(
                              (publishingId, tracker) -> tracker.producer.unavailable());
                          // execute in thread pool to free the IO thread
                          environment
                              .scheduledExecutorService()
                              .execute(
                                  () -> {
                                    streamToProducerTrackers
                                        .entrySet()
                                        .forEach(
                                            entry -> {
                                              String stream = entry.getKey();
                                              Set<ProducerTracker> trackers = entry.getValue();
                                              assignProducersToNewManagers(
                                                  trackers,
                                                  stream,
                                                  environment.recoveryBackOffDelayPolicy());
                                            });
                                  });
                        }
                      })
                  .metadataListener(
                      (stream, code) -> {
                        synchronized (ClientProducersManager.this) {
                          Set<ProducerTracker> affectedProducerTrackers =
                              streamToProducerTrackers.remove(stream);
                          if (affectedProducerTrackers != null
                              && !affectedProducerTrackers.isEmpty()) {
                            affectedProducerTrackers.forEach(
                                tracker -> {
                                  tracker.producer.unavailable();
                                  producers.remove(tracker.publisherId);
                                });
                            environment
                                .scheduledExecutorService()
                                .execute(
                                    () -> {
                                      // close manager if no more trackers for it
                                      // needs to be done in another thread than the IO thread
                                      this.owner.maybeDisposeManager(this);
                                      assignProducersToNewManagers(
                                          affectedProducerTrackers,
                                          stream,
                                          environment.topologyUpdateBackOffDelayPolicy());
                                    });
                          }
                        }
                      })
                  .clientProperty("name", "rabbitmq-stream-producer"));
      ref.set(this.client);
    }

    private void assignProducersToNewManagers(
        Collection<ProducerTracker> trackers, String stream, BackOffDelayPolicy delayPolicy) {
      AsyncRetry.asyncRetry(() -> getBrokerForProducer(stream))
          .description("Candidate lookup to publish to " + stream)
          .scheduler(environment.scheduledExecutorService())
          .retry(ex -> !(ex instanceof StreamDoesNotExistException))
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(
              broker -> {
                String key = keyForManagerPool(broker);
                LOGGER.debug("Assigning {} producer(s) to {}", trackers.size(), key);
                ManagerPool pool =
                    pools.computeIfAbsent(
                        key,
                        s ->
                            new ManagerPool(
                                key,
                                environment
                                    .clientParametersCopy()
                                    .host(broker.getHost())
                                    .port(broker.getPort())));
                trackers.forEach(
                    tracker -> {
                      pool.add(tracker);
                      tracker.producer.running();
                    });
              })
          .exceptionally(
              ex -> {
                for (ProducerTracker tracker : trackers) {
                  try {
                    tracker.producer.closeAfterStreamDeletion();
                    producerRegistry.remove(tracker.id);
                  } catch (Exception e) {
                    LOGGER.debug("Error while closing producer", e.getMessage());
                  }
                }
                return null;
              });
    }

    private synchronized void register(ProducerTracker producerTracker) {
      // using the next available slot
      for (int i = 0; i < MAX_PRODUCERS_PER_CLIENT; i++) {
        ProducerTracker previousValue = producers.putIfAbsent((byte) i, producerTracker);
        if (previousValue == null) {
          producerTracker.publisherId = (byte) i;
          break;
        }
      }
      producerTracker.producer.setPublisherId(producerTracker.publisherId);
      producerTracker.producer.setClient(this.client);
      producerTracker.clientProducersManager = this;
      producers.put(producerTracker.publisherId, producerTracker);
      streamToProducerTrackers
          .computeIfAbsent(producerTracker.stream, s -> ConcurrentHashMap.newKeySet())
          .add(producerTracker);
    }

    private synchronized void unregister(ProducerTracker producerTracker) {
      producers.remove(producerTracker.publisherId);
      streamToProducerTrackers.compute(
          producerTracker.stream,
          (s, producerTrackersForThisStream) -> {
            if (s == null || producerTrackersForThisStream == null) {
              // should not happen
              return null;
            } else {
              producerTrackersForThisStream.remove(producerTracker);
              return producerTrackersForThisStream.isEmpty() ? null : producerTrackersForThisStream;
            }
          });
      this.owner.maybeDisposeManager(this);
    }

    synchronized boolean isFull() {
      return producers.size() == MAX_PRODUCERS_PER_CLIENT;
    }

    synchronized boolean isEmpty() {
      return producers.isEmpty();
    }

    private void close() {
      try {
        this.client.close();
      } catch (Exception e) {

      }
    }
  }
}

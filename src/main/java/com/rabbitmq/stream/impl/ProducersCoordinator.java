// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.Utils.formatConstant;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducersCoordinator {

  static final int MAX_PRODUCERS_PER_CLIENT = 256;
  static final int MAX_COMMITTING_CONSUMERS_PER_CLIENT = 50;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);
  private final StreamEnvironment environment;
  private final Function<Client.ClientParameters, Client> clientFactory;
  private final Map<String, ManagerPool> pools = new ConcurrentHashMap<>();
  private final int maxProducersByClient, maxCommittingConsumersByClient;

  ProducersCoordinator(
      StreamEnvironment environment, int maxProducersByClient, int maxCommittingConsumersByClient) {
    this(environment, maxProducersByClient, maxCommittingConsumersByClient, Client::new);
  }

  ProducersCoordinator(
      StreamEnvironment environment,
      int maxProducersByClient,
      int maxCommittingConsumersByClient,
      Function<Client.ClientParameters, Client> clientFactory) {
    this.environment = environment;
    this.clientFactory =
        clientParameters -> {
          ClientParameters parametersCopy = clientParameters.duplicate();
          parametersCopy.host(environment.hostResolver().apply(parametersCopy.host));
          return clientFactory.apply(parametersCopy);
        };
    this.maxProducersByClient = maxProducersByClient;
    this.maxCommittingConsumersByClient = maxCommittingConsumersByClient;
  }

  private static String keyForManagerPool(Client.Broker broker) {
    // FIXME make sure this is a reasonable key for brokers
    return broker.getHost() + ":" + broker.getPort();
  }

  Runnable registerProducer(StreamProducer producer, String reference, String stream) {
    return registerAgentTracker(new ProducerTracker(reference, stream, producer), stream);
  }

  Runnable registerCommittingConsumer(StreamConsumer consumer) {
    return registerAgentTracker(
        new CommittingConsumerTracker(consumer.stream(), consumer), consumer.stream());
  }

  private Runnable registerAgentTracker(AgentTracker tracker, String stream) {
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

    pool.add(tracker);

    return tracker::cancel;
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
    return pools.values().stream().map(pool -> pool.managers.size()).reduce(0, Integer::sum);
  }

  private interface AgentTracker {

    void assign(byte producerId, Client client, ClientProducersManager manager);

    boolean identifiable();

    byte id();

    void unavailable();

    void running();

    void cancel();

    void closeAfterStreamDeletion();

    String stream();

    String reference();
  }

  private static class ProducerTracker implements AgentTracker {

    private final String reference;
    private final String stream;
    private final StreamProducer producer;
    private volatile byte publisherId;
    private volatile ClientProducersManager clientProducersManager;

    private ProducerTracker(String reference, String stream, StreamProducer producer) {
      this.reference = reference;
      this.stream = stream;
      this.producer = producer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      synchronized (ProducerTracker.this) {
        this.publisherId = producerId;
        this.clientProducersManager = manager;
      }
      this.producer.setPublisherId(producerId);
      this.producer.setClient(client);
    }

    @Override
    public boolean identifiable() {
      return true;
    }

    @Override
    public byte id() {
      return this.publisherId;
    }

    @Override
    public String reference() {
      return this.reference;
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void unavailable() {
      synchronized (ProducerTracker.this) {
        this.clientProducersManager = null;
      }
      this.producer.unavailable();
    }

    @Override
    public void running() {
      this.producer.running();
    }

    @Override
    public synchronized void cancel() {
      if (this.clientProducersManager != null) {
        this.clientProducersManager.unregister(this);
      }
    }

    @Override
    public void closeAfterStreamDeletion() {
      this.producer.closeAfterStreamDeletion();
    }
  }

  private static class CommittingConsumerTracker implements AgentTracker {

    private final String stream;
    private final StreamConsumer consumer;
    private volatile ClientProducersManager clientProducersManager;

    private CommittingConsumerTracker(String stream, StreamConsumer consumer) {
      this.stream = stream;
      this.consumer = consumer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      synchronized (CommittingConsumerTracker.this) {
        this.clientProducersManager = manager;
      }
      this.consumer.setClient(client);
    }

    @Override
    public boolean identifiable() {
      return false;
    }

    @Override
    public byte id() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String reference() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void unavailable() {
      synchronized (CommittingConsumerTracker.this) {
        this.clientProducersManager = null;
      }
      this.consumer.unavailable();
    }

    @Override
    public void running() {
      this.consumer.running();
    }

    @Override
    public synchronized void cancel() {
      if (this.clientProducersManager != null) {
        this.clientProducersManager.unregister(this);
      }
    }

    @Override
    public void closeAfterStreamDeletion() {
      // nothing to do here, the consumer will be closed by the consumers coordinator if
      // the stream has been deleted
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

    private synchronized void add(AgentTracker producerTracker) {
      boolean added = false;
      // FIXME deal with state unavailability (state may be closing because of connection closing)
      // try all of them until it succeeds, throw exception if failure
      for (ClientProducersManager manager : this.managers) {
        if (!manager.isFullFor(producerTracker)) {
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
        new ConcurrentHashMap<>(maxProducersByClient);
    private final Set<AgentTracker> committingConsumerTrackers =
        ConcurrentHashMap.newKeySet(maxCommittingConsumersByClient);
    private final Map<String, Set<AgentTracker>> streamToTrackers = new ConcurrentHashMap<>();
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
                          LOGGER.warn(
                              "Received publish confirm for unknown producer: {}", publisherId);
                        } else {
                          producerTracker.producer.confirm(publishingId);
                        }
                      })
                  .publishErrorListener(
                      (publisherId, publishingId, errorCode) -> {
                        ProducerTracker producerTracker = producers.get(publisherId);
                        if (producerTracker == null) {
                          LOGGER.warn(
                              "Received publish error for unknown producer: {}", publisherId);
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
                          producers.forEach((publishingId, tracker) -> tracker.unavailable());
                          committingConsumerTrackers.forEach(AgentTracker::unavailable);
                          // execute in thread pool to free the IO thread
                          environment
                              .scheduledExecutorService()
                              .execute(
                                  () ->
                                      streamToTrackers.forEach(
                                          (stream, trackers) ->
                                              assignProducersToNewManagers(
                                                  trackers,
                                                  stream,
                                                  environment.recoveryBackOffDelayPolicy())));
                        }
                      })
                  .metadataListener(
                      (stream, code) -> {
                        synchronized (ClientProducersManager.this) {
                          Set<AgentTracker> affectedTrackers = streamToTrackers.remove(stream);
                          if (affectedTrackers != null && !affectedTrackers.isEmpty()) {
                            affectedTrackers.forEach(
                                tracker -> {
                                  tracker.unavailable();
                                  if (tracker.identifiable()) {
                                    producers.remove(tracker.id());
                                  } else {
                                    committingConsumerTrackers.remove(tracker);
                                  }
                                });
                            environment
                                .scheduledExecutorService()
                                .execute(
                                    () -> {
                                      // close manager if no more trackers for it
                                      // needs to be done in another thread than the IO thread
                                      this.owner.maybeDisposeManager(this);
                                      assignProducersToNewManagers(
                                          affectedTrackers,
                                          stream,
                                          environment.topologyUpdateBackOffDelayPolicy());
                                    });
                          }
                        }
                      })
                  .clientProperty("connection_name", "rabbitmq-stream-producer"));
      ref.set(this.client);
    }

    private void assignProducersToNewManagers(
        Collection<AgentTracker> trackers, String stream, BackOffDelayPolicy delayPolicy) {
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
                      try {
                        pool.add(tracker);
                        tracker.running();
                      } catch (Exception e) {
                        LOGGER.info(
                            "Error while re-assigning producer {} to {}: {}. Moving on.",
                            tracker.identifiable() ? tracker.id() : "(committing consumer)",
                            pool.name,
                            e.getMessage());
                      }
                    });
              })
          .exceptionally(
              ex -> {
                LOGGER.info("Error while re-assigning producers: {}", ex.getMessage());
                for (AgentTracker tracker : trackers) {
                  // FIXME what to do with committing consumers after a timeout?
                  // here they are left as "unavailable" and not, meaning they will not be
                  // able to commit. Yet recovery mechanism could try to reconnect them, but
                  // that seems far-fetched (the first recovery already failed). They could
                  // be put in a state whereby they refuse all new commit commands and inform
                  // with an exception they should be restarted.
                  try {
                    tracker.closeAfterStreamDeletion();
                  } catch (Exception e) {
                    LOGGER.debug("Error while closing producer: {}", e.getMessage());
                  }
                }
                return null;
              });
    }

    private synchronized void register(AgentTracker tracker) {
      if (tracker.identifiable()) {
        ProducerTracker producerTracker = (ProducerTracker) tracker;
        // using the next available slot
        for (int i = 0; i < maxProducersByClient; i++) {
          ProducerTracker previousValue = producers.putIfAbsent((byte) i, producerTracker);
          if (previousValue == null) {
            Response response =
                this.client.declarePublisher((byte) i, tracker.reference(), tracker.stream());
            if (response.isOk()) {
              tracker.assign((byte) i, this.client, this);
            } else {
              LOGGER.info(
                  "Error while declaring publisher: {}. Could not assign producer to client.",
                  formatConstant(response.getResponseCode()));
            }
            break;
          }
        }
        producers.put(tracker.id(), producerTracker);
      } else {
        tracker.assign((byte) 0, this.client, this);
        committingConsumerTrackers.add(tracker);
      }

      streamToTrackers
          .computeIfAbsent(tracker.stream(), s -> ConcurrentHashMap.newKeySet())
          .add(tracker);
    }

    private synchronized void unregister(AgentTracker tracker) {
      if (tracker.identifiable()) {
        producers.remove(tracker.id());
      } else {
        committingConsumerTrackers.remove(tracker);
      }
      streamToTrackers.compute(
          tracker.stream(),
          (s, trackersForThisStream) -> {
            if (s == null || trackersForThisStream == null) {
              // should not happen
              return null;
            } else {
              trackersForThisStream.remove(tracker);
              return trackersForThisStream.isEmpty() ? null : trackersForThisStream;
            }
          });
      this.owner.maybeDisposeManager(this);
    }

    synchronized boolean isFullFor(AgentTracker tracker) {
      if (tracker.identifiable()) {
        return producers.size() == maxProducersByClient;
      } else {
        return committingConsumerTrackers.size() == maxCommittingConsumersByClient;
      }
    }

    synchronized boolean isEmpty() {
      return producers.isEmpty() && committingConsumerTrackers.isEmpty();
    }

    private void close() {
      try {
        this.client.close();
      } catch (Exception e) {
        // ok
      }
    }
  }

  @Override
  public String toString() {
    return ("[ \n"
            + pools.entrySet().stream()
                .map(
                    poolEntry ->
                        "  { 'broker' : '"
                            + poolEntry.getKey()
                            + "', 'clients' : [ "
                            + poolEntry.getValue().managers.stream()
                                .map(
                                    manager ->
                                        "{ 'producer_count' : "
                                            + manager.producers.size()
                                            + ", "
                                            + "  'committing_consumer_count' : "
                                            + manager.committingConsumerTrackers.size()
                                            + " }")
                                .collect(Collectors.joining(", "))
                            + " ] }")
                .collect(Collectors.joining(", \n"))
            + "\n]")
        .replace("'", "\"");
  }
}

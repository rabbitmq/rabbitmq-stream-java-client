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

import static com.rabbitmq.stream.impl.Utils.convertCodeToException;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.namedRunnable;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AddressResolver;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamStats;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.Client.StreamStatsResponse;
import com.rabbitmq.stream.impl.OffsetTrackingCoordinator.Registration;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironmentBuilder.DefaultTlsConfiguration;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamEnvironment implements Environment {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironment.class);

  private final EventLoopGroup eventLoopGroup;
  private final ScheduledExecutorService scheduledExecutorService;
  private final boolean privateScheduleExecutorService;
  private final Client.ClientParameters clientParametersPrototype;
  private final List<Address> addresses;
  private final List<StreamProducer> producers = new CopyOnWriteArrayList<>();
  private final List<StreamConsumer> consumers = new CopyOnWriteArrayList<>();
  private final Codec codec;
  private final BackOffDelayPolicy recoveryBackOffDelayPolicy;
  private final BackOffDelayPolicy topologyUpdateBackOffDelayPolicy;
  private final ConsumersCoordinator consumersCoordinator;
  private final ProducersCoordinator producersCoordinator;
  private final OffsetTrackingCoordinator offsetTrackingCoordinator;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AddressResolver addressResolver;
  private final Clock clock = new Clock();
  private final ScheduledFuture<?> clockRefreshFuture;
  private final ByteBufAllocator byteBufAllocator;
  private final AtomicBoolean locatorsInitialized = new AtomicBoolean(false);
  private final Runnable locatorInitializationSequence;
  private final List<Locator> locators = new CopyOnWriteArrayList<>();

  StreamEnvironment(
      ScheduledExecutorService scheduledExecutorService,
      Client.ClientParameters clientParametersPrototype,
      List<URI> uris,
      BackOffDelayPolicy recoveryBackOffDelayPolicy,
      BackOffDelayPolicy topologyBackOffDelayPolicy,
      AddressResolver addressResolver,
      int maxProducersByConnection,
      int maxTrackingConsumersByConnection,
      int maxConsumersByConnection,
      DefaultTlsConfiguration tlsConfiguration,
      ByteBufAllocator byteBufAllocator,
      boolean lazyInit,
      Function<ClientConnectionType, String> connectionNamingStrategy) {
    this(
        scheduledExecutorService,
        clientParametersPrototype,
        uris,
        recoveryBackOffDelayPolicy,
        topologyBackOffDelayPolicy,
        addressResolver,
        maxProducersByConnection,
        maxTrackingConsumersByConnection,
        maxConsumersByConnection,
        tlsConfiguration,
        byteBufAllocator,
        lazyInit,
        connectionNamingStrategy,
        cp -> new Client(cp));
  }

  StreamEnvironment(
      ScheduledExecutorService scheduledExecutorService,
      Client.ClientParameters clientParametersPrototype,
      List<URI> uris,
      BackOffDelayPolicy recoveryBackOffDelayPolicy,
      BackOffDelayPolicy topologyBackOffDelayPolicy,
      AddressResolver addressResolver,
      int maxProducersByConnection,
      int maxTrackingConsumersByConnection,
      int maxConsumersByConnection,
      DefaultTlsConfiguration tlsConfiguration,
      ByteBufAllocator byteBufAllocator,
      boolean lazyInit,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory) {
    this.recoveryBackOffDelayPolicy = recoveryBackOffDelayPolicy;
    this.topologyUpdateBackOffDelayPolicy = topologyBackOffDelayPolicy;
    this.byteBufAllocator = byteBufAllocator;
    clientParametersPrototype.byteBufAllocator(byteBufAllocator);
    clientParametersPrototype = maybeSetUpClientParametersFromUris(uris, clientParametersPrototype);

    this.addressResolver = addressResolver;

    boolean tls;
    if (tlsConfiguration != null && tlsConfiguration.enabled()) {
      tls = true;
      try {
        SslContext sslContext =
            tlsConfiguration.sslContext() == null
                ? SslContextBuilder.forClient().build()
                : tlsConfiguration.sslContext();

        clientParametersPrototype.sslContext(sslContext);
        clientParametersPrototype.tlsHostnameVerification(
            tlsConfiguration.hostnameVerificationEnabled());

      } catch (SSLException e) {
        throw new StreamException("Error while creating Netty SSL context", e);
      }
    } else {
      tls = false;
    }

    if (uris.isEmpty()) {
      this.addresses =
          Collections.singletonList(
              new Address(clientParametersPrototype.host(), clientParametersPrototype.port()));
    } else {
      int defaultPort = tls ? Client.DEFAULT_TLS_PORT : Client.DEFAULT_PORT;
      this.addresses =
          uris.stream()
              .map(
                  uriItem ->
                      new Address(
                          uriItem.getHost() == null ? "localhost" : uriItem.getHost(),
                          uriItem.getPort() == -1 ? defaultPort : uriItem.getPort()))
              .collect(Collectors.toList());
    }

    this.addresses.forEach(address -> this.locators.add(new Locator(address)));

    if (clientParametersPrototype.eventLoopGroup == null) {
      this.eventLoopGroup = new NioEventLoopGroup();
      this.clientParametersPrototype =
          clientParametersPrototype.duplicate().eventLoopGroup(this.eventLoopGroup);
    } else {
      this.eventLoopGroup = null;
      this.clientParametersPrototype =
          clientParametersPrototype
              .duplicate()
              .eventLoopGroup(clientParametersPrototype.eventLoopGroup);
    }
    ScheduledExecutorService executorService;
    if (scheduledExecutorService == null) {
      executorService =
          Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
      this.privateScheduleExecutorService = true;
    } else {
      executorService = scheduledExecutorService;
      this.privateScheduleExecutorService = false;
    }
    this.scheduledExecutorService = new ScheduledExecutorServiceWrapper(executorService);

    this.producersCoordinator =
        new ProducersCoordinator(
            this,
            maxProducersByConnection,
            maxTrackingConsumersByConnection,
            connectionNamingStrategy,
            Utils.coordinatorClientFactory(this));
    this.consumersCoordinator =
        new ConsumersCoordinator(
            this,
            maxConsumersByConnection,
            connectionNamingStrategy,
            Utils.coordinatorClientFactory(this));
    this.offsetTrackingCoordinator = new OffsetTrackingCoordinator(this);
    ClientParameters clientParametersForInit = clientParametersPrototype.duplicate();
    Runnable locatorInitSequence =
        () -> {
          RuntimeException lastException = null;
          for (int i = 0; i < addresses.size(); i++) {
            Address address = addresses.get(i);
            Locator locator = locator(i);
            address = addressResolver.resolve(address);
            String connectionName = connectionNamingStrategy.apply(ClientConnectionType.LOCATOR);
            Client.ClientParameters locatorParameters =
                clientParametersForInit
                    .duplicate()
                    .host(address.host())
                    .port(address.port())
                    .clientProperty("connection_name", connectionName)
                    .shutdownListener(
                        shutdownListener(locator, connectionNamingStrategy, clientFactory));
            try {
              Client client = clientFactory.apply(locatorParameters);
              locator.client(client);
              LOGGER.debug("Created locator connection '{}'", connectionName);
              LOGGER.debug("Locator connected to {}", address);
            } catch (RuntimeException e) {
              LOGGER.debug("Error while try to connect to {}: {}", address, e.getMessage());
              lastException = e;
            }
          }
          if (this.locators.stream().allMatch(l -> l.isNotSet())) {
            throw lastException;
          } else {
            this.locators.forEach(
                l -> {
                  if (l.isNotSet()) {
                    scheduleLocatorConnection(l, connectionNamingStrategy, clientFactory);
                  }
                });
          }
        };
    if (lazyInit) {
      this.locatorInitializationSequence = locatorInitSequence;
    } else {
      locatorInitSequence.run();
      locatorsInitialized.set(true);
      this.locatorInitializationSequence = () -> {};
    }
    this.codec =
        clientParametersPrototype.codec() == null
            ? Codecs.DEFAULT
            : clientParametersPrototype.codec();
    this.clockRefreshFuture =
        this.scheduledExecutorService.scheduleAtFixedRate(
            Utils.namedRunnable(() -> this.clock.refresh(), "Background clock refresh"),
            1,
            1,
            SECONDS);
  }

  private ShutdownListener shutdownListener(
      Locator locator,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory) {
    AtomicReference<Client.ShutdownListener> shutdownListenerReference = new AtomicReference<>();
    Client.ShutdownListener shutdownListener =
        shutdownContext -> {
          if (shutdownContext.isShutdownUnexpected()) {
            locator.client(null);
            LOGGER.debug("Unexpected locator disconnection, trying to reconnect");
            try {
              Client.ClientParameters newLocatorParameters =
                  this.clientParametersPrototype
                      .duplicate()
                      .shutdownListener(shutdownListenerReference.get());
              AsyncRetry.asyncRetry(
                      () -> {
                        LOGGER.debug("Locator reconnection...");
                        Address resolvedAddress = addressResolver.resolve(locator.address());
                        String connectionName =
                            connectionNamingStrategy.apply(ClientConnectionType.LOCATOR);
                        LOGGER.debug(
                            "Trying to reconnect locator on {}, with client connection name '{}'",
                            resolvedAddress,
                            connectionName);
                        Client newLocator =
                            clientFactory.apply(
                                newLocatorParameters
                                    .host(resolvedAddress.host())
                                    .port(resolvedAddress.port())
                                    .clientProperty("connection_name", connectionName));
                        LOGGER.debug("Created locator connection '{}'", connectionName);
                        LOGGER.debug("Locator connected on {}", resolvedAddress);
                        return newLocator;
                      })
                  .description("Locator recovery")
                  .scheduler(this.scheduledExecutorService)
                  .delayPolicy(recoveryBackOffDelayPolicy)
                  .build()
                  .thenAccept(newClient -> locator.client(newClient))
                  .exceptionally(
                      ex -> {
                        LOGGER.debug("Locator recovery failed", ex);
                        return null;
                      });
            } catch (Exception e) {
              LOGGER.debug("Error while scheduling locator reconnection", e);
            }
          }
        };
    shutdownListenerReference.set(shutdownListener);
    return shutdownListener;
  }

  private void scheduleLocatorConnection(
      Locator locator,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory) {
    ShutdownListener shutdownListener =
        shutdownListener(locator, connectionNamingStrategy, clientFactory);
    try {
      Client.ClientParameters newLocatorParameters =
          this.clientParametersPrototype.duplicate().shutdownListener(shutdownListener);
      AsyncRetry.asyncRetry(
              () -> {
                LOGGER.debug("Locator reconnection...");
                Address resolvedAddress = addressResolver.resolve(locator.address());
                String connectionName =
                    connectionNamingStrategy.apply(ClientConnectionType.LOCATOR);
                LOGGER.debug(
                    "Trying to reconnect locator on {}, with client connection name '{}'",
                    resolvedAddress,
                    connectionName);
                Client newLocator =
                    clientFactory.apply(
                        newLocatorParameters
                            .host(resolvedAddress.host())
                            .port(resolvedAddress.port())
                            .clientProperty("connection_name", connectionName));
                LOGGER.debug("Created locator connection '{}'", connectionName);
                LOGGER.debug("Locator connected on {}", resolvedAddress);
                return newLocator;
              })
          .description("Locator recovery")
          .scheduler(this.scheduledExecutorService)
          .delayPolicy(recoveryBackOffDelayPolicy)
          .build()
          .thenAccept(newClient -> locator.client(newClient))
          .exceptionally(
              ex -> {
                LOGGER.debug("Locator recovery failed", ex);
                return null;
              });
    } catch (Exception e) {
      LOGGER.debug("Error while scheduling locator reconnection", e);
    }
  }

  private Locator locator(int i) {
    return this.locators.get(i);
  }

  private static String uriDecode(String s) {
    try {
      // URLDecode decodes '+' to a space, as for
      // form encoding. So protect plus signs.
      return URLDecoder.decode(s.replace("+", "%2B"), "US-ASCII");
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  Client.ClientParameters maybeSetUpClientParametersFromUris(
      List<URI> uris, Client.ClientParameters clientParametersPrototype) {
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

  public ByteBufAllocator byteBufAllocator() {
    return byteBufAllocator;
  }

  void maybeInitializeLocator() {
    if (this.locatorsInitialized.compareAndSet(false, true)) {
      try {
        this.locatorInitializationSequence.run();
      } catch (RuntimeException e) {
        this.locatorsInitialized.set(false);
        throw e;
      }
    }
  }

  @Override
  public StreamCreator streamCreator() {
    checkNotClosed();
    return new StreamStreamCreator(this);
  }

  @Override
  public void deleteStream(String stream) {
    checkNotClosed();
    this.maybeInitializeLocator();
    Client.Response response = this.locator().delete(stream);
    if (!response.isOk()) {
      throw new StreamException(
          "Error while deleting stream "
              + stream
              + " ("
              + formatConstant(response.getResponseCode())
              + ")",
          response.getResponseCode());
    }
  }

  @Override
  public StreamStats queryStreamStats(String stream) {
    checkNotClosed();
    StreamStatsResponse response =
        locatorOperation(
            Utils.namedFunction(
                client -> {
                  if (Utils.is3_11_OrMore(client.brokerVersion())) {
                    return client.streamStats(stream);
                  } else {
                    throw new UnsupportedOperationException(
                        "QueryStringInfo is available only for RabbitMQ 3.11 or more.");
                  }
                },
                "Query stream stats on stream '%s'",
                stream));
    if (response.isOk()) {
      Map<String, Long> info = response.getInfo();
      BiFunction<String, String, LongSupplier> offsetSupplierLogic =
          (key, message) -> {
            if (!info.containsKey(key) || info.get(key) == -1) {
              return () -> {
                throw new NoOffsetException(message);
              };
            } else {
              try {
                long offset = info.get(key);
                return () -> offset;
              } catch (NumberFormatException e) {
                return () -> {
                  throw new NoOffsetException(message);
                };
              }
            }
          };
      LongSupplier firstOffsetSupplier =
          offsetSupplierLogic.apply("first_chunk_id", "No first offset for stream " + stream);
      LongSupplier committedOffsetSupplier =
          offsetSupplierLogic.apply(
              "committed_chunk_id", "No committed chunk ID for stream " + stream);
      return new DefaultStreamStats(firstOffsetSupplier, committedOffsetSupplier);
    } else {
      throw convertCodeToException(
          response.getResponseCode(),
          stream,
          () ->
              "Error while querying stream info: "
                  + formatConstant(response.getResponseCode())
                  + ".");
    }
  }

  private static class DefaultStreamStats implements StreamStats {

    private final LongSupplier firstOffsetSupplier, committedOffsetSupplier;

    private DefaultStreamStats(
        LongSupplier firstOffsetSupplier, LongSupplier committedOffsetSupplier) {
      this.firstOffsetSupplier = firstOffsetSupplier;
      this.committedOffsetSupplier = committedOffsetSupplier;
    }

    @Override
    public long firstOffset() {
      return firstOffsetSupplier.getAsLong();
    }

    @Override
    public long committedChunkId() {
      return committedOffsetSupplier.getAsLong();
    }
  }

  @Override
  public ProducerBuilder producerBuilder() {
    checkNotClosed();
    return new StreamProducerBuilder(this);
  }

  void addProducer(StreamProducer producer) {
    this.producers.add(producer);
  }

  void removeProducer(StreamProducer producer) {
    this.producers.remove(producer);
  }

  void addConsumer(StreamConsumer consumer) {
    this.consumers.add(consumer);
  }

  void removeConsumer(StreamConsumer consumer) {
    this.consumers.remove(consumer);
  }

  @Override
  public ConsumerBuilder consumerBuilder() {
    checkNotClosed();
    return new StreamConsumerBuilder(this);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      for (StreamProducer producer : producers) {
        try {
          producer.closeFromEnvironment();
        } catch (Exception e) {
          LOGGER.warn("Error while closing producer, moving on to the next one", e);
        }
      }

      for (StreamConsumer consumer : consumers) {
        try {
          consumer.closeFromEnvironment();
        } catch (Exception e) {
          LOGGER.warn("Error while closing consumer, moving on to the next one", e);
        }
      }

      this.producersCoordinator.close();
      this.consumersCoordinator.close();
      this.offsetTrackingCoordinator.close();

      for (Locator locator : this.locators) {
        try {
          if (locator.isSet()) {
            locator.client().close();
            locator.client(null);
          }
        } catch (Exception e) {
          LOGGER.warn("Error while closing locator client", e);
        }
      }

      this.clockRefreshFuture.cancel(false);
      if (privateScheduleExecutorService) {
        this.scheduledExecutorService.shutdownNow();
      }
      try {
        if (this.eventLoopGroup != null
            && (!this.eventLoopGroup.isShuttingDown() || !this.eventLoopGroup.isShutdown())) {
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
  }

  ScheduledExecutorService scheduledExecutorService() {
    return this.scheduledExecutorService;
  }

  void execute(Runnable task, String description, Object... args) {
    this.scheduledExecutorService().execute(namedRunnable(task, description, args));
  }

  BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.recoveryBackOffDelayPolicy;
  }

  BackOffDelayPolicy topologyUpdateBackOffDelayPolicy() {
    return this.topologyUpdateBackOffDelayPolicy;
  }

  CompressionCodecFactory compressionCodecFactory() {
    return this.clientParametersPrototype.compressionCodecFactory;
  }

  Runnable registerConsumer(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      String trackingReference,
      SubscriptionListener subscriptionListener,
      Runnable trackingClosingCallback,
      MessageHandler messageHandler,
      Map<String, String> subscriptionProperties) {
    Runnable closingCallback =
        this.consumersCoordinator.subscribe(
            consumer,
            stream,
            offsetSpecification,
            trackingReference,
            subscriptionListener,
            trackingClosingCallback,
            messageHandler,
            subscriptionProperties);
    return closingCallback;
  }

  Runnable registerProducer(StreamProducer producer, String reference, String stream) {
    return producersCoordinator.registerProducer(producer, reference, stream);
  }

  Client locator() {
    return this.locators.stream()
        .filter(Locator::isSet)
        .findAny()
        .orElseThrow(() -> new LocatorNotAvailableException())
        .client();
  }

  <T> T locatorOperation(Function<Client, T> operation) {
    return locatorOperation(operation, () -> locator(), this.recoveryBackOffDelayPolicy);
  }

  static <T> T locatorOperation(
      Function<Client, T> operation,
      Supplier<Client> clientSupplier,
      BackOffDelayPolicy backOffDelayPolicy) {
    int maxAttempt = 3;
    int attempt = 0;
    boolean executed = false;
    Exception lastException = null;
    T result = null;
    LOGGER.debug("Starting locator operation '{}'", operation);
    long start = System.nanoTime();
    while (attempt < maxAttempt) {
      try {
        Client client = clientSupplier.get();
        LOGGER.debug(
            "Using locator on {}:{} to run operation '{}'",
            client.getHost(),
            client.getPort(),
            operation);
        result = operation.apply(client);
        LOGGER.debug(
            "Locator operation '{}' succeeded in {}",
            operation,
            Duration.ofNanos(System.nanoTime() - start));
        executed = true;
        break;
      } catch (LocatorNotAvailableException e) {
        attempt++;
        try {
          Thread.sleep(backOffDelayPolicy.delay(attempt).toMillis());
        } catch (InterruptedException ex) {
          lastException = ex;
          Thread.currentThread().interrupt();
          break;
        }
      } catch (Exception e) {
        LOGGER.debug("Exception during locator operation '{}': {}", operation, e.getMessage());
        lastException = e;
        break;
      }
    }
    if (!executed) {
      if (lastException == null) {
        throw new LocatorNotAvailableException();
      } else {
        throw new StreamException(
            "Could not execute operation after " + maxAttempt + " attempts", lastException);
      }
    }
    return result;
  }

  Clock clock() {
    return this.clock;
  }

  AddressResolver addressResolver() {
    return this.addressResolver;
  }

  Codec codec() {
    return this.codec;
  }

  Client.ClientParameters clientParametersCopy() {
    return this.clientParametersPrototype.duplicate();
  }

  TrackingConsumerRegistration registerTrackingConsumer(
      StreamConsumer streamConsumer, TrackingConfiguration configuration) {
    Runnable closingCallable = this.producersCoordinator.registerTrackingConsumer(streamConsumer);
    Registration offsetTrackingRegistration;
    if (this.offsetTrackingCoordinator.needTrackingRegistration(configuration)) {
      offsetTrackingRegistration =
          this.offsetTrackingCoordinator.registerTrackingConsumer(streamConsumer, configuration);
    } else {
      offsetTrackingRegistration = null;
    }

    Runnable closingSequence;
    if (offsetTrackingRegistration == null) {
      closingSequence = closingCallable;
    } else {
      closingSequence =
          () -> {
            try {
              LOGGER.debug("Executing offset tracking registration closing sequence ");
              offsetTrackingRegistration.closingCallback().run();
              LOGGER.debug("Offset tracking registration closing sequence executed");
            } catch (Exception e) {
              LOGGER.warn(
                  "Error while executing offset tracking registration closing sequence: {}",
                  e.getMessage());
            }
            closingCallable.run();
          };
    }

    return new TrackingConsumerRegistration(
        closingSequence,
        offsetTrackingRegistration == null
            ? null
            : offsetTrackingRegistration.postMessageProcessingCallback(),
        offsetTrackingRegistration == null
            ? Utils.NO_OP_LONG_CONSUMER
            : offsetTrackingRegistration.trackingCallback(),
        offsetTrackingRegistration == null
            ? Utils.NO_OP_LONG_SUPPLIER
            : () -> offsetTrackingRegistration.flush());
  }

  @Override
  public String toString() {
    return "{ \"locators\" : ["
        + this.locators.stream()
            .map(
                l -> {
                  Client c = l.nullableClient();
                  return c == null ? "null" : ("\"" + c.connectionName() + "\"");
                })
            .collect(Collectors.joining(","))
        + "], "
        + "\"producers\" : "
        + this.producersCoordinator
        + ", \"consumers\" : "
        + this.consumersCoordinator
        + ", \"offset_tracking\" : "
        + this.offsetTrackingCoordinator
        + "}";
  }

  static class TrackingConsumerRegistration {

    private final Runnable closingCallback;
    private final Consumer<Context> postMessageProcessingCallback;
    private final LongConsumer trackingCallback;
    private final LongSupplier flushOperation;

    TrackingConsumerRegistration(
        Runnable closingCallback,
        Consumer<Context> postMessageProcessingCallback,
        LongConsumer trackingCallback,
        LongSupplier flushOperation) {
      this.closingCallback = closingCallback;
      this.postMessageProcessingCallback = postMessageProcessingCallback;
      this.trackingCallback = trackingCallback;
      this.flushOperation = flushOperation;
    }

    Runnable closingCallback() {
      return closingCallback;
    }

    LongConsumer trackingCallback() {
      return trackingCallback;
    }

    Consumer<Context> postMessageProcessingCallback() {
      return postMessageProcessingCallback;
    }

    long flush() {
      return this.flushOperation.getAsLong();
    }
  }

  static class LocatorNotAvailableException extends StreamException {

    public LocatorNotAvailableException() {
      super("Locator not available");
    }
  }

  private void checkNotClosed() {
    if (this.closed.get()) {
      throw new IllegalStateException("This environment instance has been closed");
    }
  }

  private static class Locator {

    private final Address address;
    private volatile Optional<Client> client;

    private Locator(Address address) {
      this.address = address;
      this.client = Optional.empty();
    }

    Locator client(Client client) {
      this.client = Optional.ofNullable(client);
      return this;
    }

    private boolean isNotSet() {
      return !this.isSet();
    }

    private boolean isSet() {
      return this.client.isPresent();
    }

    private Client client() {
      return this.client.orElseThrow(() -> new LocatorNotAvailableException());
    }

    private Client nullableClient() {
      return this.client.orElse(null);
    }

    private Address address() {
      return this.address;
    }
  }
}

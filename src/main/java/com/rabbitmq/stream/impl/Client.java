// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.Constants.COMMAND_CLOSE;
import static com.rabbitmq.stream.Constants.COMMAND_CONSUMER_UPDATE;
import static com.rabbitmq.stream.Constants.COMMAND_CREATE_STREAM;
import static com.rabbitmq.stream.Constants.COMMAND_CREDIT;
import static com.rabbitmq.stream.Constants.COMMAND_DECLARE_PUBLISHER;
import static com.rabbitmq.stream.Constants.COMMAND_DELETE_PUBLISHER;
import static com.rabbitmq.stream.Constants.COMMAND_DELETE_STREAM;
import static com.rabbitmq.stream.Constants.COMMAND_EXCHANGE_COMMAND_VERSIONS;
import static com.rabbitmq.stream.Constants.COMMAND_HEARTBEAT;
import static com.rabbitmq.stream.Constants.COMMAND_METADATA;
import static com.rabbitmq.stream.Constants.COMMAND_OPEN;
import static com.rabbitmq.stream.Constants.COMMAND_PARTITIONS;
import static com.rabbitmq.stream.Constants.COMMAND_PEER_PROPERTIES;
import static com.rabbitmq.stream.Constants.COMMAND_PUBLISH;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_OFFSET;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_PUBLISHER_SEQUENCE;
import static com.rabbitmq.stream.Constants.COMMAND_ROUTE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_AUTHENTICATE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_HANDSHAKE;
import static com.rabbitmq.stream.Constants.COMMAND_STORE_OFFSET;
import static com.rabbitmq.stream.Constants.COMMAND_STREAM_STATS;
import static com.rabbitmq.stream.Constants.COMMAND_SUBSCRIBE;
import static com.rabbitmq.stream.Constants.COMMAND_UNSUBSCRIBE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_OK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SASL_CHALLENGE;
import static com.rabbitmq.stream.Constants.VERSION_1;
import static com.rabbitmq.stream.impl.Utils.encodeRequestCode;
import static com.rabbitmq.stream.impl.Utils.encodeResponseCode;
import static com.rabbitmq.stream.impl.Utils.extractResponseCode;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.noOpConsumer;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Codec.EncodedMessage;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.ShutdownContext.ShutdownReason;
import com.rabbitmq.stream.impl.ServerFrameHandler.FrameHandler;
import com.rabbitmq.stream.impl.ServerFrameHandler.FrameHandlerInfo;
import com.rabbitmq.stream.impl.Utils.NamedThreadFactory;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.metrics.NoOpMetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.DefaultSaslConfiguration;
import com.rabbitmq.stream.sasl.DefaultUsernamePasswordCredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
import com.rabbitmq.stream.sasl.SaslMechanism;
import com.rabbitmq.stream.sasl.UsernamePasswordCredentialsProvider;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is low-level client API to communicate with the broker.
 *
 * <p><b>It is not meant for public usage and can change at any time.</b>
 *
 * <p>Users are encouraged to use the {@link Environment}, {@link Producer}, {@link
 * com.rabbitmq.stream.Consumer} API, and their respective builders to interact with the broker.
 *
 * <p>People wanting very fine control over their interaction with the broker can use {@link Client}
 * but at their own risk.
 */
public class Client implements AutoCloseable {

  public static final int DEFAULT_PORT = 5552;
  public static final int DEFAULT_TLS_PORT = 5551;
  static final OutboundEntityWriteCallback OUTBOUND_MESSAGE_WRITE_CALLBACK =
      new OutboundMessageWriteCallback();
  static final OutboundEntityWriteCallback OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK =
      new OutboundMessageBatchWriteCallback();
  static final String NETTY_HANDLER_FRAME_DECODER =
      LengthFieldBasedFrameDecoder.class.getSimpleName();
  static final String NETTY_HANDLER_IDLE_STATE = IdleStateHandler.class.getSimpleName();
  static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofSeconds(10);
  private static final PublishConfirmListener NO_OP_PUBLISH_CONFIRM_LISTENER =
      (publisherId, publishingId) -> {};
  private static final PublishErrorListener NO_OP_PUBLISH_ERROR_LISTENER =
      (publisherId, publishingId, errorCode) -> {};
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  final PublishConfirmListener publishConfirmListener;
  final PublishErrorListener publishErrorListener;
  final ChunkListener chunkListener;
  final MessageListener messageListener;
  final CreditNotification creditNotification;
  final ConsumerUpdateListener consumerUpdateListener;
  final MetadataListener metadataListener;
  final Codec codec;
  final Channel channel;
  final ConcurrentMap<Integer, OutstandingRequest<?>> outstandingRequests =
      new ConcurrentHashMap<>();
  final List<SubscriptionOffset> subscriptionOffsets = new CopyOnWriteArrayList<>();
  final ExecutorService executorService;
  final ExecutorService dispatchingExecutorService;
  final TuneState tuneState;
  final AtomicBoolean closing = new AtomicBoolean(false);
  final ChunkChecksum chunkChecksum;
  final MetricsCollector metricsCollector;
  final CompressionCodecFactory compressionCodecFactory;
  private final Consumer<ShutdownContext.ShutdownReason> shutdownListenerCallback;
  private final ToLongFunction<Object> publishSequenceFunction =
      new ToLongFunction<Object>() {
        private final AtomicLong publishSequence = new AtomicLong(0);

        @Override
        public long applyAsLong(Object value) {
          return publishSequence.getAndIncrement();
        }
      };
  private final AtomicInteger correlationSequence = new AtomicInteger(0);
  private final Runnable executorServiceClosing;
  private final SaslConfiguration saslConfiguration;
  private final CredentialsProvider credentialsProvider;
  private final Runnable nettyClosing;
  private final int maxFrameSize;
  private final boolean frameSizeCopped;
  private final EventLoopGroup eventLoopGroup;
  private final Map<String, String> clientProperties;
  private final String NETTY_HANDLER_FLUSH_CONSOLIDATION =
      FlushConsolidationHandler.class.getSimpleName();
  private final String NETTY_HANDLER_STREAM = StreamHandler.class.getSimpleName();
  private final String host;
  private final int port;
  private final Map<String, String> serverProperties;
  private final Map<String, String> connectionProperties;
  private final Duration rpcTimeout;
  private volatile ShutdownReason shutdownReason = null;
  private final Runnable exchangeCommandVersionsCheck;

  public Client() {
    this(new ClientParameters());
  }

  public Client(ClientParameters parameters) {
    this.publishConfirmListener = parameters.publishConfirmListener;
    this.publishErrorListener = parameters.publishErrorListener;
    this.chunkListener = parameters.chunkListener;
    this.messageListener = parameters.messageListener;
    this.creditNotification = parameters.creditNotification;
    this.codec = parameters.codec == null ? Codecs.DEFAULT : parameters.codec;
    this.saslConfiguration = parameters.saslConfiguration;
    this.credentialsProvider = parameters.credentialsProvider;
    this.chunkChecksum = parameters.chunkChecksum;
    this.metricsCollector = parameters.metricsCollector;
    this.metadataListener = parameters.metadataListener;
    this.consumerUpdateListener = parameters.consumerUpdateListener;
    this.compressionCodecFactory =
        parameters.compressionCodecFactory == null
            ? compression -> null
            : parameters.compressionCodecFactory;
    this.rpcTimeout = parameters.rpcTimeout == null ? DEFAULT_RPC_TIMEOUT : parameters.rpcTimeout;
    final ShutdownListener shutdownListener = parameters.shutdownListener;
    final AtomicBoolean started = new AtomicBoolean(false);
    this.shutdownListenerCallback =
        Utils.makeIdempotent(
            shutdownReason -> {
              // the channel can become inactive even though the opening is not done yet,
              // so we guard the shutdown listener invocation to avoid trying to reconnect
              // even before having connected. The caller should be notified of the failure
              // by an exception anyway.
              if (started.get()) {
                this.metricsCollector.closeConnection();
                shutdownListener.handle(new ShutdownContext(shutdownReason));
              }
            });

    Consumer<Bootstrap> bootstrapCustomizer =
        parameters.bootstrapCustomizer == null ? noOpConsumer() : parameters.bootstrapCustomizer;

    Bootstrap b = new Bootstrap();
    bootstrapCustomizer.accept(b);
    if (b.config().group() == null) {
      EventLoopGroup eventLoopGroup;
      if (parameters.eventLoopGroup == null) {
        this.eventLoopGroup = new NioEventLoopGroup();
        eventLoopGroup = this.eventLoopGroup;
      } else {
        this.eventLoopGroup = null;
        eventLoopGroup = parameters.eventLoopGroup;
      }
      b.group(eventLoopGroup);
    } else {
      this.eventLoopGroup = null;
    }
    if (b.config().channelFactory() == null) {
      b.channel(NioSocketChannel.class);
    }
    if (!b.config().options().containsKey(ChannelOption.SO_KEEPALIVE)) {
      b.option(ChannelOption.SO_KEEPALIVE, true);
    }
    if (!b.config().options().containsKey(ChannelOption.ALLOCATOR)) {
      b.option(
          ChannelOption.ALLOCATOR,
          parameters.byteBufAllocator == null
              ? ByteBufAllocator.DEFAULT
              : parameters.byteBufAllocator);
    }

    Consumer<Channel> channelCustomizer =
        parameters.channelCustomizer == null ? noOpConsumer() : parameters.channelCustomizer;
    b.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            ch.pipeline()
                .addFirst(
                    NETTY_HANDLER_FLUSH_CONSOLIDATION,
                    new FlushConsolidationHandler(
                        FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true));
            ch.pipeline()
                .addLast(
                    NETTY_HANDLER_FRAME_DECODER,
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
            ch.pipeline().addLast(NETTY_HANDLER_STREAM, new StreamHandler());
            ch.pipeline().addLast(new MetricsHandler(metricsCollector));
            if (parameters.sslContext != null) {
              SslHandler sslHandler =
                  parameters.sslContext.newHandler(ch.alloc(), parameters.host, parameters.port);

              if (parameters.tlsHostnameVerification) {
                SSLEngine sslEngine = sslHandler.engine();
                SSLParameters sslParameters = sslEngine.getSSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(sslParameters);
              }

              ch.pipeline().addFirst("ssl", sslHandler);
            }
            channelCustomizer.accept(ch);
          }
        });

    ChannelFuture f;
    String clientConnectionName =
        parameters.clientProperties == null
            ? ""
            : (parameters.clientProperties.containsKey("connection_name")
                ? parameters.clientProperties.get("connection_name")
                : "");
    try {
      LOGGER.debug(
          "Trying to create stream connection to {}:{}, with client connection name '{}'",
          parameters.host,
          parameters.port,
          clientConnectionName);
      f = b.connect(parameters.host, parameters.port).sync();
      this.host = parameters.host;
      this.port = parameters.port;
    } catch (Exception e) {
      String message =
          format(
              "Error while creating stream connection to %s:%d", parameters.host, parameters.port);
      if (e instanceof ConnectTimeoutException) {
        throw new TimeoutStreamException(message, e);
      } else if (e instanceof ConnectException) {
        throw new ConnectionStreamException(message, e);
      } else {
        throw new StreamException(message, e);
      }
    }

    this.channel = f.channel();
    this.nettyClosing = Utils.makeIdempotent(this::closeNetty);
    ExecutorServiceFactory executorServiceFactory = parameters.executorServiceFactory;
    if (executorServiceFactory == null) {
      this.executorService =
          Executors.newSingleThreadExecutor(new NamedThreadFactory(clientConnectionName + "-"));
    } else {
      this.executorService = executorServiceFactory.get();
    }
    ExecutorServiceFactory dispatchingExecutorServiceFactory =
        parameters.dispatchingExecutorServiceFactory;
    if (dispatchingExecutorServiceFactory == null) {
      this.dispatchingExecutorService =
          Executors.newSingleThreadExecutor(
              new NamedThreadFactory("dispatching-" + clientConnectionName + "-"));
    } else {
      this.dispatchingExecutorService = dispatchingExecutorServiceFactory.get();
    }
    this.executorServiceClosing =
        Utils.makeIdempotent(
            () -> {
              this.dispatchingExecutorService.shutdownNow();
              if (dispatchingExecutorServiceFactory == null) {
                this.dispatchingExecutorService.shutdownNow();
              } else {
                dispatchingExecutorServiceFactory.clientClosed(this.dispatchingExecutorService);
              }
              if (executorServiceFactory == null) {
                this.executorService.shutdownNow();
              } else {
                executorServiceFactory.clientClosed(this.executorService);
              }
            });
    try {
      this.tuneState =
          new TuneState(
              parameters.requestedMaxFrameSize, (int) parameters.requestedHeartbeat.getSeconds());
      this.clientProperties = clientProperties(parameters.clientProperties);
      this.serverProperties = peerProperties();
      authenticate();
      this.tuneState.await(Duration.ofSeconds(10));
      this.maxFrameSize = this.tuneState.getMaxFrameSize();
      this.frameSizeCopped = this.maxFrameSize() > 0;
      LOGGER.debug(
          "Connection tuned with max frame size {} and heartbeat {}",
          this.maxFrameSize(),
          tuneState.getHeartbeat());
      this.connectionProperties = open(parameters.virtualHost);
      Set<FrameHandlerInfo> supportedCommands = maybeExchangeCommandVersions();
      if (supportedCommands.stream().anyMatch(i -> i.getKey() == COMMAND_STREAM_STATS)) {
        this.exchangeCommandVersionsCheck = () -> {};
      } else {
        this.exchangeCommandVersionsCheck =
            () -> {
              throw new UnsupportedOperationException(
                  "QueryStreamInfo is available only on RabbitMQ 3.11 or more.");
            };
      }
      started.set(true);
      this.metricsCollector.openConnection();
    } catch (RuntimeException e) {
      this.closingSequence(null);
      throw e;
    }
  }

  private static class MetricsHandler extends ChannelOutboundHandlerAdapter {

    private final MetricsCollector metricsCollector;

    private MetricsHandler(MetricsCollector metricsCollector) {
      this.metricsCollector = metricsCollector;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      metricsCollector.writtenBytes(((ByteBuf) msg).capacity());
      super.write(ctx, msg, promise);
    }
  }

  private static Map<String, String> clientProperties(Map<String, String> fromParameters) {
    fromParameters = fromParameters == null ? Collections.emptyMap() : fromParameters;
    Map<String, String> clientProperties = new HashMap<>(fromParameters);
    clientProperties.putAll(ClientProperties.DEFAULT_CLIENT_PROPERTIES);
    return Collections.unmodifiableMap(clientProperties);
  }

  static void checkMessageFitsInFrame(int maxFrameSize, Codec.EncodedMessage encodedMessage) {
    int frameBeginning = 4 + 2 + 2 + 4 + 8 + 4 + encodedMessage.getSize();
    if (frameBeginning > maxFrameSize) {
      throw new IllegalArgumentException(
          "Message too big to fit in one frame: " + encodedMessage.getSize());
    }
  }

  Codec codec() {
    return codec;
  }

  int maxFrameSize() {
    return this.maxFrameSize;
  }

  private Map<String, String> peerProperties() {
    int clientPropertiesSize = 4; // size of the map, always there
    if (!clientProperties.isEmpty()) {
      for (Map.Entry<String, String> entry : clientProperties.entrySet()) {
        clientPropertiesSize += 2 + entry.getKey().length() + 2 + entry.getValue().length();
      }
    }
    int length = 2 + 2 + 4 + clientPropertiesSize;
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocateNoCheck(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_PEER_PROPERTIES));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeInt(clientProperties.size());
      for (Map.Entry<String, String> entry : clientProperties.entrySet()) {
        bb.writeShort(entry.getKey().length())
            .writeBytes(entry.getKey().getBytes(StandardCharsets.UTF_8))
            .writeShort(entry.getValue().length())
            .writeBytes(entry.getValue().getBytes(StandardCharsets.UTF_8));
      }
      OutstandingRequest<Map<String, String>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      if (request.error() == null) {
        return request.response.get();
      } else {
        throw new StreamException("Error when establishing stream connection", request.error());
      }
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while trying to exchange peer properties", e);
    }
  }

  private void authenticate() {
    List<String> saslMechanisms = getSaslMechanisms();
    SaslMechanism saslMechanism = this.saslConfiguration.getSaslMechanism(saslMechanisms);

    byte[] challenge = null;
    boolean authDone = false;
    while (!authDone) {
      byte[] saslResponse = saslMechanism.handleChallenge(challenge, this.credentialsProvider);
      SaslAuthenticateResponse saslAuthenticateResponse =
          sendSaslAuthenticate(saslMechanism, saslResponse);
      if (saslAuthenticateResponse.isOk()) {
        authDone = true;
      } else if (saslAuthenticateResponse.isChallenge()) {
        challenge = saslAuthenticateResponse.challenge;
      } else if (saslAuthenticateResponse.isAuthenticationFailure()) {
        throw new AuthenticationFailureException(
            "Unexpected response code during authentication: "
                + formatConstant(saslAuthenticateResponse.getResponseCode()),
            saslAuthenticateResponse.getResponseCode());
      } else {
        throw new StreamException(
            "Unexpected response code during authentication: "
                + formatConstant(saslAuthenticateResponse.getResponseCode()));
      }
    }
  }

  private SaslAuthenticateResponse sendSaslAuthenticate(
      SaslMechanism saslMechanism, byte[] challengeResponse) {
    int length =
        2
            + 2
            + 4
            + 2
            + saslMechanism.getName().length()
            + 4
            + (challengeResponse == null ? 0 : challengeResponse.length);
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocateNoCheck(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_SASL_AUTHENTICATE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(saslMechanism.getName().length());
      bb.writeBytes(saslMechanism.getName().getBytes(StandardCharsets.UTF_8));
      if (challengeResponse == null) {
        bb.writeInt(-1);
      } else {
        bb.writeInt(challengeResponse.length).writeBytes(challengeResponse);
      }
      OutstandingRequest<SaslAuthenticateResponse> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while trying to authenticate", e);
    }
  }

  private Map<String, String> open(String virtualHost) {
    int length = 2 + 2 + 4 + 2 + virtualHost.length();
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_OPEN));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(virtualHost.length());
      bb.writeBytes(virtualHost.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<OpenResponse> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      if (!request.response.get().isOk()) {
        throw new StreamException(
            "Unexpected response code when connecting to virtual host: "
                + formatConstant(request.response.get().getResponseCode()));
      }
      return request.response.get().connectionProperties;
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error during open command", e);
    }
  }

  // for testing
  void send(byte[] content) {
    ByteBuf bb = allocateNoCheck(content.length);
    bb.writeBytes(content);
    try {
      channel.writeAndFlush(bb).sync();
    } catch (InterruptedException e) {
      throw new StreamException("Error while sending bytes", e);
    }
  }

  private void sendClose(short code, String reason) {
    int length = 2 + 2 + 4 + 2 + 2 + reason.length();
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_CLOSE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(code);
      bb.writeShort(reason.length());
      bb.writeBytes(reason.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      if (!request.response.get().isOk()) {
        LOGGER.warn(
            "Unexpected response code when closing: {}",
            formatConstant(request.response.get().getResponseCode()));
        throw new StreamException(
            "Unexpected response code when closing: "
                + formatConstant(request.response.get().getResponseCode()));
      }
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while closing connection", e);
    }
  }

  private List<String> getSaslMechanisms() {
    int length = 2 + 2 + 4;
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocateNoCheck(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_SASL_HANDSHAKE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      OutstandingRequest<List<String>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while exchanging SASL mechanisms", e);
    }
  }

  public Response create(String stream) {
    return create(stream, Collections.emptyMap());
  }

  public Response create(String stream, Map<String, String> arguments) {
    int length = 2 + 2 + 4 + 2 + stream.length() + 4;
    for (Map.Entry<String, String> argument : arguments.entrySet()) {
      length = length + 2 + argument.getKey().length() + 2 + argument.getValue().length();
    }
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_CREATE_STREAM));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      bb.writeInt(arguments.size());
      for (Map.Entry<String, String> argument : arguments.entrySet()) {
        bb.writeShort(argument.getKey().length());
        bb.writeBytes(argument.getKey().getBytes(StandardCharsets.UTF_8));
        bb.writeShort(argument.getValue().length());
        bb.writeBytes(argument.getValue().getBytes(StandardCharsets.UTF_8));
      }
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(format("Error while creating stream '%s'", stream), e);
    }
  }

  ByteBuf allocate(ByteBufAllocator allocator, int capacity) {
    if (frameSizeCopped && capacity > this.maxFrameSize()) {
      throw new IllegalArgumentException(
          "Cannot allocate "
              + capacity
              + " bytes for outbound frame, limit is "
              + this.maxFrameSize());
    }
    return allocator.buffer(capacity);
  }

  private ByteBuf allocate(int capacity) {
    return allocate(channel.alloc(), capacity);
  }

  ByteBuf allocateNoCheck(ByteBufAllocator allocator, int capacity) {
    return allocator.buffer(capacity);
  }

  private ByteBuf allocateNoCheck(int capacity) {
    return allocateNoCheck(channel.alloc(), capacity);
  }

  public Response delete(String stream) {
    int length = 2 + 2 + 4 + 2 + stream.length();
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_DELETE_STREAM));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(format("Error while deleting stream '%s'", stream), e);
    }
  }

  public Map<String, StreamMetadata> metadata(String... streams) {
    if (streams == null || streams.length == 0) {
      throw new IllegalArgumentException("At least one stream must be specified");
    }
    int length = 2 + 2 + 4 + 4; // API code, version, correlation ID, size of array
    for (String stream : streams) {
      length += 2;
      length += stream.length();
    }
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_METADATA));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeInt(streams.length);
      for (String stream : streams) {
        bb.writeShort(stream.length());
        bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      }
      OutstandingRequest<Map<String, StreamMetadata>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format("Error while getting metadata for stream(s) '%s'", join(",", streams)), e);
    }
  }

  public Response declarePublisher(byte publisherId, String publisherReference, String stream) {
    int publisherReferenceSize =
        (publisherReference == null || publisherReference.isEmpty()
            ? 0
            : publisherReference.length());
    if (publisherReferenceSize > 256) {
      throw new IllegalArgumentException(
          "If specified, publisher reference must less than 256 characters");
    }
    int length = 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + stream.length();
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_DECLARE_PUBLISHER));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeByte(publisherId);
      bb.writeShort(publisherReferenceSize);
      if (publisherReferenceSize > 0) {
        bb.writeBytes(publisherReference.getBytes(StandardCharsets.UTF_8));
      }
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format("Error while declaring publisher for stream '%s'", stream), e);
    }
  }

  public Response deletePublisher(byte publisherId) {
    int length = 2 + 2 + 4 + 1;
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_DELETE_PUBLISHER));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeByte(publisherId);
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while deleting publisher", e);
    }
  }

  public List<Long> publish(byte publisherId, List<Message> messages) {
    return this.publish(publisherId, messages, this.publishSequenceFunction);
  }

  public List<Long> publish(
      byte publisherId, List<Message> messages, ToLongFunction<Object> publishSequenceFunction) {
    List<Object> encodedMessages = new ArrayList<>(messages.size());
    for (Message message : messages) {
      Codec.EncodedMessage encodedMessage = codec.encode(message);
      checkMessageFitsInFrame(encodedMessage);
      encodedMessages.add(encodedMessage);
    }
    return publishInternal(
        this.channel,
        publisherId,
        encodedMessages,
        OUTBOUND_MESSAGE_WRITE_CALLBACK,
        publishSequenceFunction);
  }

  public List<Long> publish(
      byte publisherId, List<Message> messages, OutboundEntityMappingCallback mappingCallback) {
    return this.publish(publisherId, messages, mappingCallback, this.publishSequenceFunction);
  }

  public List<Long> publish(
      byte publisherId,
      List<Message> messages,
      OutboundEntityMappingCallback mappingCallback,
      ToLongFunction<Object> publishSequenceFunction) {
    List<Object> encodedMessages = new ArrayList<>(messages.size());
    for (Message message : messages) {
      Codec.EncodedMessage encodedMessage = codec.encode(message);
      checkMessageFitsInFrame(encodedMessage);
      OriginalAndEncodedOutboundEntity wrapper =
          new OriginalAndEncodedOutboundEntity(message, encodedMessage);
      encodedMessages.add(wrapper);
    }
    return publishInternal(
        this.channel,
        publisherId,
        encodedMessages,
        new OriginalEncodedEntityOutboundEntityWriteCallback(
            mappingCallback, OUTBOUND_MESSAGE_WRITE_CALLBACK),
        publishSequenceFunction);
  }

  public List<Long> publishBatches(byte publisherId, List<MessageBatch> messageBatches) {
    return this.publishBatches(publisherId, messageBatches, this.publishSequenceFunction);
  }

  public List<Long> publishBatches(
      byte publisherId,
      List<MessageBatch> messageBatches,
      ToLongFunction<Object> publishSequenceFunction) {
    List<Object> encodedMessageBatches = new ArrayList<>(messageBatches.size());
    for (MessageBatch batch : messageBatches) {
      EncodedMessageBatch encodedMessageBatch =
          createEncodedMessageBatch(batch.compression, batch.messages.size());
      for (Message message : batch.messages) {
        Codec.EncodedMessage encodedMessage = codec.encode(message);
        checkMessageFitsInFrame(encodedMessage);
        encodedMessageBatch.add(encodedMessage);
      }
      encodedMessageBatch.close();
      checkMessageBatchFitsInFrame(encodedMessageBatch);
      encodedMessageBatches.add(encodedMessageBatch);
    }
    return publishInternal(
        this.channel,
        publisherId,
        encodedMessageBatches,
        OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK,
        publishSequenceFunction);
  }

  public List<Long> publishBatches(
      byte publisherId,
      List<MessageBatch> messageBatches,
      OutboundEntityMappingCallback mappingCallback) {
    return this.publishBatches(
        publisherId, messageBatches, mappingCallback, this.publishSequenceFunction);
  }

  public List<Long> publishBatches(
      byte publisherId,
      List<MessageBatch> messageBatches,
      OutboundEntityMappingCallback mappingCallback,
      ToLongFunction<Object> publishSequenceFunction) {
    List<Object> encodedMessageBatches = new ArrayList<>(messageBatches.size());
    for (MessageBatch batch : messageBatches) {
      EncodedMessageBatch encodedMessageBatch =
          createEncodedMessageBatch(batch.compression, batch.messages.size());
      for (Message message : batch.messages) {
        Codec.EncodedMessage encodedMessage = codec.encode(message);
        checkMessageFitsInFrame(encodedMessage);
        encodedMessageBatch.add(encodedMessage);
      }
      encodedMessageBatch.close();
      checkMessageBatchFitsInFrame(encodedMessageBatch);
      OriginalAndEncodedOutboundEntity wrapper =
          new OriginalAndEncodedOutboundEntity(batch, encodedMessageBatch);
      encodedMessageBatches.add(wrapper);
    }
    return publishInternal(
        this.channel,
        publisherId,
        encodedMessageBatches,
        new OriginalEncodedEntityOutboundEntityWriteCallback(
            mappingCallback, OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK),
        publishSequenceFunction);
  }

  private void checkMessageFitsInFrame(Codec.EncodedMessage encodedMessage) {
    checkMessageFitsInFrame(this.maxFrameSize(), encodedMessage);
  }

  private void checkMessageBatchFitsInFrame(EncodedMessageBatch encodedMessageBatch) {
    int frameBeginning =
        4
            + 2
            + 2
            + 4
            + 8
            + 1
            + 2 // byte with entry type and compression, short with number of messages in batch
            + 4
            + encodedMessageBatch.sizeInBytes();
    if (frameBeginning > this.maxFrameSize()) {
      throw new IllegalArgumentException(
          "Message batch too big to fit in one frame: " + encodedMessageBatch.sizeInBytes());
    }
  }

  List<Long> publishInternal(
      byte publisherId,
      List<Object> encodedEntities,
      OutboundEntityWriteCallback callback,
      ToLongFunction<Object> publishSequenceFunction) {
    return this.publishInternal(
        this.channel, publisherId, encodedEntities, callback, publishSequenceFunction);
  }

  List<Long> publishInternal(
      Channel ch,
      byte publisherId,
      List<Object> encodedEntities,
      OutboundEntityWriteCallback callback,
      ToLongFunction<Object> publishSequenceFunction) {
    int frameHeaderLength = 2 + 2 + 1 + 4;
    List<Long> sequences = new ArrayList<>(encodedEntities.size());
    int length = frameHeaderLength;
    int currentIndex = 0;
    int startIndex = 0;
    for (Object encodedEntity : encodedEntities) {
      length += callback.fragmentLength(encodedEntity);
      if (length > this.maxFrameSize()) {
        // the current message/batch does not fit, we're sending the batch
        int frameLength = length - callback.fragmentLength(encodedEntity);
        sendEntityBatch(
            ch,
            frameLength,
            publisherId,
            startIndex,
            currentIndex,
            encodedEntities,
            callback,
            sequences,
            publishSequenceFunction);
        length = frameHeaderLength + callback.fragmentLength(encodedEntity);
        startIndex = currentIndex;
      }
      currentIndex++;
    }
    sendEntityBatch(
        ch,
        length,
        publisherId,
        startIndex,
        currentIndex,
        encodedEntities,
        callback,
        sequences,
        publishSequenceFunction);

    return sequences;
  }

  private void sendEntityBatch(
      Channel ch,
      int frameLength,
      byte publisherId,
      int fromIncluded,
      int toExcluded,
      List<Object> messages,
      OutboundEntityWriteCallback callback,
      List<Long> sequences,
      ToLongFunction<Object> publishSequenceFunction) {
    // no check because it's been done already
    ByteBuf out = allocateNoCheck(ch.alloc(), frameLength + 4);
    out.writeInt(frameLength);
    out.writeShort(encodeRequestCode(COMMAND_PUBLISH));
    out.writeShort(VERSION_1);
    out.writeByte(publisherId);
    int messageCount = 0;
    out.writeInt(toExcluded - fromIncluded);
    for (int i = fromIncluded; i < toExcluded; i++) {
      Object message = messages.get(i);
      long sequence = publishSequenceFunction.applyAsLong(message);
      out.writeLong(sequence);
      messageCount += callback.write(out, message, sequence);
      sequences.add(sequence);
    }
    int msgCount = messageCount;
    ch.writeAndFlush(out)
        .addListener(
            future -> {
              if (future.isSuccess()) {
                metricsCollector.publish(msgCount);
              }
            });
  }

  public MessageBuilder messageBuilder() {
    return this.codec.messageBuilder();
  }

  public void credit(byte subscriptionId, int credit) {
    if (credit < 0 || credit > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Credit value must be between 0 and " + Short.MAX_VALUE);
    }
    int length = 2 + 2 + 1 + 2;

    ByteBuf bb = allocate(length + 4);
    bb.writeInt(length);
    bb.writeShort(encodeRequestCode(COMMAND_CREDIT));
    bb.writeShort(VERSION_1);
    bb.writeByte(subscriptionId);
    bb.writeShort((short) credit);
    channel.writeAndFlush(bb);
  }

  /**
   * Subscribe to receive messages from a stream.
   *
   * <p>Note the offset is an unsigned long. Longs are signed in Java, but unsigned longs can be
   * used as long as some care is taken for some operations. See the <code>unsigned*</code> static
   * methods in {@link Long}.
   *
   * @param subscriptionId identifier to correlate inbound messages to this subscription
   * @param stream the stream to consume from
   * @param offsetSpecification the specification of the offset to consume from
   * @param credit the initial number of credits
   * @return the subscription confirmation
   */
  public Response subscribe(
      byte subscriptionId, String stream, OffsetSpecification offsetSpecification, int credit) {
    return this.subscribe(
        subscriptionId, stream, offsetSpecification, credit, Collections.emptyMap());
  }

  /**
   * Subscribe to receive messages from a stream.
   *
   * <p>Note the offset is an unsigned long. Longs are signed in Java, but unsigned longs can be
   * used as long as some care is taken for some operations. See the <code>unsigned*</code> static
   * methods in {@link Long}.
   *
   * @param subscriptionId identifier to correlate inbound messages to this subscription
   * @param stream the stream to consume from
   * @param offsetSpecification the specification of the offset to consume from
   * @param initialCredits the initial number of credits
   * @param properties some optional properties to describe the subscription
   * @return the subscription confirmation
   */
  public Response subscribe(
      byte subscriptionId,
      String stream,
      OffsetSpecification offsetSpecification,
      int initialCredits,
      Map<String, String> properties) {
    if (initialCredits < 0 || initialCredits > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Credit value must be between 0 and " + Short.MAX_VALUE);
    }
    int length = 2 + 2 + 4 + 1 + 2 + stream.length() + 2 + 2; // misses the offset
    if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
      length += 8;
    }
    int propertiesSize = 0;
    if (properties != null && !properties.isEmpty()) {
      propertiesSize = 4; // size of the map
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        propertiesSize += 2 + entry.getKey().length() + 2 + entry.getValue().length();
      }
    }
    length += propertiesSize;
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_SUBSCRIBE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeByte(subscriptionId);
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      bb.writeShort(offsetSpecification.getType());
      if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
        bb.writeLong(offsetSpecification.getOffset());
      }
      bb.writeShort(initialCredits);
      if (properties != null && !properties.isEmpty()) {
        bb.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          bb.writeShort(entry.getKey().length())
              .writeBytes(entry.getKey().getBytes(StandardCharsets.UTF_8))
              .writeShort(entry.getValue().length())
              .writeBytes(entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
      }
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      if (offsetSpecification.isOffset()) {
        subscriptionOffsets.add(
            new SubscriptionOffset(subscriptionId, offsetSpecification.getOffset()));
      }
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format("Error while trying to subscribe to stream '%s'", stream), e);
    }
  }

  public void storeOffset(String reference, String stream, long offset) {
    if (reference == null || reference.isEmpty() || reference.length() > 256) {
      throw new IllegalArgumentException(
          "Reference must a non-empty string of less than 256 characters");
    }
    if (stream == null || stream.isEmpty()) {
      throw new IllegalArgumentException("Stream cannot be null or empty");
    }
    int length = 2 + 2 + 2 + reference.length() + 2 + stream.length() + 8;
    ByteBuf bb = allocate(length + 4);
    bb.writeInt(length);
    bb.writeShort(encodeRequestCode(COMMAND_STORE_OFFSET));
    bb.writeShort(VERSION_1);
    bb.writeShort(reference.length());
    bb.writeBytes(reference.getBytes(StandardCharsets.UTF_8));
    bb.writeShort(stream.length());
    bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
    bb.writeLong(offset);
    channel.writeAndFlush(bb);
  }

  public QueryOffsetResponse queryOffset(String reference, String stream) {
    if (reference == null || reference.isEmpty() || reference.length() > 256) {
      throw new IllegalArgumentException(
          "Reference must a non-empty string of less than 256 characters");
    }
    if (stream == null || stream.isEmpty()) {
      throw new IllegalArgumentException("Stream cannot be null or empty");
    }

    int length = 2 + 2 + 4 + 2 + reference.length() + 2 + stream.length();
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_QUERY_OFFSET));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(reference.length());
      bb.writeBytes(reference.getBytes(StandardCharsets.UTF_8));
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<QueryOffsetResponse> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      QueryOffsetResponse response = request.response.get();
      return response;
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format(
              "Error while querying offset for reference '%s' on stream '%s'", reference, stream),
          e);
    }
  }

  public long queryPublisherSequence(String publisherReference, String stream) {
    if (publisherReference == null
        || publisherReference.isEmpty()
        || publisherReference.length() > 256) {
      throw new IllegalArgumentException(
          "Publisher reference must a non-empty string of less than 256 characters");
    }
    if (stream == null || stream.isEmpty()) {
      throw new IllegalArgumentException("Stream cannot be null or empty");
    }

    int length = 2 + 2 + 4 + 2 + publisherReference.length() + 2 + stream.length();
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_QUERY_PUBLISHER_SEQUENCE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(publisherReference.length());
      bb.writeBytes(publisherReference.getBytes(StandardCharsets.UTF_8));
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<QueryPublisherSequenceResponse> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      QueryPublisherSequenceResponse response = request.response.get();
      if (!response.isOk()) {
        LOGGER.info(
            "Query publisher sequence failed with code {}",
            formatConstant(response.getResponseCode()));
      }
      return response.getSequence();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format(
              "Error while querying publisher sequence for '%s' on stream '%s'",
              publisherReference, stream),
          e);
    }
  }

  public Response unsubscribe(byte subscriptionId) {
    int length = 2 + 2 + 4 + 1;
    int correlationId = correlationSequence.getAndIncrement();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_UNSUBSCRIBE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeByte(subscriptionId);
      OutstandingRequest<Response> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while unsubscribing", e);
    }
  }

  public void close() {
    if (closing.compareAndSet(false, true)) {
      LOGGER.debug("Closing client");

      sendClose(RESPONSE_CODE_OK, "OK");

      closingSequence(ShutdownContext.ShutdownReason.CLIENT_CLOSE);

      LOGGER.debug("Client closed");
    }
  }

  void closingSequence(ShutdownContext.ShutdownReason reason) {
    if (reason != null) {
      this.shutdownListenerCallback.accept(reason);
    }
    this.nettyClosing.run();
    this.executorServiceClosing.run();
  }

  private void closeNetty() {
    try {
      if (this.channel.isOpen()) {
        LOGGER.debug("Closing Netty channel");
        this.channel.close().get(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      LOGGER.info("Channel closing has been interrupted");
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.info("Channel closing failed", e);
    } catch (TimeoutException e) {
      LOGGER.info("Could not close channel in 10 seconds");
    }

    maybeShutdownEventLoop();
  }

  private void maybeShutdownEventLoop() {
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

  public boolean isOpen() {
    return !closing.get();
  }

  String getHost() {
    return host;
  }

  int getPort() {
    return port;
  }

  String connectionName() {
    StringBuilder builder = new StringBuilder();
    SocketAddress localAddress = localAddress();
    if (localAddress instanceof InetSocketAddress) {
      InetSocketAddress address = (InetSocketAddress) localAddress;
      builder.append(address.getHostString()).append(":").append(address.getPort());
    } else {
      builder.append("?");
    }
    builder.append(" -> ");
    return builder.append(serverAddress()).toString();
  }

  private String serverAddress() {
    SocketAddress remoteAddress = remoteAddress();
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress address = (InetSocketAddress) remoteAddress;
      return address.getHostString() + ":" + address.getPort();
    } else {
      return this.host + ":" + this.port;
    }
  }

  public List<String> route(String routingKey, String superStream) {
    if (routingKey == null || superStream == null) {
      throw new IllegalArgumentException("routing key and stream must not be null");
    }
    int length =
        2
            + 2
            + 4
            + 2
            + routingKey.length()
            + 2
            + superStream.length(); // API code, version, correlation ID, 2 strings
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_ROUTE));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(routingKey.length());
      bb.writeBytes(routingKey.getBytes(StandardCharsets.UTF_8));
      bb.writeShort(superStream.length());
      bb.writeBytes(superStream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<List<String>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format(
              "Error while querying route for routing key '%s' on super stream '%s'",
              routingKey, superStream),
          e);
    }
  }

  public List<String> partitions(String superStream) {
    if (superStream == null) {
      throw new IllegalArgumentException("stream must not be null");
    }
    int length =
        2 + 2 + 4 + 2 + superStream.length(); // API code, version, correlation ID, 1 string
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_PARTITIONS));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(superStream.length());
      bb.writeBytes(superStream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<List<String>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format("Error while querying partitions for super stream '%s'", superStream), e);
    }
  }

  List<FrameHandlerInfo> exchangeCommandVersions() {
    List<FrameHandlerInfo> commandVersions = ServerFrameHandler.commandVersions();
    int length = 2 + 2 + 4 + 4; // API code, version, correlation ID, array size
    length += commandVersions.size() * (2 + 2 + 2);
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_EXCHANGE_COMMAND_VERSIONS));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeInt(commandVersions.size());
      for (FrameHandlerInfo commandVersion : commandVersions) {
        bb.writeShort(commandVersion.getKey());
        bb.writeShort(commandVersion.getMinVersion());
        bb.writeShort(commandVersion.getMaxVersion());
      }
      OutstandingRequest<List<FrameHandlerInfo>> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException("Error while exchanging command version", e);
    }
  }

  StreamStatsResponse streamStats(String stream) {
    this.exchangeCommandVersionsCheck.run();
    if (stream == null) {
      throw new IllegalArgumentException("stream must not be null");
    }
    int length = 2 + 2 + 4 + 2 + stream.length(); // API code, version, correlation ID, 1 string
    int correlationId = correlationSequence.incrementAndGet();
    try {
      ByteBuf bb = allocate(length + 4);
      bb.writeInt(length);
      bb.writeShort(encodeRequestCode(COMMAND_STREAM_STATS));
      bb.writeShort(VERSION_1);
      bb.writeInt(correlationId);
      bb.writeShort(stream.length());
      bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
      OutstandingRequest<StreamStatsResponse> request = outstandingRequest();
      outstandingRequests.put(correlationId, request);
      channel.writeAndFlush(bb);
      request.block();
      return request.response.get();
    } catch (StreamException e) {
      outstandingRequests.remove(correlationId);
      throw e;
    } catch (RuntimeException e) {
      outstandingRequests.remove(correlationId);
      throw new StreamException(
          format("Error while querying statistics for stream '%s'", stream), e);
    }
  }

  public void consumerUpdateResponse(
      int correlationId, short responseCode, OffsetSpecification offsetSpecification) {
    offsetSpecification =
        offsetSpecification == null ? OffsetSpecification.none() : offsetSpecification;
    int length = 2 + 2 + 4 + 2 + 2; // API code, version, correlation ID, response code, offset type

    if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
      length += 8;
    }

    ByteBuf bb = allocate(length + 4);
    bb.writeInt(length);
    bb.writeShort(encodeResponseCode(COMMAND_CONSUMER_UPDATE));
    bb.writeShort(VERSION_1);
    bb.writeInt(correlationId);
    bb.writeShort(responseCode);
    bb.writeShort(offsetSpecification.getType());
    if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
      bb.writeLong(offsetSpecification.getOffset());
    }
    channel.writeAndFlush(bb);
  }

  void shutdownReason(ShutdownReason reason) {
    this.shutdownReason = reason;
  }

  public SocketAddress localAddress() {
    return this.channel.localAddress();
  }

  public SocketAddress remoteAddress() {
    return this.channel.remoteAddress();
  }

  String serverAdvertisedHost() {
    return this.connectionProperties("advertised_host");
  }

  int serverAdvertisedPort() {
    return Integer.valueOf(this.connectionProperties("advertised_port"));
  }

  String brokerVersion() {
    return this.serverProperties.get("version");
  }

  private String connectionProperties(String key) {
    if (this.connectionProperties != null && this.connectionProperties.containsKey(key)) {
      return this.connectionProperties.get(key);
    } else {
      throw new IllegalArgumentException(
          "Connection property '"
              + key
              + "' missing. Available properties: "
              + this.connectionProperties
              + ".");
    }
  }

  private EncodedMessageBatch createEncodedMessageBatch(Compression compression, int batchSize) {
    return EncodedMessageBatch.create(
        channel.alloc(), compression.code(), compressionCodecFactory.get(compression), batchSize);
  }

  private Set<FrameHandlerInfo> maybeExchangeCommandVersions() {
    Set<FrameHandlerInfo> supported = new HashSet<>();
    try {
      if (Utils.is3_11_OrMore(brokerVersion())) {
        for (FrameHandlerInfo info : exchangeCommandVersions()) {
          if (info.getKey() == COMMAND_STREAM_STATS) {
            supported.add(info);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.info("Error while exchanging command versions: {}", e.getMessage());
    }
    return supported;
  }

  public interface OutboundEntityMappingCallback {

    void handle(long publishingId, Object originalMessageOrBatch);
  }

  interface OutboundEntityWriteCallback {

    int write(ByteBuf bb, Object entity, long publishingId);

    int fragmentLength(Object entity);
  }

  public interface PublishConfirmListener {

    void handle(byte publisherId, long publishingId);
  }

  public interface PublishErrorListener {

    void handle(byte publisherId, long publishingId, short errorCode);
  }

  public interface MetadataListener {

    void handle(String stream, short code);
  }

  public interface ChunkListener {

    /**
     * Callback when a chunk is received as part of a deliver operation.
     *
     * <p>Note the offset is an unsigned long. Longs are signed in Java, but unsigned longs can be
     * used as long as some care is taken for some operations. See the <code>unsigned*</code> static
     * methods in {@link Long}.
     *
     * @param client the client instance (e.g. to ask for more credit)
     * @param subscriptionId the subscription ID to correlate with a callback
     * @param offset the first offset in the chunk
     * @param messageCount the total number of messages in the chunk
     * @param dataSize the size in bytes of the data in the chunk
     */
    void handle(Client client, byte subscriptionId, long offset, long messageCount, long dataSize);
  }

  public interface MessageListener {

    void handle(
        byte subscriptionId,
        long offset,
        long chunkTimestamp,
        long committedChunkId,
        Message message);
  }

  public interface CreditNotification {

    void handle(byte subscriptionId, short responseCode);
  }

  public interface ConsumerUpdateListener {

    OffsetSpecification handle(Client client, byte subscriptionId, boolean active);
  }

  public interface ShutdownListener {

    void handle(ShutdownContext shutdownContext);
  }

  interface EncodedMessageBatch {

    static EncodedMessageBatch create(
        ByteBufAllocator allocator,
        byte compression,
        CompressionCodec compressionCodec,
        int batchSize) {
      if (compression == Compression.NONE.code()) {
        return new PlainEncodedMessageBatch(new ArrayList<>(batchSize));
      } else {
        return new CompressedEncodedMessageBatch(allocator, compressionCodec, batchSize);
      }
    }

    void add(Codec.EncodedMessage encodedMessage);

    void close();

    void write(ByteBuf bb);

    int batchSize();

    int sizeInBytes();

    int uncompressedSizeInBytes();

    byte compression();
  }

  private static final class OriginalAndEncodedOutboundEntity {

    private final Object original, encoded;

    private OriginalAndEncodedOutboundEntity(Object original, Object encoded) {
      this.original = original;
      this.encoded = encoded;
    }
  }

  private static final class OriginalEncodedEntityOutboundEntityWriteCallback
      implements OutboundEntityWriteCallback {

    private final OutboundEntityMappingCallback callback;
    private final OutboundEntityWriteCallback delegate;

    private OriginalEncodedEntityOutboundEntityWriteCallback(
        OutboundEntityMappingCallback callback, OutboundEntityWriteCallback delegate) {
      this.callback = callback;
      this.delegate = delegate;
    }

    @Override
    public int write(ByteBuf bb, Object entity, long publishingId) {
      OriginalAndEncodedOutboundEntity wrapper = (OriginalAndEncodedOutboundEntity) entity;
      callback.handle(publishingId, wrapper.original);
      return delegate.write(bb, wrapper.encoded, publishingId);
    }

    @Override
    public int fragmentLength(Object entity) {
      OriginalAndEncodedOutboundEntity wrapper = (OriginalAndEncodedOutboundEntity) entity;
      return delegate.fragmentLength(wrapper.encoded);
    }
  }

  private static class PlainEncodedMessageBatch implements EncodedMessageBatch {

    private final List<Codec.EncodedMessage> messages;
    private int size;

    PlainEncodedMessageBatch(List<Codec.EncodedMessage> messages) {
      this.messages = messages;
    }

    @Override
    public void add(Codec.EncodedMessage encodedMessage) {
      this.messages.add(encodedMessage);
      size += (4 + encodedMessage.getSize());
    }

    @Override
    public void close() {}

    @Override
    public void write(ByteBuf bb) {
      for (Codec.EncodedMessage message : messages) {
        bb.writeInt(message.getSize()).writeBytes(message.getData(), 0, message.getSize());
      }
    }

    @Override
    public int batchSize() {
      return this.messages.size();
    }

    @Override
    public int sizeInBytes() {
      return this.size;
    }

    @Override
    public int uncompressedSizeInBytes() {
      return this.size;
    }

    @Override
    public byte compression() {
      return Compression.NONE.code();
    }
  }

  static class CompressedEncodedMessageBatch implements EncodedMessageBatch {

    private final ByteBufAllocator allocator;
    private final CompressionCodec codec;
    private final List<EncodedMessage> messages;
    private int uncompressedByteSize = 0;
    private ByteBuf buffer;

    CompressedEncodedMessageBatch(
        ByteBufAllocator allocator,
        CompressionCodec codec,
        List<EncodedMessage> messages,
        int batchSize) {
      this.allocator = allocator;
      this.codec = codec;
      this.messages = new ArrayList<>(batchSize);
      for (int i = 0; i < messages.size(); i++) {
        this.add(messages.get(i));
      }
    }

    CompressedEncodedMessageBatch(
        ByteBufAllocator allocator, CompressionCodec codec, int batchSize) {
      this(allocator, codec, Collections.emptyList(), batchSize);
    }

    @Override
    public void add(Codec.EncodedMessage encodedMessage) {
      this.messages.add(encodedMessage);
      this.uncompressedByteSize += (4 + encodedMessage.getSize());
    }

    @Override
    public void close() {
      int maxCompressedLength = codec.maxCompressedLength(this.uncompressedByteSize);
      this.buffer = allocator.buffer(maxCompressedLength);
      OutputStream outputStream = this.codec.compress(new ByteBufOutputStream(buffer));
      try {
        for (int i = 0; i < messages.size(); i++) {
          final int size = messages.get(i).getSize();
          outputStream.write((size >>> 24) & 0xFF);
          outputStream.write((size >>> 16) & 0xFF);
          outputStream.write((size >>> 8) & 0xFF);
          outputStream.write((size >>> 0) & 0xFF);
          outputStream.write(messages.get(i).getData(), 0, size);
        }
        outputStream.flush();
        outputStream.close();
      } catch (IOException e) {
        throw new StreamException("Error while closing compressing output stream", e);
      }
    }

    @Override
    public void write(ByteBuf bb) {
      bb.writeBytes(this.buffer, 0, this.buffer.writerIndex());
      this.buffer.release();
    }

    @Override
    public int batchSize() {
      return this.messages.size();
    }

    @Override
    public int sizeInBytes() {
      return this.buffer.writerIndex();
    }

    @Override
    public int uncompressedSizeInBytes() {
      return this.uncompressedByteSize;
    }

    @Override
    public byte compression() {
      return this.codec.code();
    }
  }

  private static class OutboundMessageWriteCallback implements OutboundEntityWriteCallback {

    @Override
    public int write(ByteBuf bb, Object entity, long publishingId) {
      Codec.EncodedMessage messageToPublish = (Codec.EncodedMessage) entity;
      bb.writeInt(messageToPublish.getSize());
      bb.writeBytes(messageToPublish.getData(), 0, messageToPublish.getSize());
      return 1;
    }

    @Override
    public int fragmentLength(Object entity) {
      return 8 + 4 + ((Codec.EncodedMessage) entity).getSize(); // publish ID + message size
    }
  }

  private static class OutboundMessageBatchWriteCallback implements OutboundEntityWriteCallback {

    @Override
    public int write(ByteBuf bb, Object entity, long publishingId) {
      EncodedMessageBatch batchToPublish = (EncodedMessageBatch) entity;
      bb.writeByte(
          0x80
              | batchToPublish.compression()
                  << 4); // 1=SubBatchEntryType:1,CompressionType:3,Reserved:4,
      bb.writeShort(batchToPublish.batchSize());
      bb.writeInt(batchToPublish.uncompressedSizeInBytes());
      bb.writeInt(batchToPublish.sizeInBytes());
      batchToPublish.write(bb);
      return batchToPublish.batchSize();
    }

    @Override
    public int fragmentLength(Object entity) {
      return (8
          + 1
          + 2
          + 4
          + 4
          + ((EncodedMessageBatch) entity)
              .sizeInBytes()); // publish ID + info byte + message count + uncompressed data size +
      // data size
    }
  }

  public static class ShutdownContext {

    private final ShutdownReason shutdownReason;

    ShutdownContext(ShutdownReason shutdownReason) {
      this.shutdownReason = shutdownReason;
    }

    public ShutdownReason getShutdownReason() {
      return shutdownReason;
    }

    boolean isShutdownUnexpected() {
      return getShutdownReason() == ShutdownReason.HEARTBEAT_FAILURE
          || getShutdownReason() == ShutdownReason.UNKNOWN;
    }

    public enum ShutdownReason {
      CLIENT_CLOSE,
      SERVER_CLOSE,
      HEARTBEAT_FAILURE,
      UNKNOWN
    }
  }

  static class TuneState {

    private final CountDownLatch latch = new CountDownLatch(1);

    private final AtomicInteger maxFrameSize = new AtomicInteger();

    private final AtomicInteger heartbeat = new AtomicInteger();

    private final int requestedMaxFrameSize;

    private final int requestedHeartbeat;

    public TuneState(int requestedMaxFrameSize, int requestedHeartbeat) {
      this.requestedMaxFrameSize = requestedMaxFrameSize;
      this.requestedHeartbeat = requestedHeartbeat;
    }

    void await(Duration duration) {
      try {
        boolean completed = latch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
          throw new StreamException(
              "Waited for tune frame for " + duration.getSeconds() + " second(s)");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StreamException("Interrupted while waiting for tune frame");
      }
    }

    int getMaxFrameSize() {
      return maxFrameSize.get();
    }

    int getHeartbeat() {
      return heartbeat.get();
    }

    int requestedHeartbeat() {
      return requestedHeartbeat;
    }

    int requestedMaxFrameSize() {
      return requestedMaxFrameSize;
    }

    TuneState maxFrameSize(int maxFrameSize) {
      this.maxFrameSize.set(maxFrameSize);
      return this;
    }

    TuneState heartbeat(int heartbeat) {
      this.heartbeat.set(heartbeat);
      return this;
    }

    public void done() {
      latch.countDown();
    }
  }

  public static class Response {

    private final short responseCode;

    public Response(short responseCode) {
      this.responseCode = responseCode;
    }

    public boolean isOk() {
      return responseCode == RESPONSE_CODE_OK;
    }

    public short getResponseCode() {
      return responseCode;
    }

    @Override
    public String toString() {
      return formatConstant(this.responseCode);
    }
  }

  static class SaslAuthenticateResponse extends Response {

    private final byte[] challenge;

    public SaslAuthenticateResponse(short responseCode, byte[] challenge) {
      super(responseCode);
      this.challenge = challenge;
    }

    public boolean isChallenge() {
      return this.getResponseCode() == RESPONSE_CODE_SASL_CHALLENGE;
    }

    public boolean isAuthenticationFailure() {
      return this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE
          || this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK;
    }
  }

  static class OpenResponse extends Response {

    private final Map<String, String> connectionProperties;

    OpenResponse(short responseCode, Map<String, String> connectionProperties) {
      super(responseCode);
      this.connectionProperties = connectionProperties;
    }
  }

  public static class QueryOffsetResponse extends Response {

    private final long offset;

    public QueryOffsetResponse(short responseCode, long offset) {
      super(responseCode);
      this.offset = offset;
    }

    public long getOffset() {
      return offset;
    }
  }

  static class QueryPublisherSequenceResponse extends Response {

    private final long sequence;

    QueryPublisherSequenceResponse(short responseCode, long sequence) {
      super(responseCode);
      this.sequence = sequence;
    }

    public long getSequence() {
      return sequence;
    }
  }

  static class StreamStatsResponse extends Response {

    private final Map<String, Long> info;

    StreamStatsResponse(short responseCode, Map<String, Long> info) {
      super(responseCode);
      this.info = Collections.unmodifiableMap(new HashMap<>(info));
    }

    public Map<String, Long> getInfo() {
      return info;
    }
  }

  public static class StreamMetadata {

    private final String stream;

    private final short responseCode;

    private final Broker leader;

    private final List<Broker> replicas;

    public StreamMetadata(String stream, short responseCode, Broker leader, List<Broker> replicas) {
      this.stream = stream;
      this.responseCode = responseCode;
      this.leader = leader;
      this.replicas = replicas;
    }

    public short getResponseCode() {
      return responseCode;
    }

    public boolean isResponseOk() {
      return responseCode == RESPONSE_CODE_OK;
    }

    public Broker getLeader() {
      return leader;
    }

    public List<Broker> getReplicas() {
      return replicas;
    }

    public String getStream() {
      return stream;
    }

    @Override
    public String toString() {
      return "StreamMetadata{"
          + "stream='"
          + stream
          + '\''
          + ", responseCode="
          + responseCode
          + ", leader="
          + leader
          + ", replicas="
          + replicas
          + '}';
    }
  }

  public static class Broker {

    private final String host;

    private final int port;

    public Broker(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    @Override
    public String toString() {
      return "Broker{" + "host='" + host + '\'' + ", port=" + port + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Broker broker = (Broker) o;
      return port == broker.port && host.equals(broker.host);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port);
    }

    String label() {
      return this.host + ":" + this.port;
    }
  }

  public static class ClientParameters {

    private final Map<String, String> clientProperties = new ConcurrentHashMap<>();
    EventLoopGroup eventLoopGroup;
    private Codec codec;
    private String host = "localhost";
    private int port = DEFAULT_PORT;
    CompressionCodecFactory compressionCodecFactory;
    private String virtualHost = "/";
    private Duration requestedHeartbeat = Duration.ofSeconds(60);
    private int requestedMaxFrameSize = 1048576;
    private PublishConfirmListener publishConfirmListener = NO_OP_PUBLISH_CONFIRM_LISTENER;
    private PublishErrorListener publishErrorListener = NO_OP_PUBLISH_ERROR_LISTENER;
    private ChunkListener chunkListener =
        (client, correlationId, offset, messageCount, dataSize) -> {};
    private MessageListener messageListener =
        (correlationId, offset, chunkTimestamp, committedOffset, message) -> {};
    private MetadataListener metadataListener = (stream, code) -> {};
    private CreditNotification creditNotification =
        (subscriptionId, responseCode) ->
            LOGGER.warn(
                "Received notification for subscription {}: {}",
                subscriptionId,
                Utils.formatConstant(responseCode));
    private ConsumerUpdateListener consumerUpdateListener =
        (client, subscriptionId, active) -> null;
    private ShutdownListener shutdownListener = shutdownContext -> {};
    private SaslConfiguration saslConfiguration = DefaultSaslConfiguration.PLAIN;
    private CredentialsProvider credentialsProvider =
        new DefaultUsernamePasswordCredentialsProvider("guest", "guest");
    private ChunkChecksum chunkChecksum = JdkChunkChecksum.CRC32_SINGLETON;
    private MetricsCollector metricsCollector = NoOpMetricsCollector.SINGLETON;
    private SslContext sslContext;
    private boolean tlsHostnameVerification = true;
    private ByteBufAllocator byteBufAllocator;
    private Duration rpcTimeout;
    private Consumer<Channel> channelCustomizer = noOpConsumer();
    private Consumer<Bootstrap> bootstrapCustomizer = noOpConsumer();
    // for messages
    private ExecutorServiceFactory dispatchingExecutorServiceFactory;
    // for other server frames
    private ExecutorServiceFactory executorServiceFactory;

    public ClientParameters host(String host) {
      this.host = host;
      return this;
    }

    public ClientParameters port(int port) {
      this.port = port;
      return this;
    }

    public ClientParameters publishConfirmListener(PublishConfirmListener publishConfirmListener) {
      this.publishConfirmListener = publishConfirmListener;
      return this;
    }

    public ClientParameters publishErrorListener(PublishErrorListener publishErrorListener) {
      this.publishErrorListener = publishErrorListener;
      return this;
    }

    public ClientParameters chunkListener(ChunkListener chunkListener) {
      this.chunkListener = chunkListener;
      return this;
    }

    public ClientParameters messageListener(MessageListener messageListener) {
      this.messageListener = messageListener;
      return this;
    }

    public ClientParameters creditNotification(CreditNotification creditNotification) {
      this.creditNotification = creditNotification;
      return this;
    }

    public ClientParameters consumerUpdateListener(ConsumerUpdateListener consumerUpdateListener) {
      this.consumerUpdateListener = consumerUpdateListener;
      return this;
    }

    public ClientParameters codec(Codec codec) {
      this.codec = codec;
      return this;
    }

    public ClientParameters eventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
      return this;
    }

    public ClientParameters byteBufAllocator(ByteBufAllocator byteBufAllocator) {
      this.byteBufAllocator = byteBufAllocator;
      return this;
    }

    public ClientParameters saslConfiguration(SaslConfiguration saslConfiguration) {
      this.saslConfiguration = saslConfiguration;
      return this;
    }

    public ClientParameters credentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public ClientParameters username(String username) {
      if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
        this.credentialsProvider =
            new DefaultUsernamePasswordCredentialsProvider(
                username,
                ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getPassword());
      } else {
        this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(username, null);
      }
      return this;
    }

    public ClientParameters password(String password) {
      if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
        this.credentialsProvider =
            new DefaultUsernamePasswordCredentialsProvider(
                ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getUsername(),
                password);
      } else {
        this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(null, password);
      }
      return this;
    }

    public ClientParameters virtualHost(String virtualHost) {
      this.virtualHost = virtualHost;
      return this;
    }

    public ClientParameters requestedHeartbeat(Duration requestedHeartbeat) {
      this.requestedHeartbeat = requestedHeartbeat;
      return this;
    }

    public ClientParameters requestedMaxFrameSize(int requestedMaxFrameSize) {
      this.requestedMaxFrameSize = requestedMaxFrameSize;
      return this;
    }

    public ClientParameters chunkChecksum(ChunkChecksum chunkChecksum) {
      this.chunkChecksum = chunkChecksum;
      return this;
    }

    public ClientParameters clientProperties(Map<String, String> clientProperties) {
      this.clientProperties.putAll(clientProperties);
      return this;
    }

    public ClientParameters clientProperty(String key, String value) {
      this.clientProperties.put(key, value);
      return this;
    }

    public ClientParameters metricsCollector(MetricsCollector metricsCollector) {
      this.metricsCollector = metricsCollector;
      return this;
    }

    public ClientParameters metadataListener(MetadataListener metadataListener) {
      this.metadataListener = metadataListener;
      return this;
    }

    public ClientParameters shutdownListener(ShutdownListener shutdownListener) {
      this.shutdownListener = shutdownListener;
      return this;
    }

    public ClientParameters sslContext(SslContext sslContext) {
      this.sslContext = sslContext;
      if (this.port == DEFAULT_PORT && sslContext != null) {
        this.port = DEFAULT_TLS_PORT;
      }
      return this;
    }

    public ClientParameters tlsHostnameVerification(boolean tlsHostnameVerification) {
      this.tlsHostnameVerification = tlsHostnameVerification;
      return this;
    }

    public ClientParameters compressionCodecFactory(
        CompressionCodecFactory compressionCodecFactory) {
      this.compressionCodecFactory = compressionCodecFactory;
      return this;
    }

    public ClientParameters rpcTimeout(Duration rpcTimeout) {
      this.rpcTimeout = rpcTimeout;
      return this;
    }

    public ClientParameters dispatchingExecutorServiceFactory(
        ExecutorServiceFactory dispatchingExecutorServiceFactory) {
      this.dispatchingExecutorServiceFactory = dispatchingExecutorServiceFactory;
      return this;
    }

    public ClientParameters executorServiceFactory(ExecutorServiceFactory executorServiceFactory) {
      this.executorServiceFactory = executorServiceFactory;
      return this;
    }

    String host() {
      return this.host;
    }

    int port() {
      return this.port;
    }

    Map<String, String> clientProperties() {
      return Collections.unmodifiableMap(this.clientProperties);
    }

    Codec codec() {
      return this.codec;
    }

    public ClientParameters channelCustomizer(Consumer<Channel> channelCustomizer) {
      this.channelCustomizer = channelCustomizer;
      return this;
    }

    public ClientParameters bootstrapCustomizer(Consumer<Bootstrap> bootstrapCustomizer) {
      this.bootstrapCustomizer = bootstrapCustomizer;
      return this;
    }

    ClientParameters duplicate() {
      ClientParameters duplicate = new ClientParameters();
      for (Field field : ClientParameters.class.getDeclaredFields()) {
        field.setAccessible(true);
        try {
          Object value = field.get(this);
          if (value instanceof Map) {
            value = new ConcurrentHashMap<>((Map<?, ?>) value);
          }
          field.set(duplicate, value);
        } catch (IllegalAccessException e) {
          throw new StreamException("Error while duplicating client parameters", e);
        }
      }
      return duplicate;
    }
  }

  static class OutstandingRequest<T> {

    private final CountDownLatch latch = new CountDownLatch(1);

    private final Duration timeout;

    private final String node;

    private final AtomicReference<T> response = new AtomicReference<>();

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private OutstandingRequest(Duration timeout, String node) {
      this.timeout = timeout;
      this.node = node;
    }

    void block() {
      boolean completed;
      try {
        completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StreamException("Interrupted while waiting for response");
      }
      if (!completed) {
        throw new TimeoutStreamException(
            format("Could not get response in %d ms from node %s", timeout.toMillis(), node));
      }
    }

    void completeExceptionally(Throwable cause) {
      error.set(cause);
      latch.countDown();
    }

    Throwable error() {
      return error.get();
    }

    AtomicReference<T> response() {
      return response;
    }

    void countDown() {
      this.latch.countDown();
    }
  }

  static final class SubscriptionOffset {

    private final int subscriptionId;
    private final long offset;

    SubscriptionOffset(int subscriptionId, long offset) {
      this.subscriptionId = subscriptionId;
      this.offset = offset;
    }

    int subscriptionId() {
      return subscriptionId;
    }

    long offset() {
      return offset;
    }
  }

  public static class StreamParametersBuilder {

    private final Map<String, String> parameters = new HashMap<>();

    public StreamParametersBuilder maxLengthBytes(long bytes) {
      this.parameters.put("max-length-bytes", String.valueOf(bytes));
      return this;
    }

    public StreamParametersBuilder maxLengthBytes(ByteCapacity bytes) {
      return maxLengthBytes(bytes.toBytes());
    }

    public StreamParametersBuilder maxLengthKb(long kiloBytes) {
      return maxLengthBytes(kiloBytes * 1000);
    }

    public StreamParametersBuilder maxLengthMb(long megaBytes) {
      return maxLengthBytes(megaBytes * 1000 * 1000);
    }

    public StreamParametersBuilder maxLengthGb(long gigaBytes) {
      return maxLengthBytes(gigaBytes * 1000 * 1000 * 1000);
    }

    public StreamParametersBuilder maxLengthTb(long teraBytes) {
      return maxLengthBytes(teraBytes * 1000 * 1000 * 1000 * 1000);
    }

    public StreamParametersBuilder maxSegmentSizeBytes(long bytes) {
      this.parameters.put("stream-max-segment-size-bytes", String.valueOf(bytes));
      return this;
    }

    public StreamParametersBuilder maxSegmentSizeBytes(ByteCapacity bytes) {
      return maxSegmentSizeBytes(bytes.toBytes());
    }

    public StreamParametersBuilder maxSegmentSizeKb(long kiloBytes) {
      return maxSegmentSizeBytes(kiloBytes * 1000);
    }

    public StreamParametersBuilder maxSegmentSizeMb(long megaBytes) {
      return maxSegmentSizeBytes(megaBytes * 1000 * 1000);
    }

    public StreamParametersBuilder maxSegmentSizeGb(long gigaBytes) {
      return maxSegmentSizeBytes(gigaBytes * 1000 * 1000 * 1000);
    }

    public StreamParametersBuilder maxSegmentSizeTb(long teraBytes) {
      return maxSegmentSizeBytes(teraBytes * 1000 * 1000 * 1000 * 1000);
    }

    public StreamParametersBuilder maxAge(Duration duration) {
      if (duration == null
          || duration.isZero()
          || duration.isNegative()
          || duration.getSeconds() < 0) {
        throw new IllegalArgumentException("Max age must be a positive duration");
      }
      this.parameters.put("max-age", duration.getSeconds() + "s");
      return this;
    }

    public StreamParametersBuilder leaderLocator(LeaderLocator leaderLocator) {
      this.parameters.put("queue-leader-locator", leaderLocator.value());
      return this;
    }

    public StreamParametersBuilder put(String key, String value) {
      parameters.put(key, value);
      return this;
    }

    public Map<String, String> build() {
      return parameters;
    }
  }

  private class StreamHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf m = (ByteBuf) msg;
      metricsCollector.readBytes(m.capacity() + 4); // 32-bits integer for size not included
      int frameSize = m.readableBytes();
      short commandId = extractResponseCode(m.readShort());
      short version = m.readShort();
      Runnable task;
      if (closing.get()) {
        if (commandId == COMMAND_CLOSE) {
          task = () -> ServerFrameHandler.defaultHandler().handle(Client.this, frameSize, ctx, m);
        } else {
          LOGGER.debug("Ignoring command {} from server while closing", commandId);
          try {
            while (m.isReadable()) {
              m.readByte();
            }
          } finally {
            m.release();
          }
          task = null;
        }
      } else {
        FrameHandler frameHandler = ServerFrameHandler.lookup(commandId, version, m);
        task = () -> frameHandler.handle(Client.this, frameSize, ctx, m);
      }

      if (task != null) {
        if (commandId == Constants.COMMAND_DELIVER) {
          dispatchingExecutorService.submit(task);
        } else {
          executorService.submit(task);
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      LOGGER.debug("Netty channel became inactive");
      // the TCP connection can get closed by server after a SERVER_CLOSE is sent
      // back from the client. The connection can then get inactive before
      // the event is actually dispatched to the listener, emitting
      // an UNKNOWN reason instead of SERVER_CLOSE. So we skip the closing here
      // because it will be handled later anyway.
      if (shutdownReason == null) {
        if (closing.compareAndSet(false, true)) {
          executorService.submit(() -> closingSequence(ShutdownReason.UNKNOWN));
        }
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE) {
          LOGGER.info("Closing connection because it's been idle for too long");
          closing.set(true);
          closingSequence(ShutdownContext.ShutdownReason.HEARTBEAT_FAILURE);
        } else if (e.state() == IdleState.WRITER_IDLE) {
          LOGGER.debug("Sending heartbeat frame");
          ByteBuf bb = allocate(ctx.alloc(), 4 + 2 + 2);
          bb.writeInt(4).writeShort(encodeRequestCode(COMMAND_HEARTBEAT)).writeShort(VERSION_1);
          ctx.writeAndFlush(bb);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof DecoderException && cause.getCause() instanceof SSLHandshakeException) {
        LOGGER.debug("Error during TLS handshake");
        // likely to be an error during the handshake, there should be only one outstanding request
        if (outstandingRequests.size() == 1) {
          // the response may have arrived in between, making a copy
          List<OutstandingRequest<?>> requests = new ArrayList<>(outstandingRequests.values());
          if (requests.size() == 1) {
            OutstandingRequest<?> outstandingRequest = requests.get(0);
            outstandingRequest.completeExceptionally(cause.getCause());
          }
        } else {
          LOGGER.debug("More than 1 outstanding request: {}", outstandingRequests);
        }
      }
      LOGGER.warn("Error in stream handler", cause);
      ctx.close();
    }
  }

  private <T> OutstandingRequest<T> outstandingRequest() {
    return new OutstandingRequest<>(this.rpcTimeout, this.host + ":" + this.port);
  }
}

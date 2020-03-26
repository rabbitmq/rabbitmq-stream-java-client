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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.rabbitmq.stream.Constants.*;

public class Client implements AutoCloseable {

    public static final int DEFAULT_PORT = 5555;

    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(10);

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final ConfirmListener confirmListener;

    private final PublishErrorListener publishErrorListener;

    private final ChunkListener chunkListener;

    private final RecordListener recordListener;

    private final SubscriptionListener subscriptionListener;

    private final Codec codec;

    private final Channel channel;

    private final AtomicLong publishSequence = new AtomicLong(0);

    private final AtomicInteger correlationSequence = new AtomicInteger(0);

    private final ConcurrentMap<Integer, OutstandingRequest> outstandingRequests = new ConcurrentHashMap<>();

    private final List<SubscriptionOffset> subscriptionOffsets = new CopyOnWriteArrayList<>();

    private final Subscriptions subscriptions = new Subscriptions();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final SaslConfiguration saslConfiguration;

    private final CredentialsProvider credentialsProvider;

    private final CountDownLatch tuneLatch = new CountDownLatch(1);

    private final AtomicBoolean closing = new AtomicBoolean(false);

    public Client() {
        this(new ClientParameters());
    }

    public Client(ClientParameters parameters) {
        this.confirmListener = parameters.confirmListener;
        this.publishErrorListener = parameters.publishErrorListener;
        this.chunkListener = parameters.chunkListener;
        this.recordListener = parameters.recordListener;
        this.subscriptionListener = parameters.subscriptionListener;
        this.codec = parameters.codec;
        this.saslConfiguration = parameters.saslConfiguration;
        this.credentialsProvider = parameters.credentialsProvider;

        Bootstrap b = new Bootstrap();
        b.group(parameters.eventLoopGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        // is that the default?
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline().addFirst(new FlushConsolidationHandler(FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(
                        Integer.MAX_VALUE, 0, 4, 0, 4));
                ch.pipeline().addLast(new StreamHandler());
            }

        });

        ChannelFuture f = null;
        try {
            f = b.connect(parameters.host, parameters.port).sync();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }

        this.channel = f.channel();
        authenticate();
        try {
            boolean completed = this.tuneLatch.await(10, TimeUnit.SECONDS);
            if (!completed) {
                throw new ClientException("Waited for tune frame for 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Interrupted while waiting for tune frame");
        }
        open(parameters.virtualHost);
    }

    static void handleMetadataUpdate(ByteBuf bb, int frameSize, Subscriptions subscriptions, SubscriptionListener subscriptionListener) {
        int read = 2 + 2; // already read the command id and version
        short code = bb.readShort();
        read += 2;
        if (code == RESPONSE_CODE_TARGET_DELETED) {
            String target = readString(bb);
            read += (2 + target.length());
            LOGGER.info("Target {} has been deleted", target);
            List<Integer> subscriptionIds = subscriptions.removeSubscriptionsFor(target);
            for (Integer subscriptionId : subscriptionIds) {
                subscriptionListener.subscriptionCancelled(subscriptionId, target, RESPONSE_CODE_TARGET_DELETED);
            }

        } else {
            throw new IllegalArgumentException("Unsupported metadata update code " + code);
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;
        short responseCode = bb.readShort();
        read += 2;

        OutstandingRequest<Response> outstandingRequest = remove(outstandingRequests, correlationId, Response.class);
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            Response response = new Response(responseCode);
            outstandingRequest.response.set(response);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleSaslHandshakeResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        short responseCode = bb.readShort();
        read += 2;
        if (responseCode != RESPONSE_CODE_OK) {
            if (read != frameSize) {
                bb.readBytes(new byte[frameSize - read]);
            }
            // FIXME: should we unlock the request and notify that there's something wrong?
            throw new ClientException("Unexpected response code for SASL handshake response: " + responseCode);
        }


        int mechanismsCount = bb.readInt();

        read += 4;
        List<String> mechanisms = new ArrayList<>(mechanismsCount);
        for (int i = 0; i < mechanismsCount; i++) {
            String mechanism = readString(bb);
            mechanisms.add(mechanism);
            read += 2 + mechanism.length();
        }

        OutstandingRequest<List<String>> outstandingRequest = remove(outstandingRequests, correlationId, new ParameterizedTypeReference<List<String>>() {
        });
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(mechanisms);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleSaslAuthenticateResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        short responseCode = bb.readShort();
        read += 2;

        byte[] challenge;
        if (responseCode == RESPONSE_CODE_SASL_CHALLENGE) {
            int challengeSize = bb.readInt();
            read += 4;
            challenge = new byte[challengeSize];
            bb.readBytes(challenge);
            read += challenge.length;
        } else {
            challenge = null;
        }

        SaslAuthenticateResponse response = new SaslAuthenticateResponse(responseCode, challenge);

        OutstandingRequest<SaslAuthenticateResponse> outstandingRequest = remove(outstandingRequests, correlationId, SaslAuthenticateResponse.class);
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(response);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private static void handleTune(ByteBuf bb, int frameSize, ChannelHandlerContext ctx, CountDownLatch latch) {
        int read = 2 + 2; // already read the command id and version
        long maxFrameSize = bb.readLong();
        read += 8;
        short heartbeat = bb.readShort();
        read += 2;

        int length = 2 + 2 + 8 + 2;
        ByteBuf byteBuf = ctx.alloc().buffer(length + 4);
        byteBuf.writeInt(length)
                .writeShort(COMMAND_TUNE).writeShort(VERSION_0)
                .writeLong(maxFrameSize).writeShort(heartbeat);
        ctx.writeAndFlush(byteBuf);

        // FIXME take frame max size and heartbeat into account

        latch.countDown();

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> OutstandingRequest<T> remove(ConcurrentMap<Integer, OutstandingRequest> outstandingRequests, int correlationId, ParameterizedTypeReference<T> type) {
        return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
    }

    @SuppressWarnings("unchecked")
    private static <T> OutstandingRequest<T> remove(ConcurrentMap<Integer, OutstandingRequest> outstandingRequests, int correlationId, Class<T> clazz) {
        return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
    }

    static void handleMetadata(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        Map<Short, Broker> brokers = new HashMap<>();
        int brokersCount = bb.readInt();
        read += 4;
        for (int i = 0; i < brokersCount; i++) {
            short brokerReference = bb.readShort();
            read += 2;
            String host = readString(bb);
            read += 2 + host.length();
            int port = bb.readInt();
            read += 4;
            brokers.put(brokerReference, new Broker(host, port));
        }

        int targetsCount = bb.readInt();
        Map<String, TargetMetadata> results = new LinkedHashMap<>(targetsCount);
        read += 4;
        for (int i = 0; i < targetsCount; i++) {
            String target = readString(bb);
            read += 2 + target.length();
            short responseCode = bb.readShort();
            read += 2;
            short leaderReference = bb.readShort();
            read += 2;
            int replicasCount = bb.readInt();
            read += 4;
            Collection<Broker> replicas;
            if (replicasCount == 0) {
                replicas = Collections.emptyList();
            } else {
                replicas = new ArrayList<>(replicasCount);
                for (int j = 0; j < replicasCount; j++) {
                    short replicaReference = bb.readShort();
                    read += 2;
                    replicas.add(brokers.get(replicaReference));
                }
            }
            TargetMetadata targetMetadata = new TargetMetadata(target, responseCode, brokers.get(leaderReference), replicas);
            results.put(target, targetMetadata);
        }

        OutstandingRequest<Map<String, TargetMetadata>> outstandingRequest = remove(outstandingRequests, correlationId,
                new ParameterizedTypeReference<Map<String, TargetMetadata>>() {
                });
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(results);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private static String readString(ByteBuf bb) {
        short size = bb.readShort();
        byte[] bytes = new byte[size];
        bb.readBytes(bytes);
        String string = new String(bytes, StandardCharsets.UTF_8);
        return string;
    }

    static void handleDeliver(ByteBuf bb, Client client, ChunkListener chunkListener, RecordListener recordListener, int frameSize, Codec codec, List<SubscriptionOffset> subscriptionOffsets) {
        int read = 2 + 2; // already read the command id and version
        int subscriptionId = bb.readInt();
        read += 4;
/*
%% <<
%%   Magic=5:4/unsigned,
%%   ProtoVersion:4/unsigned,
%%   NumEntries:16/unsigned, %% need some kind of limit on chunk sizes 64k is a good start
%%   NumRecords:32/unsigned, %% total including all sub batch entries
%%   Epoch:64/unsigned,
%%   ChunkFirstOffset:64/unsigned,
%%   ChunkCrc:32/integer, %% CRC for the records portion of the data
%%   DataLength:32/unsigned, %% length until end of chunk
%%   [Entry]
%%   ...>>
 */
        // FIXME handle magic and version
        byte magicAndVersion = bb.readByte();
        read += 1;

        // FIXME handle unsigned
        short numEntries = bb.readShort();
        read += 2;
        int numRecords = bb.readInt();
        read += 4;
        long epoch = bb.readLong();
        read += 8;
        long offset = bb.readLong();
        read += 8;
        int crc = bb.readInt();
        read += 4;
        int dataLength = bb.readInt();
        read += 4;

        chunkListener.handle(client, subscriptionId, offset, numRecords, dataLength);

        boolean filter = false;
        long offsetLimit = -1;
        if (!subscriptionOffsets.isEmpty()) {
            Iterator<SubscriptionOffset> iterator = subscriptionOffsets.iterator();
            while (iterator.hasNext()) {
                SubscriptionOffset subscriptionOffset = iterator.next();
                if (subscriptionOffset.subscriptionId == subscriptionId) {
                    subscriptionOffsets.remove(subscriptionOffset);
                    filter = true;
                    offsetLimit = subscriptionOffset.offset;
                    break;
                }
            }
        }

        byte[] data;
        while (numRecords != 0) {
                /*
%%   <<0=SimpleEntryType:1,
%%     Size:31/unsigned,
%%     Data:Size/binary>> |
%%
%%   <<1=SubBatchEntryType:1,
%%     CompressionType:3,
%%     Reserved:4,
%%     NumRecords:16/unsigned,
%%     Size:32/unsigned,
%%     Data:Size/binary>>
                 */

            // FIXME deal with other type of entry than simple (first bit = 0)
            int typeAndSize = bb.readInt();
            read += 4;

            data = new byte[typeAndSize];
            bb.readBytes(data);
            read += typeAndSize;

            if (filter && offset < offsetLimit) {
                // filter
            } else {
                Message message = codec.decode(data);
                recordListener.handle(subscriptionId, offset, message);
            }
            numRecords--;
            offset++;
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleConfirm(ByteBuf bb, ConfirmListener confirmListener, int frameSize) {
        int read = 4; // already read the command id and version
        int publishingIdCount = bb.readInt();
        read += 4;
        long publishingId = -1;
        while (publishingIdCount != 0) {
            publishingId = bb.readLong();
            read += 8;
            confirmListener.handle(publishingId);
            publishingIdCount--;
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handlePublishError(ByteBuf bb, PublishErrorListener publishErrorListener, int frameSize) {
        int read = 4; // already read the command id and version
        long publishingErrorCount = bb.readInt();
        read += 4;
        long publishingId = -1;
        short code = -1;
        while (publishingErrorCount != 0) {
            publishingId = bb.readLong();
            read += 8;
            code = bb.readShort();
            read += 2;
            publishErrorListener.handle(publishingId, code);
            publishingErrorCount--;
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void handleClose(ByteBuf bb, int frameSize, ChannelHandlerContext ctx) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;
        short closeCode = bb.readShort();
        read += 2;
        String closeReason = readString(bb);
        read += 2 + closeReason.length();

        LOGGER.info("Received close from server, reason: {} {}", closeCode, closeReason);

        int length = 2 + 2 + 4 + 2;
        ByteBuf byteBuf = ctx.alloc().buffer(length + 4);
        byteBuf.writeInt(length)
                .writeShort(COMMAND_CLOSE).writeShort(VERSION_0)
                .writeInt(correlationId).writeShort(RESPONSE_CODE_OK);
        ctx.writeAndFlush(byteBuf);

        if (closing.compareAndSet(false, true)) {
            closeNetty();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void authenticate() {
        List<String> saslMechanisms = getSaslMechanisms();
        SaslMechanism saslMechanism = this.saslConfiguration.getSaslMechanism(saslMechanisms);

        byte[] challenge = null;
        boolean authDone = false;
        while (!authDone) {
            byte[] saslResponse = saslMechanism.handleChallenge(challenge, this.credentialsProvider);
            SaslAuthenticateResponse saslAuthenticateResponse = sendSaslAuthenticate(saslMechanism, saslResponse);
            if (saslAuthenticateResponse.isOk()) {
                authDone = true;
            } else if (saslAuthenticateResponse.isChallenge()) {
                challenge = saslAuthenticateResponse.challenge;
            } else if (saslAuthenticateResponse.isAuthenticationFailure()) {
                throw new AuthenticationFailureException("Unexpected response code during authentication: " + saslAuthenticateResponse.getResponseCode());
            } else {
                throw new ClientException("Unexpected response code during authentication: " + saslAuthenticateResponse.getResponseCode());
            }
        }
    }

    private SaslAuthenticateResponse sendSaslAuthenticate(SaslMechanism saslMechanism, byte[] challengeResponse) {
        int length = 2 + 2 + 4 + 2 + saslMechanism.getName().length() +
                4 + (challengeResponse == null ? 0 : challengeResponse.length);
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SASL_AUTHENTICATE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(saslMechanism.getName().length());
            bb.writeBytes(saslMechanism.getName().getBytes(StandardCharsets.UTF_8));
            if (challengeResponse == null) {
                bb.writeInt(-1);
            } else {
                bb.writeInt(challengeResponse.length).writeBytes(challengeResponse);
            }
            OutstandingRequest<SaslAuthenticateResponse> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private void open(String virtualHost) {
        int length = 2 + 2 + 4 + 2 + virtualHost.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_OPEN);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(virtualHost.length());
            bb.writeBytes(virtualHost.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            if (!request.response.get().isOk()) {
                throw new ClientException("Unexpected response code when connecting to virtual host: " + request.response.get().getResponseCode());
            }
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    // for testing
    void send(byte[] content) {
        ByteBuf bb = allocate(content.length);
        bb.writeBytes(content);
        try {
            channel.writeAndFlush(bb).sync();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    private void sendClose(short code, String reason) {
        int length = 2 + 2 + 4 + 2 + 2 + reason.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_CLOSE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(code);
            bb.writeShort(reason.length());
            bb.writeBytes(reason.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            if (!request.response.get().isOk()) {
                LOGGER.warn("Unexpected response code when closing: {}", request.response.get().getResponseCode());
                throw new ClientException("Unexpected response code when closing: " + request.response.get().getResponseCode());
            }
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private List<String> getSaslMechanisms() {
        int length = 2 + 2 + 4;
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SASL_HANDSHAKE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            OutstandingRequest<List<String>> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Response create(String target) {
        int length = 2 + 2 + 4 + 2 + target.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_CREATE_TARGET);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(target.length());
            bb.writeBytes(target.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private ByteBuf allocate(int capacity) {
        return channel.alloc().buffer(capacity);
    }

    public Response delete(String target) {
        int length = 2 + 2 + 4 + 2 + target.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_DELETE_TARGET);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(target.length());
            bb.writeBytes(target.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Map<String, TargetMetadata> metadata(String... targets) {
        if (targets == null || targets.length == 0) {
            throw new IllegalArgumentException("At least one target must be specified");
        }
        int length = 2 + 2 + 4 + 4; // API code, version, correlation ID, size of array
        for (String target : targets) {
            length += 2;
            length += target.length();
        }
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_METADATA);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(targets.length);
            for (String target : targets) {
                bb.writeShort(target.length());
                bb.writeBytes(target.getBytes(StandardCharsets.UTF_8));
            }
            OutstandingRequest<Map<String, TargetMetadata>> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public List<Long> publish(String target, List<Message> messages) {
        List<Codec.EncodedMessage> encodedMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            encodedMessages.add(codec.encode(message));
        }
        return publishInternal(target, encodedMessages);
    }

    public long publish(String target, Message message) {
        return publishInternal(target, Collections.singletonList(codec.encode(message))).get(0);
    }

    public List<Long> publishBinary(String target, List<byte[]> messages) {
        List<Codec.EncodedMessage> encodedMessages = new ArrayList<>(messages.size());
        for (byte[] message : messages) {
            encodedMessages.add(codec.encode(new BinaryOnlyMessage(message)));
        }
        return publishInternal(target, encodedMessages);
    }

    public long publish(String target, byte[] data) {
        Codec.EncodedMessage encodedMessage = codec.encode(new BinaryOnlyMessage(data));
        return publishInternal(target, Collections.singletonList(encodedMessage)).get(0);
    }

    private List<Long> publishInternal(String target, List<Codec.EncodedMessage> encodedMessages) {
        int length = 2 + 2 + 2 + target.length() + 4;
        for (Codec.EncodedMessage encodedMessage : encodedMessages) {
            length += (8 + 4 + encodedMessage.getSize());
        }

        List<Long> sequences = new ArrayList<>(encodedMessages.size());
        ByteBuf out = allocate(length + 4);
        out.writeInt(length);
        out.writeShort(COMMAND_PUBLISH);
        out.writeShort(VERSION_0);
        out.writeShort(target.length());
        out.writeBytes(target.getBytes(StandardCharsets.UTF_8));
        out.writeInt(encodedMessages.size());
        for (Codec.EncodedMessage encodedMessage : encodedMessages) {
            long sequence = publishSequence.getAndIncrement();
            out.writeLong(sequence);
            out.writeInt(encodedMessage.getSize());
            out.writeBytes(encodedMessage.getData(), 0, encodedMessage.getSize());
            sequences.add(sequence);
        }
        channel.writeAndFlush(out);
        return sequences;
    }

    public void credit(int subscriptionId, int credit) {
        int length = 2 + 2 + 4 + 2;

        ByteBuf bb = allocate(length + 4);
        bb.writeInt(length);
        bb.writeShort(COMMAND_CREDIT);
        bb.writeShort(VERSION_0);
        bb.writeInt(subscriptionId);
        bb.writeShort((short) credit);
        channel.writeAndFlush(bb);
    }

    public Response subscribe(int subscriptionId, String target, long offset, int credit) {
        int length = 2 + 2 + 4 + 4 + 2 + target.length() + 8 + 2;
        int correlationId = correlationSequence.getAndIncrement();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SUBSCRIBE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(subscriptionId);
            bb.writeShort(target.length());
            bb.writeBytes(target.getBytes(StandardCharsets.UTF_8));
            bb.writeLong(offset);
            bb.writeShort((short) credit);
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            if (offset > 0) {
                subscriptionOffsets.add(new SubscriptionOffset(subscriptionId, offset));
            }
            channel.writeAndFlush(bb);
            request.block();
            subscriptions.add(target, subscriptionId);
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Response unsubscribe(int subscriptionId) {
        int length = 2 + 2 + 4 + 4;
        int correlationId = correlationSequence.getAndIncrement();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_UNSUBSCRIBE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(subscriptionId);
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            subscriptions.remove(subscriptionId);
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public void close() {
        if (closing.compareAndSet(false, true)) {
            LOGGER.debug("Closing client");

            sendClose(RESPONSE_CODE_OK, "OK");

            closeNetty();

            LOGGER.debug("Client closed");
        }
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
    }

    public boolean isOpen() {
        return !closing.get();
    }

    public interface ConfirmListener {

        void handle(long publishingId);

    }


    public interface PublishErrorListener {

        void handle(long publishingId, short responseCode);

    }

    public interface SubscriptionListener {

        void subscriptionCancelled(int subscriptionId, String target, short reason);

    }

    public interface ChunkListener {

        void handle(Client client, int subscriptionId, long offset, int recordCount, int dataSize);

    }


    public interface RecordListener {

        void handle(int subscriptionId, long offset, Message message);

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
    }

    private static class SaslAuthenticateResponse extends Response {

        private final byte[] challenge;

        public SaslAuthenticateResponse(short responseCode, byte[] challenge) {
            super(responseCode);
            this.challenge = challenge;
        }

        public boolean isChallenge() {
            return this.getResponseCode() == RESPONSE_CODE_SASL_CHALLENGE;
        }

        public boolean isAuthenticationFailure() {
            return this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE ||
                    this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK;
        }
    }

    public static class TargetMetadata {

        private final String target;

        private final short responseCode;

        private final Broker leader;

        private final Collection<Broker> replicas;

        public TargetMetadata(String target, short responseCode, Broker leader, Collection<Broker> replicas) {
            this.target = target;
            this.responseCode = responseCode;
            this.leader = leader;
            this.replicas = replicas;
        }

        public short getResponseCode() {
            return responseCode;
        }

        public Broker getLeader() {
            return leader;
        }

        public Collection<Broker> getReplicas() {
            return replicas;
        }

        public String getTarget() {
            return target;
        }

        @Override
        public String toString() {
            return "TargetMetadata{" +
                    "target='" + target + '\'' +
                    ", responseCode=" + responseCode +
                    ", leader=" + leader +
                    ", replicas=" + replicas +
                    '}';
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
            return "Broker{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static class ClientParameters {

        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private String virtualHost = "/";

        private ConfirmListener confirmListener = (publishingId) -> {

        };

        private PublishErrorListener publishErrorListener = (publishingId, responseCode) -> {

        };

        private ChunkListener chunkListener = (client, correlationId, offset, recordCount, dataSize) -> {

        };

        private RecordListener recordListener = (correlationId, offset, message) -> {

        };

        private SubscriptionListener subscriptionListener = (subscriptionId, target, reason) -> {

        };

        private Codec codec = new SwiftMqCodec();

        // TODO eventloopgroup should be shared between clients, this could justify the introduction of client factory
        private EventLoopGroup eventLoopGroup;

        private SaslConfiguration saslConfiguration = DefaultSaslConfiguration.PLAIN;

        private CredentialsProvider credentialsProvider = new DefaultUsernamePasswordCredentialsProvider("guest", "guest");

        public ClientParameters host(String host) {
            this.host = host;
            return this;
        }

        public ClientParameters port(int port) {
            this.port = port;
            return this;
        }

        public ClientParameters confirmListener(ConfirmListener confirmListener) {
            this.confirmListener = confirmListener;
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

        public ClientParameters recordListener(RecordListener recordListener) {
            this.recordListener = recordListener;
            return this;
        }

        public ClientParameters subscriptionListener(SubscriptionListener subscriptionListener) {
            this.subscriptionListener = subscriptionListener;
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
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(
                        username,
                        ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getPassword()
                );
            } else {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(username, null);
            }
            return this;
        }

        public ClientParameters password(String password) {
            if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(
                        ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getUsername(),
                        password
                );
            } else {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(null, password);
            }
            return this;
        }

        public ClientParameters virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }
    }

    private static final class BinaryOnlyMessage implements Message {

        private final byte[] body;

        private BinaryOnlyMessage(byte[] body) {
            this.body = body;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return body;
        }

        @Override
        public Object getBody() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Properties getProperties() {
            return null;
        }

        @Override
        public Map<String, Object> getApplicationProperties() {
            return null;
        }
    }

    private static class OutstandingRequest<T> {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final Duration timeout;

        private final AtomicReference<T> response = new AtomicReference<>();

        private OutstandingRequest(Duration timeout) {
            this.timeout = timeout;
        }

        void block() {
            boolean completed;
            try {
                completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClientException("Interrupted while waiting for response");
            }
            if (!completed) {
                throw new ClientException("Could not get response in " + timeout.toMillis() + " ms");
            }
        }

    }

    private static final class SubscriptionOffset {

        private final int subscriptionId;
        private final long offset;

        private SubscriptionOffset(int subscriptionId, long offset) {
            this.subscriptionId = subscriptionId;
            this.offset = offset;
        }
    }

    private static class Subscriptions {

        private Map<String, List<Integer>> targetsToSubscriptions = new ConcurrentHashMap<>();

        void add(String target, int subscriptionId) {
            List<Integer> subscriptionIds = targetsToSubscriptions.compute(target, (k, v) -> v == null ? new CopyOnWriteArrayList<>() : v);
            subscriptionIds.add(subscriptionId);
        }

        void remove(int subscriptionId) {
            Iterator<Map.Entry<String, List<Integer>>> iterator = targetsToSubscriptions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<Integer>> entry = iterator.next();
                boolean removed = entry.getValue().remove((Object) subscriptionId);
                if (removed) {
                    if (entry.getValue().isEmpty()) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        List<Integer> removeSubscriptionsFor(String target) {
            List<Integer> subscriptionIds = targetsToSubscriptions.remove(target);
            return subscriptionIds == null ? Collections.emptyList() : subscriptionIds;
        }

    }

    private class StreamHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg;

            int frameSize = m.readableBytes();
            short commandId = m.readShort();
            short version = m.readShort();
            if (version != VERSION_0) {
                throw new ClientException("Unsupported version " + version + " for command " + commandId);
            }
            Runnable task;
            if (closing.get()) {
                if (commandId == COMMAND_CLOSE) {
                    task = () -> handleResponse(m, frameSize, outstandingRequests);
                } else {
                    LOGGER.warn("Ignoring command {} from server while closing", commandId);
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
                if (commandId == COMMAND_PUBLISH_CONFIRM) {
                    task = () -> handleConfirm(m, confirmListener, frameSize);
                } else if (commandId == COMMAND_DELIVER) {
                    task = () -> handleDeliver(m, Client.this, chunkListener, recordListener, frameSize, codec, subscriptionOffsets);
                } else if (commandId == COMMAND_PUBLISH_ERROR) {
                    task = () -> handlePublishError(m, publishErrorListener, frameSize);
                } else if (commandId == COMMAND_METADATA_UPDATE) {
                    task = () -> handleMetadataUpdate(m, frameSize, subscriptions, subscriptionListener);
                } else if (commandId == COMMAND_METADATA) {
                    task = () -> handleMetadata(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_SASL_HANDSHAKE) {
                    task = () -> handleSaslHandshakeResponse(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_SASL_AUTHENTICATE) {
                    task = () -> handleSaslAuthenticateResponse(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_TUNE) {
                    task = () -> handleTune(m, frameSize, ctx, tuneLatch);
                } else if (commandId == COMMAND_CLOSE) {
                    task = () -> handleClose(m, frameSize, ctx);
                } else if (commandId == COMMAND_SUBSCRIBE || commandId == COMMAND_UNSUBSCRIBE
                        || commandId == COMMAND_CREATE_TARGET || commandId == COMMAND_DELETE_TARGET
                        || commandId == COMMAND_OPEN) {
                    task = () -> handleResponse(m, frameSize, outstandingRequests);
                } else {
                    throw new ClientException("Unsupported command " + commandId);
                }
            }

            if (task != null) {
                executorService.submit(() -> {
                    try {
                        task.run();
                    } catch (Exception e) {
                        LOGGER.warn("Error while handling response from server", e);
                    } finally {
                        m.release();
                    }
                });
            }


        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            LOGGER.debug("Netty channel became inactive", closing.get());
            closing.set(true);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.warn("Error in stream handler", cause);
            ctx.close();
        }
    }


}

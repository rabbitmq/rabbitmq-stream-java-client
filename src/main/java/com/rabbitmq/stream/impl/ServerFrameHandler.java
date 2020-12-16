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

import static com.rabbitmq.stream.Constants.COMMAND_CLOSE;
import static com.rabbitmq.stream.Constants.COMMAND_CREATE_STREAM;
import static com.rabbitmq.stream.Constants.COMMAND_CREDIT;
import static com.rabbitmq.stream.Constants.COMMAND_DECLARE_PUBLISHER;
import static com.rabbitmq.stream.Constants.COMMAND_DELETE_PUBLISHER;
import static com.rabbitmq.stream.Constants.COMMAND_DELETE_STREAM;
import static com.rabbitmq.stream.Constants.COMMAND_DELIVER;
import static com.rabbitmq.stream.Constants.COMMAND_HEARTBEAT;
import static com.rabbitmq.stream.Constants.COMMAND_METADATA;
import static com.rabbitmq.stream.Constants.COMMAND_METADATA_UPDATE;
import static com.rabbitmq.stream.Constants.COMMAND_OPEN;
import static com.rabbitmq.stream.Constants.COMMAND_PEER_PROPERTIES;
import static com.rabbitmq.stream.Constants.COMMAND_PUBLISH_CONFIRM;
import static com.rabbitmq.stream.Constants.COMMAND_PUBLISH_ERROR;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_OFFSET;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_PUBLISHER_SEQUENCE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_AUTHENTICATE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_HANDSHAKE;
import static com.rabbitmq.stream.Constants.COMMAND_SUBSCRIBE;
import static com.rabbitmq.stream.Constants.COMMAND_TUNE;
import static com.rabbitmq.stream.Constants.COMMAND_UNSUBSCRIBE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_OK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SASL_CHALLENGE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE;
import static com.rabbitmq.stream.Constants.VERSION_0;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.ChunkChecksumValidationException;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ChunkListener;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.OutstandingRequest;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.Client.QueryPublisherSequenceResponse;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.SaslAuthenticateResponse;
import com.rabbitmq.stream.impl.Client.ShutdownContext;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import com.rabbitmq.stream.impl.Client.SubscriptionOffset;
import com.rabbitmq.stream.metrics.MetricsCollector;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ServerFrameHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerFrameHandler.class);
  private static final FrameHandler RESPONSE_FRAME_HANDLER = new ResponseFrameHandler();

  private static final FrameHandler[] HANDLERS;

  static {
    Map<Short, FrameHandler> handlers = new HashMap<>();
    handlers.put(COMMAND_CLOSE, new CloseFrameHandler());
    handlers.put(COMMAND_SUBSCRIBE, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_UNSUBSCRIBE, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DECLARE_PUBLISHER, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DELETE_PUBLISHER, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_CREATE_STREAM, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DELETE_STREAM, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_OPEN, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_PUBLISH_CONFIRM, new ConfirmFrameHandler());
    handlers.put(COMMAND_DELIVER, new DeliverFrameHandler());
    handlers.put(COMMAND_PUBLISH_ERROR, new PublishErrorHandler());
    handlers.put(COMMAND_METADATA_UPDATE, new MetadataUpdateFrameHandler());
    handlers.put(COMMAND_METADATA, new MetadataFrameHandler());
    handlers.put(COMMAND_SASL_HANDSHAKE, new SaslHandshakeFrameHandler());
    handlers.put(COMMAND_SASL_AUTHENTICATE, new SaslAuthenticateFrameHandler());
    handlers.put(COMMAND_TUNE, new TuneFrameHandler());
    handlers.put(COMMAND_HEARTBEAT, new HeartbeatFrameHandler());
    handlers.put(COMMAND_PEER_PROPERTIES, new PeerPropertiesFrameHandler());
    handlers.put(COMMAND_CREDIT, new CreditNotificationFrameHandler());
    handlers.put(COMMAND_QUERY_OFFSET, new QueryOffsetFrameHandler());
    handlers.put(COMMAND_QUERY_PUBLISHER_SEQUENCE, new QueryPublisherSequenceFrameHandler());
    HANDLERS =
        new FrameHandler[1000]; // FIXME put create/delete stream command IDs back at the beginning
    handlers.entrySet().forEach(entry -> HANDLERS[entry.getKey()] = entry.getValue());
  }

  static FrameHandler defaultHandler() {
    return RESPONSE_FRAME_HANDLER;
  }

  static FrameHandler lookup(short commandId, short version, ByteBuf message) {
    if (version != VERSION_0) {
      message.release();
      throw new StreamException("Unsupported version " + version + " for command " + commandId);
    }
    FrameHandler handler = HANDLERS[commandId];
    if (handler == null) {
      message.release();
      throw new StreamException("Unsupported command " + commandId);
    }
    return handler;
  }

  @SuppressWarnings("unchecked")
  private static <T> OutstandingRequest<T> remove(
      ConcurrentMap<Integer, OutstandingRequest<?>> outstandingRequests,
      int correlationId,
      ParameterizedTypeReference<T> type) {
    return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
  }

  @SuppressWarnings("unchecked")
  private static <T> OutstandingRequest<T> remove(
      ConcurrentMap<Integer, OutstandingRequest<?>> outstandingRequests,
      int correlationId,
      Class<T> clazz) {
    return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
  }

  private static String readString(ByteBuf bb) {
    short size = bb.readShort();
    byte[] bytes = new byte[size];
    bb.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  interface FrameHandler {

    void handle(Client client, int frameSize, ChannelHandlerContext ctx, ByteBuf message);
  }

  private abstract static class BaseFrameHandler implements FrameHandler {

    @Override
    public void handle(Client client, int frameSize, ChannelHandlerContext ctx, ByteBuf message) {
      try {
        int read = doHandle(client, ctx, message) + 4; // already read the command id and version
        if (read != frameSize) {
          LOGGER.warn("Read {} bytes in frame, expecting {}", read, frameSize);
        }
      } catch (Exception e) {
        LOGGER.warn("Error while handling response from server", e);
      } finally {
        message.release();
      }
    }

    abstract int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message);
  }

  private static class ConfirmFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      byte publisherId = message.readByte();
      int read = 1;
      int publishingIdCount = message.readInt();
      read += 4;
      client.metricsCollector.publishConfirm(publishingIdCount);
      long publishingId;
      while (publishingIdCount != 0) {
        publishingId = message.readLong();
        read += 8;
        client.publishConfirmListener.handle(publisherId, publishingId);
        publishingIdCount--;
      }
      return read;
    }
  }

  private static class PublishErrorHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      byte publisherId = message.readByte();
      int read = 1;
      int publishingErrorCount = message.readInt();
      read += 4;
      client.metricsCollector.publishError(publishingErrorCount);
      long publishingId;
      short code;
      while (publishingErrorCount != 0) {
        publishingId = message.readLong();
        read += 8;
        code = message.readShort();
        read += 2;
        client.publishErrorListener.handle(publisherId, publishingId, code);
        publishingErrorCount--;
      }
      return read;
    }
  }

  static class DeliverFrameHandler extends BaseFrameHandler {

    static int handleMessage(
        ByteBuf bb,
        int read,
        boolean filter,
        long offset,
        long offsetLimit,
        Codec codec,
        MessageListener messageListener,
        byte subscriptionId) {
      int entrySize = bb.readInt();
      read += 4;
      byte[] data = new byte[entrySize];
      bb.readBytes(data);
      read += entrySize;

      if (filter && Long.compareUnsigned(offset, offsetLimit) < 0) {
        // filter
      } else {
        Message message = codec.decode(data);
        messageListener.handle(subscriptionId, offset, message);
      }
      return read;
    }

    static int handleDeliver(
        ByteBuf message,
        Client client,
        ChunkListener chunkListener,
        MessageListener messageListener,
        Codec codec,
        List<SubscriptionOffset> subscriptionOffsets,
        ChunkChecksum chunkChecksum,
        MetricsCollector metricsCollector) {
      byte subscriptionId = message.readByte();
      int read = 1;
      /*
      %% <<
      %%   Magic=5:4/unsigned,
      %%   ProtoVersion:4/unsigned,
      %%   ChunkType:8/unsigned, %% 0=user, 1=tracking delta, 2=tracking snapshot
      %%   NumEntries:16/unsigned, %% need some kind of limit on chunk sizes 64k is a good start
      %%   NumRecords:32/unsigned, %% total including all sub batch entries
      %%   Timestamp:64/signed, %% millisecond posix (ish) timestamp
      %%   Epoch:64/unsigned,
      %%   ChunkFirstOffset:64/unsigned,
      %%   ChunkCrc:32/integer, %% CRC for the records portion of the data
      %%   DataLength:32/unsigned, %% length until end of chunk
      %%   TrailerLength:32/unsigned
      %%   [Entry]
      %%   ...>>
       */
      // FIXME handle magic and version
      message.readByte();
      read += 1;

      byte chunkType = message.readByte();
      if (chunkType != 0) {
        throw new IllegalStateException("Invalid chunk type: " + chunkType);
      }
      read += 1;

      int numEntries = message.readUnsignedShort();
      read += 2;
      long numRecords = message.readUnsignedInt();
      read += 4;
      message.readLong(); // timestamp
      read += 8;
      message.readLong(); // epoch, unsigned long
      read += 8;
      long offset = message.readLong(); // unsigned long
      read += 8;
      long crc = message.readUnsignedInt();
      read += 4;
      long dataLength = message.readUnsignedInt();
      read += 4;
      message.readUnsignedInt(); // trailer length, unused here
      read += 4;

      chunkListener.handle(client, subscriptionId, offset, numRecords, dataLength);

      long offsetLimit = -1;
      if (!subscriptionOffsets.isEmpty()) {
        for (SubscriptionOffset subscriptionOffset : subscriptionOffsets) {
          if (subscriptionOffset.subscriptionId() == subscriptionId) {
            subscriptionOffsets.remove(subscriptionOffset);
            offsetLimit = subscriptionOffset.offset();
            break;
          }
        }
      }

      final boolean filter = offsetLimit != -1;

      try {
        // TODO handle exception in exception handler
        chunkChecksum.checksum(message, dataLength, crc);
      } catch (ChunkChecksumValidationException e) {
        LOGGER.warn(
            "Checksum failure at offset {}, expecting {}, got {}",
            offset,
            e.getExpected(),
            e.getComputed());
        throw e;
      }

      metricsCollector.chunk(numEntries);
      metricsCollector.consume(numRecords);

      while (numRecords != 0) {
        byte entryType = message.readByte();
        if ((entryType & 0x80) == 0) {
          /*
          %%   <<0=SimpleEntryType:1,
          %%     Size:31/unsigned,
          %%     Data:Size/binary>>
           */
          message.readerIndex(message.readerIndex() - 1);
          read =
              handleMessage(
                  message,
                  read,
                  filter,
                  offset,
                  offsetLimit,
                  codec,
                  messageListener,
                  subscriptionId);
          numRecords--;
          offset++; // works even for unsigned long
        } else {
          /*
          %%   <<1=SubBatchEntryType:1,
          %%     CompressionType:3,
          %%     Reserved:4,
          %%     NumRecords:16/unsigned,
          %%     Size:32/unsigned,
          %%     Data:Size/binary>>
           */
          byte compression = (byte) ((entryType & 0x70) >> 4);
          read++;
          // compression not used yet, just making sure we get something
          MessageBatch.Compression.get(compression);
          int numRecordsInBatch = message.readUnsignedShort();
          read += 2;
          message.readInt(); // batch size, does not need it
          read += 4;

          numRecords -= numRecordsInBatch;

          while (numRecordsInBatch != 0) {
            read =
                handleMessage(
                    message,
                    read,
                    filter,
                    offset,
                    offsetLimit,
                    codec,
                    messageListener,
                    subscriptionId);
            numRecordsInBatch--;
            offset++; // works even for unsigned long
          }
        }
      }
      return read;
    }

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      return handleDeliver(
          message,
          client,
          client.chunkListener,
          client.messageListener,
          client.codec,
          client.subscriptionOffsets,
          client.chunkChecksum,
          client.metricsCollector);
    }
  }

  private static class MetadataUpdateFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      short code = message.readShort();
      int read = 2;
      if (code == RESPONSE_CODE_STREAM_NOT_AVAILABLE) {
        String stream = readString(message);
        LOGGER.debug("Stream {} is no longer available", stream);
        read += (2 + stream.length());
        client.metadataListener.handle(stream, code);
      } else {
        throw new IllegalArgumentException("Unsupported metadata update code " + code);
      }
      return read;
    }
  }

  private static class CloseFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short closeCode = message.readShort();
      read += 2;
      String closeReason = readString(message);
      read += 2 + closeReason.length();

      LOGGER.info("Received close from server, reason: {} {}", closeCode, closeReason);

      int length = 2 + 2 + 4 + 2;
      ByteBuf byteBuf = client.allocate(ctx.alloc(), length + 4);
      byteBuf
          .writeInt(length)
          .writeShort(COMMAND_CLOSE)
          .writeShort(VERSION_0)
          .writeInt(correlationId)
          .writeShort(RESPONSE_CODE_OK);

      ctx.writeAndFlush(byteBuf)
          .addListener(
              future -> {
                if (client.closing.compareAndSet(false, true)) {
                  client.executorService.submit(
                      () -> client.closingSequence(ShutdownContext.ShutdownReason.SERVER_CLOSE));
                }
              });
      return read;
    }
  }

  private static class QueryPublisherSequenceFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;
      long sequence = message.readLong();
      read += 8;

      OutstandingRequest<QueryPublisherSequenceResponse> outstandingRequest =
          remove(client.outstandingRequests, correlationId, QueryPublisherSequenceResponse.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        QueryPublisherSequenceResponse response =
            new QueryPublisherSequenceResponse(responseCode, sequence);
        outstandingRequest.response().set(response);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class QueryOffsetFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;
      long offset = message.readLong();
      read += 8;

      OutstandingRequest<QueryOffsetResponse> outstandingRequest =
          remove(client.outstandingRequests, correlationId, QueryOffsetResponse.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        QueryOffsetResponse response = new QueryOffsetResponse(responseCode, offset);
        outstandingRequest.response().set(response);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class CreditNotificationFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      short responseCode = message.readShort();
      int read = 2;
      byte subscriptionId = message.readByte();
      read += 1;

      client.creditNotification.handle(subscriptionId, responseCode);
      return read;
    }
  }

  private static class PeerPropertiesFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;

      short responseCode = message.readShort();
      read += 2;
      if (responseCode != RESPONSE_CODE_OK) {
        while (message.isReadable()) {
          message.readByte();
        }
        // FIXME: should we unblock the request and notify that there's something wrong?
        throw new StreamException(
            "Unexpected response code for SASL handshake response: " + responseCode);
      }

      int serverPropertiesCount = message.readInt();
      read += 4;
      Map<String, String> serverProperties = new LinkedHashMap<>(serverPropertiesCount);

      for (int i = 0; i < serverPropertiesCount; i++) {
        String key = readString(message);
        read += 2 + key.length();
        String value = readString(message);
        read += 2 + value.length();
        serverProperties.put(key, value);
      }

      OutstandingRequest<Map<String, String>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<Map<String, String>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(Collections.unmodifiableMap(serverProperties));
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class HeartbeatFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      return 0;
    }
  }

  private static class TuneFrameHandler extends BaseFrameHandler {

    private static int negotiatedMaxValue(int clientValue, int serverValue) {
      return (clientValue == 0 || serverValue == 0)
          ? Math.max(clientValue, serverValue)
          : Math.min(clientValue, serverValue);
    }

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int serverMaxFrameSize = message.readInt();
      int read = 4;
      int serverHeartbeat = message.readInt();
      read += 4;

      int maxFrameSize =
          negotiatedMaxValue(client.tuneState.requestedMaxFrameSize(), serverMaxFrameSize);
      int heartbeat = negotiatedMaxValue(client.tuneState.requestedHeartbeat(), serverHeartbeat);

      int length = 2 + 2 + 4 + 4;
      ByteBuf byteBuf = client.allocateNoCheck(ctx.alloc(), length + 4);
      byteBuf
          .writeInt(length)
          .writeShort(COMMAND_TUNE)
          .writeShort(VERSION_0)
          .writeInt(maxFrameSize)
          .writeInt(heartbeat);
      ctx.writeAndFlush(byteBuf);

      client.tuneState.maxFrameSize(maxFrameSize).heartbeat(heartbeat);

      if (heartbeat > 0) {
        client
            .channel
            .pipeline()
            .addBefore(
                Client.NETTY_HANDLER_FRAME_DECODER,
                Client.NETTY_HANDLER_IDLE_STATE,
                new IdleStateHandler(heartbeat * 2, heartbeat, 0));
      }

      client.tuneState.done();
      return read;
    }
  }

  private static class SaslAuthenticateFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;

      short responseCode = message.readShort();
      read += 2;

      byte[] challenge;
      if (responseCode == RESPONSE_CODE_SASL_CHALLENGE) {
        int challengeSize = message.readInt();
        read += 4;
        challenge = new byte[challengeSize];
        message.readBytes(challenge);
        read += challenge.length;
      } else {
        challenge = null;
      }

      SaslAuthenticateResponse response = new SaslAuthenticateResponse(responseCode, challenge);

      OutstandingRequest<SaslAuthenticateResponse> outstandingRequest =
          remove(client.outstandingRequests, correlationId, SaslAuthenticateResponse.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(response);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class SaslHandshakeFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;

      short responseCode = message.readShort();
      read += 2;
      if (responseCode != RESPONSE_CODE_OK) {
        while (message.isReadable()) {
          message.readByte();
        }
        // FIXME: should we unlock the request and notify that there's something wrong?
        throw new StreamException(
            "Unexpected response code for SASL handshake response: " + responseCode);
      }

      int mechanismsCount = message.readInt();

      read += 4;
      List<String> mechanisms = new ArrayList<>(mechanismsCount);
      for (int i = 0; i < mechanismsCount; i++) {
        String mechanism = readString(message);
        mechanisms.add(mechanism);
        read += 2 + mechanism.length();
      }

      OutstandingRequest<List<String>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<List<String>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(mechanisms);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class MetadataFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      Map<Short, Broker> brokers = new HashMap<>();
      int brokersCount = message.readInt();
      read += 4;
      for (int i = 0; i < brokersCount; i++) {
        short brokerReference = message.readShort();
        read += 2;
        String host = readString(message);
        read += 2 + host.length();
        int port = message.readInt();
        read += 4;
        brokers.put(brokerReference, new Broker(host, port));
      }

      int streamsCount = message.readInt();
      Map<String, StreamMetadata> results = new LinkedHashMap<>(streamsCount);
      read += 4;
      for (int i = 0; i < streamsCount; i++) {
        String stream = readString(message);
        read += 2 + stream.length();
        short responseCode = message.readShort();
        read += 2;
        short leaderReference = message.readShort();
        read += 2;
        int replicasCount = message.readInt();
        read += 4;
        List<Broker> replicas;
        if (replicasCount == 0) {
          replicas = Collections.emptyList();
        } else {
          replicas = new ArrayList<>(replicasCount);
          for (int j = 0; j < replicasCount; j++) {
            short replicaReference = message.readShort();
            read += 2;
            replicas.add(brokers.get(replicaReference));
          }
        }
        StreamMetadata streamMetadata =
            new StreamMetadata(stream, responseCode, brokers.get(leaderReference), replicas);
        results.put(stream, streamMetadata);
      }

      OutstandingRequest<Map<String, StreamMetadata>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<Map<String, StreamMetadata>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(results);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class ResponseFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;

      OutstandingRequest<Response> outstandingRequest =
          remove(client.outstandingRequests, correlationId, Response.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        Response response = new Response(responseCode);
        outstandingRequest.response().set(response);
        outstandingRequest.countDown();
      }
      return read;
    }
  }
}

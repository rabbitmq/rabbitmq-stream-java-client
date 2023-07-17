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
import static com.rabbitmq.stream.Constants.COMMAND_DELIVER;
import static com.rabbitmq.stream.Constants.COMMAND_EXCHANGE_COMMAND_VERSIONS;
import static com.rabbitmq.stream.Constants.COMMAND_HEARTBEAT;
import static com.rabbitmq.stream.Constants.COMMAND_METADATA;
import static com.rabbitmq.stream.Constants.COMMAND_METADATA_UPDATE;
import static com.rabbitmq.stream.Constants.COMMAND_OPEN;
import static com.rabbitmq.stream.Constants.COMMAND_PARTITIONS;
import static com.rabbitmq.stream.Constants.COMMAND_PEER_PROPERTIES;
import static com.rabbitmq.stream.Constants.COMMAND_PUBLISH_CONFIRM;
import static com.rabbitmq.stream.Constants.COMMAND_PUBLISH_ERROR;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_OFFSET;
import static com.rabbitmq.stream.Constants.COMMAND_QUERY_PUBLISHER_SEQUENCE;
import static com.rabbitmq.stream.Constants.COMMAND_ROUTE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_AUTHENTICATE;
import static com.rabbitmq.stream.Constants.COMMAND_SASL_HANDSHAKE;
import static com.rabbitmq.stream.Constants.COMMAND_STREAM_STATS;
import static com.rabbitmq.stream.Constants.COMMAND_SUBSCRIBE;
import static com.rabbitmq.stream.Constants.COMMAND_TUNE;
import static com.rabbitmq.stream.Constants.COMMAND_UNSUBSCRIBE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_OK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SASL_CHALLENGE;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE;
import static com.rabbitmq.stream.Constants.VERSION_1;
import static com.rabbitmq.stream.Constants.VERSION_2;
import static com.rabbitmq.stream.impl.Utils.encodeResponseCode;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.ChunkChecksumValidationException;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.impl.Client.*;
import com.rabbitmq.stream.impl.Client.ShutdownContext.ShutdownReason;
import com.rabbitmq.stream.impl.Utils.MutableBoolean;
import com.rabbitmq.stream.metrics.MetricsCollector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ServerFrameHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerFrameHandler.class);
  private static final FrameHandler RESPONSE_FRAME_HANDLER = new ResponseFrameHandler();

  private static final FrameHandler[][] HANDLERS;

  static {
    short maxCommandKey =
        (short)
            Arrays.stream(Constants.class.getDeclaredFields())
                .filter(f -> f.getName().startsWith("COMMAND_"))
                .mapToInt(
                    field -> {
                      try {
                        return ((Number) field.get(null)).intValue();
                      } catch (IllegalAccessException e) {
                        LOGGER.info(
                            "Error while trying to access field Constants." + field.getName());
                        return 0;
                      }
                    })
                .max()
                .getAsInt();
    Map<Short, FrameHandler> handlers = new HashMap<>();
    handlers.put(COMMAND_CLOSE, new CloseFrameHandler());
    handlers.put(COMMAND_SUBSCRIBE, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_UNSUBSCRIBE, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DECLARE_PUBLISHER, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DELETE_PUBLISHER, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_CREATE_STREAM, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_DELETE_STREAM, RESPONSE_FRAME_HANDLER);
    handlers.put(COMMAND_OPEN, new OpenFrameHandler());
    handlers.put(COMMAND_PUBLISH_CONFIRM, new ConfirmFrameHandler());
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
    handlers.put(COMMAND_ROUTE, new RouteFrameHandler());
    handlers.put(COMMAND_PARTITIONS, new PartitionsFrameHandler());
    handlers.put(COMMAND_CONSUMER_UPDATE, new ConsumerUpdateFrameHandler());
    handlers.put(COMMAND_EXCHANGE_COMMAND_VERSIONS, new ExchangeCommandVersionsFrameHandler());
    handlers.put(COMMAND_STREAM_STATS, new StreamStatsFrameHandler());
    HANDLERS = new FrameHandler[maxCommandKey + 1][];
    handlers
        .entrySet()
        .forEach(
            entry -> {
              HANDLERS[entry.getKey()] = new FrameHandler[VERSION_1 + 1];
              HANDLERS[entry.getKey()][VERSION_1] = entry.getValue();
            });
    HANDLERS[COMMAND_DELIVER] = new FrameHandler[VERSION_2 + 1];
    HANDLERS[COMMAND_DELIVER][VERSION_1] = new DeliverVersion1FrameHandler();
    HANDLERS[COMMAND_DELIVER][VERSION_2] = new DeliverVersion2FrameHandler();
  }

  static class FrameHandlerInfo {

    private final short key, minVersion, maxVersion;

    FrameHandlerInfo(short key, short minVersion, short maxVersion) {
      this.key = key;
      this.minVersion = minVersion;
      this.maxVersion = maxVersion;
    }

    short getKey() {
      return key;
    }

    short getMinVersion() {
      return minVersion;
    }

    short getMaxVersion() {
      return maxVersion;
    }

    @Override
    public String toString() {
      return "FrameHandlerInfo{"
          + "key="
          + key
          + ", minVersion="
          + minVersion
          + ", maxVersion="
          + maxVersion
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FrameHandlerInfo that = (FrameHandlerInfo) o;
      return key == that.key && minVersion == that.minVersion && maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, minVersion, maxVersion);
    }
  }

  static List<FrameHandlerInfo> commandVersions() {
    List<FrameHandlerInfo> infos = new ArrayList<>(HANDLERS.length);
    for (int i = 0; i < HANDLERS.length; i++) {
      FrameHandler[] handlers = HANDLERS[i];
      if (handlers == null) {
        continue;
      }
      FrameHandler handler = null;
      int minVersion = Short.MAX_VALUE, maxVersion = 0;
      for (short j = VERSION_1; j < handlers.length; j++) {
        if (handlers[j] != null && handlers[j].isInitiatedByServer()) {
          minVersion = Math.min(minVersion, j);
          maxVersion = Math.max(maxVersion, j);
          handler = handlers[j];
        }
      }
      if (handler != null) {
        infos.add(new FrameHandlerInfo((short) i, (short) minVersion, (short) maxVersion));
      }
    }
    return infos;
  }

  static FrameHandler defaultHandler() {
    return RESPONSE_FRAME_HANDLER;
  }

  static FrameHandler lookup(short commandId, short version, ByteBuf message) {
    FrameHandler handler = HANDLERS[commandId][version];
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

    default boolean isInitiatedByServer() {
      return false;
    }
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

  static class DeliverVersion1FrameHandler extends BaseFrameHandler {

    @Override
    public boolean isInitiatedByServer() {
      return true;
    }

    static int handleMessage(
        ByteBuf bb,
        int read,
        boolean ignore,
        MutableBoolean messageIgnored,
        long offset,
        long offsetLimit,
        long chunkTimestamp,
        long committedChunkId,
        Codec codec,
        MessageListener messageListener,
        byte subscriptionId,
        Object chunkContext) {
      int entrySize = bb.readInt();
      read += 4;
      byte[] data = new byte[entrySize];
      bb.readBytes(data);
      read += entrySize;

      if (ignore && Long.compareUnsigned(offset, offsetLimit) < 0) {
        messageIgnored.set(true);
      } else {
        Message message = codec.decode(data);
        messageListener.handle(
            subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext, message);
      }
      return read;
    }

    static int handleDeliverVersion1(
        ByteBuf message,
        Client client,
        ChunkListener chunkListener,
        MessageListener messageListener,
        MessageIgnoredListener messageIgnoredListener,
        Codec codec,
        List<SubscriptionOffset> subscriptionOffsets,
        ChunkChecksum chunkChecksum,
        MetricsCollector metricsCollector) {
      return handleDeliver(
          message,
          client,
          chunkListener,
          messageListener,
          messageIgnoredListener,
          codec,
          subscriptionOffsets,
          chunkChecksum,
          metricsCollector,
          message.readByte(), // subscription ID
          0, // last committed offset
          1 // byte read count
          );
    }

    static int handleDeliver(
        ByteBuf message,
        Client client,
        ChunkListener chunkListener,
        MessageListener messageListener,
        MessageIgnoredListener messageIgnoredListener,
        Codec codec,
        List<SubscriptionOffset> subscriptionOffsets,
        ChunkChecksum chunkChecksum,
        MetricsCollector metricsCollector,
        byte subscriptionId,
        long committedOffset,
        int read) {
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
      long chunkTimestamp = message.readLong(); // timestamp
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
      message.readInt(); // 4 reserved bytes, unused here
      read += 4;

      Object chunkContext =
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

      final boolean ignore = offsetLimit != -1;

      try {
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
      long messagesRead = 0;
      MutableBoolean messageIgnored = new MutableBoolean(false);

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
                  ignore,
                  messageIgnored,
                  offset,
                  offsetLimit,
                  chunkTimestamp,
                  committedOffset,
                  codec,
                  messageListener,
                  subscriptionId,
                  chunkContext);
          if (messageIgnored.get()) {
            messageIgnoredListener.ignored(
                subscriptionId, offset, chunkTimestamp, committedOffset, chunkContext);
            messageIgnored.set(false);
          } else {
            messagesRead++;
          }
          numRecords--;
          offset++; // works even for unsigned long
        } else {
          /*
          %%   |0              |1              |2              |3              | Bytes
          %%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
          %%   +-+-----+-------+---------------+---------------+---------------+
          %%   |1| Cmp | Rsvd  | Number of records             | Length  (...) |
          %%   +-+-----+-------+-------------------------------+---------------+
          %%   | Length                                        | Body          |
          %%   +-+---------------------------------------------+               +
          %%   | Body                                                          |
          %%   :                                                               :
          %%   +---------------------------------------------------------------+
                     */
          byte compression = (byte) ((entryType & 0x70) >> 4);
          read++;
          Compression comp = Compression.get(compression);
          int numRecordsInBatch = message.readUnsignedShort();
          read += 2;
          int uncompressedDataSize = message.readInt();
          read += 4;
          int dataSize = message.readInt();
          read += 4;

          int readBeforeSubEntries = read;
          ByteBuf bbToReadFrom = message;
          if (comp.code() != Compression.NONE.code()) {
            CompressionCodec compressionCodec = client.compressionCodecFactory.get(comp);
            ByteBuf outBb = client.channel.alloc().heapBuffer(uncompressedDataSize);
            ByteBuf slice = message.slice(message.readerIndex(), dataSize);
            InputStream inputStream = compressionCodec.decompress(new ByteBufInputStream(slice));
            byte[] inBuffer = new byte[Math.min(uncompressedDataSize, 1024)];
            int n;
            try {
              while (-1 != (n = inputStream.read(inBuffer))) {
                outBb.writeBytes(inBuffer, 0, n);
              }
            } catch (IOException e) {
              throw new StreamException("Error while uncompressing sub-entry", e);
            }
            message.readerIndex(message.readerIndex() + dataSize);
            bbToReadFrom = outBb;
          }

          numRecords -= numRecordsInBatch;

          while (numRecordsInBatch != 0) {
            read =
                handleMessage(
                    bbToReadFrom,
                    read,
                    ignore,
                    messageIgnored,
                    offset,
                    offsetLimit,
                    chunkTimestamp,
                    committedOffset,
                    codec,
                    messageListener,
                    subscriptionId,
                    chunkContext);
            if (messageIgnored.get()) {
              messageIgnoredListener.ignored(
                  subscriptionId, offset, chunkTimestamp, committedOffset, chunkContext);
              messageIgnored.set(false);
            } else {
              messagesRead++;
            }
            numRecordsInBatch--;
            offset++; // works even for unsigned long
          }

          if (comp.code() != Compression.NONE.code()) {
            bbToReadFrom.release();
            // to avoid a warning, we read more from what it's inside the frame with compression
            read = readBeforeSubEntries + dataSize;
          }
        }
      }
      metricsCollector.consume(messagesRead);
      return read;
    }

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      return handleDeliverVersion1(
          message,
          client,
          client.chunkListener,
          client.messageListener,
          client.messageIgnoredListener,
          client.codec,
          client.subscriptionOffsets,
          client.chunkChecksum,
          client.metricsCollector);
    }
  }

  static class DeliverVersion2FrameHandler extends BaseFrameHandler {

    @Override
    public boolean isInitiatedByServer() {
      return true;
    }

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      return DeliverVersion1FrameHandler.handleDeliver(
          message,
          client,
          client.chunkListener,
          client.messageListener,
          client.messageIgnoredListener,
          client.codec,
          client.subscriptionOffsets,
          client.chunkChecksum,
          client.metricsCollector,
          message.readByte(), // subscription ID
          message.readLong(), // committed chunk ID, unsigned long
          9 // byte read count, 1 + 9
          );
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
          .writeShort(encodeResponseCode(COMMAND_CLOSE))
          .writeShort(VERSION_1)
          .writeInt(correlationId)
          .writeShort(RESPONSE_CODE_OK);

      client.shutdownReason(ShutdownReason.SERVER_CLOSE);

      ctx.writeAndFlush(byteBuf)
          .addListener(
              future -> {
                if (client.closing.compareAndSet(false, true)) {
                  client.executorService.submit(
                      () -> client.closingSequence(ShutdownReason.SERVER_CLOSE));
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

  private static class ConsumerUpdateFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      byte subscriptionId = message.readByte();
      read += 1;
      byte activeByte = message.readByte();
      read += 1;

      OffsetSpecification offsetSpecification =
          client.consumerUpdateListener.handle(
              client, subscriptionId, activeByte == 1 ? true : false);

      client.consumerUpdateResponse(correlationId, RESPONSE_CODE_OK, offsetSpecification);

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

  private static class OpenFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;

      short responseCode = message.readShort();
      read += 2;

      Map<String, String> connectionProperties;
      if (message.isReadable()) {
        int connectionPropertiesCount = message.readInt();
        read += 4;
        connectionProperties = new LinkedHashMap<>(connectionPropertiesCount);
        for (int i = 0; i < connectionPropertiesCount; i++) {
          String key = readString(message);
          read += 2 + key.length();
          String value = readString(message);
          read += 2 + value.length();
          connectionProperties.put(key, value);
        }
      } else {
        connectionProperties = Collections.emptyMap();
      }

      OutstandingRequest<OpenResponse> outstandingRequest =
          remove(client.outstandingRequests, correlationId, OpenResponse.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(new OpenResponse(responseCode, connectionProperties));
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
          .writeShort(encodeResponseCode(COMMAND_TUNE))
          .writeShort(VERSION_1)
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

  private static class RouteFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;
      int streamCount = message.readInt();
      read += 4;
      List<String> streams;
      if (streamCount == 0) {
        streams = Collections.emptyList();
      } else {
        streams = new ArrayList<>(streamCount);
        for (int i = 0; i < streamCount; i++) {
          String stream = readString(message);
          read += (2 + stream.length());
          streams.add(stream);
        }
      }

      if (responseCode != RESPONSE_CODE_OK) {
        LOGGER.info("Route returned error: {}", Utils.formatConstant(responseCode));
      }

      OutstandingRequest<List<String>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<List<String>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(streams);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class PartitionsFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;
      int streamCount = message.readInt();
      read += 4;

      List<String> streams;
      if (streamCount == 0) {
        streams = Collections.emptyList();
      } else {
        streams = new ArrayList<>(streamCount);
        for (int i = 0; i < streamCount; i++) {
          String stream = readString(message);
          read += (2 + stream.length());
          streams.add(stream);
        }
      }

      if (responseCode != RESPONSE_CODE_OK) {
        LOGGER.info("Route returned error: {}", Utils.formatConstant(responseCode));
      }

      OutstandingRequest<List<String>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<List<String>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(streams);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class ExchangeCommandVersionsFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;
      short responseCode = message.readShort();
      read += 2;
      int commandVersionsCount = message.readInt();
      read += 4;

      List<FrameHandlerInfo> commandVersions;
      if (commandVersionsCount == 0) {
        commandVersions = Collections.emptyList();
      } else {
        commandVersions = new ArrayList<>(commandVersionsCount);
        for (int i = 0; i < commandVersionsCount; i++) {
          short key = message.readShort();
          short minVersion = message.readShort();
          short maxVersion = message.readShort();
          read += 6;
          commandVersions.add(new FrameHandlerInfo(key, minVersion, maxVersion));
        }
      }

      if (responseCode != RESPONSE_CODE_OK) {
        LOGGER.info(
            "Exchange command versions returned error: {}", Utils.formatConstant(responseCode));
      }

      OutstandingRequest<List<FrameHandlerInfo>> outstandingRequest =
          remove(
              client.outstandingRequests,
              correlationId,
              new ParameterizedTypeReference<List<FrameHandlerInfo>>() {});
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(commandVersions);
        outstandingRequest.countDown();
      }
      return read;
    }
  }

  private static class StreamStatsFrameHandler extends BaseFrameHandler {

    @Override
    int doHandle(Client client, ChannelHandlerContext ctx, ByteBuf message) {
      int correlationId = message.readInt();
      int read = 4;

      short responseCode = message.readShort();
      read += 2;

      int infoCount = message.readInt();
      read += 4;
      Map<String, Long> info = new LinkedHashMap<>(infoCount);

      for (int i = 0; i < infoCount; i++) {
        String key = readString(message);
        read += 2 + key.length();
        long value = message.readLong();
        info.put(key, value);
        read += 8;
      }

      OutstandingRequest<StreamStatsResponse> outstandingRequest =
          remove(client.outstandingRequests, correlationId, StreamStatsResponse.class);
      if (outstandingRequest == null) {
        LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
      } else {
        outstandingRequest.response().set(new StreamStatsResponse(responseCode, info));
        outstandingRequest.countDown();
      }
      return read;
    }
  }
}

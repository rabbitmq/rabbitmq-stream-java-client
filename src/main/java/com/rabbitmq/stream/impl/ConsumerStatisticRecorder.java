package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.CallbackStreamDataHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerStatisticRecorder implements CallbackStreamDataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerStatisticRecorder.class);

    private final ConcurrentMap<String, Set<Byte>> streamNameToSubscriptionIdMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Byte, SubscriptionStatistics> subscriptionStatisticsMap = new ConcurrentHashMap<>();

    @Override
    public void handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    ) {
        this.streamNameToSubscriptionIdMap.compute(
            stream,
            (k, v) -> {
                if(v == null) {
                    v = Collections.newSetFromMap(new ConcurrentHashMap<>());
                }
                boolean isNewElement = v.add(subscriptionId);
                if(!isNewElement) {
                    LOGGER.warn(
                        "handleSubscribed called for stream that already had same associated subscription! " +
                                "subscriptionId={}, stream={}",
                        subscriptionId,
                        stream
                    );
                }
                return v;
            }
        );
        this.subscriptionStatisticsMap.compute(
            subscriptionId,
            (k, v) -> {
                if(v != null) {
                    LOGGER.warn(
                        "handleSubscribed called for subscription that already exists! subscriptionId={}",
                        subscriptionId
                    );
                }
                return new SubscriptionStatistics(subscriptionId, stream, subscriptionProperties);
            }
        );
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        this.subscriptionStatisticsMap.compute(
            subscriptionId,
            (k, v) -> {
                if(v == null) {
                    LOGGER.warn(
                        "handleChunk called for subscription that does not exist! subscriptionId={}",
                        subscriptionId
                    );
                    return null;
                }
                v.pendingChunks.decrementAndGet();
                v.unprocessedChunksByOffset.put(offset, new ChunkStatistics(offset, messageCount, dataSize));
                return v;
            }
        );
    }

    @Override
    public void handleMessage(
            byte subscriptionId,
            long offset,
            long chunkTimestamp,
            long committedChunkId,
            Message message
    ) {
        this.subscriptionStatisticsMap.compute(
            subscriptionId,
            (k, v) -> {
                if(v == null) {
                    LOGGER.warn(
                        "handleMessage called for subscription that does not exist! subscriptionId={}",
                        subscriptionId
                    );
                    return null;
                }
                NavigableMap<Long, ChunkStatistics> subHeadMapByOffset = v.unprocessedChunksByOffset.headMap(offset, true);
                Map.Entry<Long, ChunkStatistics> lastOffsetToChunkEntry = subHeadMapByOffset.lastEntry();
                if(lastOffsetToChunkEntry == null) {
                    LOGGER.warn(
                        "handleMessage called but chunk was not found! subscriptionId={} offset={}",
                        subscriptionId,
                        offset
                    );
                    return v;
                }
                ChunkStatistics chunkStatistics = lastOffsetToChunkEntry.getValue();
                chunkStatistics.unprocessedMessagesByOffset.put(offset, message);
                return v;
            }
        );
    }

    @Override
    public void handleUnsubscribe(byte subscriptionId) {
        Object removed = this.subscriptionStatisticsMap.remove(subscriptionId);
        if(removed == null) {
            LOGGER.warn(
                "handleUnsubscribe called for subscriptionId that does not exist! subscriptionId={}",
                subscriptionId
            );
        }
    }

    /**
     * Marks a message as handled, changing internal statistics.
     *
     * @param messageContext The {@link MessageHandler.Context} of the handled message
     * @return Whether the message was marked as handled (returning {@code true})
     *         or was not found (either because it was already marked as handled, or wasn't tracked)
     */
    public boolean markHandled(MessageHandler.Context messageContext) {
        AggregatedMessageStatistics entry = retrieveStatistics(messageContext);
        if (entry == null) {
            return false;
        }
        return markHandled(entry);
    }

    /**
     * Marks a message as handled, changing internal statistics.
     *
     * @param aggregatedMessageStatistics The {@link AggregatedMessageStatistics} of the handled message
     * @return Whether the message was marked as handled (returning {@code true})
     *         or was not found (either because it was already marked as handled, or wasn't tracked)
     */
    public boolean markHandled(AggregatedMessageStatistics aggregatedMessageStatistics) {
        // Can't remove, not enough information
        if (aggregatedMessageStatistics.chunkStatistics == null
                || aggregatedMessageStatistics.messageEntry == null
                || aggregatedMessageStatistics.chunkHeadMap == null) {
            return false;
        }
        Message removedMessage = aggregatedMessageStatistics.chunkStatistics
                .unprocessedMessagesByOffset
                .remove(aggregatedMessageStatistics.offset);
        if (removedMessage == null) {
            return false;
        }
        // Remove chunk from list of unprocessed chunks if all its messages have been processed
        aggregatedMessageStatistics.chunkStatistics.processedMessages.incrementAndGet();
        if (aggregatedMessageStatistics.chunkStatistics.isDone()) {
            aggregatedMessageStatistics.chunkHeadMap.remove(aggregatedMessageStatistics.messageEntry.getKey(), aggregatedMessageStatistics.chunkStatistics);
        }
        return true;
    }

    public AggregatedMessageStatistics retrieveStatistics(String stream, long offset) {
        Set<Byte> possibleSubscriptionIds = this.streamNameToSubscriptionIdMap.get(stream);
        AggregatedMessageStatistics entry = null;
        for (Byte subscriptionId : possibleSubscriptionIds) {
            entry = retrieveStatistics(subscriptionId, offset);
            if (entry == null) {
                continue;
            }
            // We have all the info we need, we found the specific chunk. Stop right here
            if (entry.chunkHeadMap != null && entry.chunkStatistics != null) {
                return entry;
            }
        }
        // Return the next-best result, because we might find the subscription but not the message
        return entry;
    }

    public AggregatedMessageStatistics retrieveStatistics(byte subscriptionId, long offset) {
        SubscriptionStatistics subscriptionStatistics = this.subscriptionStatisticsMap.get(subscriptionId);
        if (subscriptionStatistics == null) {
            return null;
        }
        NavigableMap<Long, ChunkStatistics> chunkStatisticsHeadMap = subscriptionStatistics.unprocessedChunksByOffset.headMap(offset, true);
        Map.Entry<Long, ChunkStatistics> messageEntry = chunkStatisticsHeadMap.lastEntry();
        ChunkStatistics chunkStatistics = messageEntry == null ? null : messageEntry.getValue();
        return new AggregatedMessageStatistics(offset, subscriptionStatistics, chunkStatisticsHeadMap, chunkStatistics, messageEntry);
    }

    public AggregatedMessageStatistics retrieveStatistics(MessageHandler.Context messageContext) {
        return retrieveStatistics(messageContext.stream(), messageContext.offset());
    }

    public static class AggregatedMessageStatistics {

        private final long offset;
        private final SubscriptionStatistics subscriptionStatistics;
        private final NavigableMap<Long, ChunkStatistics> chunkHeadMap;
        private final ChunkStatistics chunkStatistics;
        private final Map.Entry<Long, ChunkStatistics> messageEntry;

        public AggregatedMessageStatistics(
                long offset,
                @Nonnull SubscriptionStatistics subscriptionStatistics,
                @Nullable NavigableMap<Long, ChunkStatistics> chunkHeadMap,
                @Nullable ChunkStatistics chunkStatistics,
                @Nullable Map.Entry<Long, ChunkStatistics> messageEntry) {
            this.subscriptionStatistics = subscriptionStatistics;
            this.chunkStatistics = chunkStatistics;
            this.chunkHeadMap = chunkHeadMap;
            this.messageEntry = messageEntry;
            this.offset = offset;
        }

        @Nonnull
        public SubscriptionStatistics getSubscriptionStatistics() {
            return subscriptionStatistics;
        }

        @Nullable
        public ChunkStatistics getChunkStatistics() {
            return chunkStatistics;
        }

        @Nullable
        public NavigableMap<Long, ChunkStatistics> getChunkHeadMap() {
            return chunkHeadMap;
        }

        @Nullable
        public Map.Entry<Long, ChunkStatistics> getMessageEntry() {
            return messageEntry;
        }

        public long getOffset() {
            return offset;
        }

    }

    public static class SubscriptionStatistics {

        private final byte subscriptionId;
        private final String stream;
        private final AtomicInteger pendingChunks = new AtomicInteger(0);
        private final Map<String, String> subscriptionProperties;
        private final NavigableMap<Long, ChunkStatistics> unprocessedChunksByOffset;

        public SubscriptionStatistics(byte subscriptionId, String stream, Map<String, String> subscriptionProperties) {
            this(subscriptionId, stream, subscriptionProperties, new ConcurrentSkipListMap<>());
        }

        public SubscriptionStatistics(
                byte subscriptionId,
                String stream,
                Map<String, String> subscriptionProperties,
                NavigableMap<Long, ChunkStatistics> unprocessedChunksByOffset
        ) {
            this.subscriptionId = subscriptionId;
            this.stream = stream;
            this.subscriptionProperties = subscriptionProperties;
            this.unprocessedChunksByOffset = unprocessedChunksByOffset;
        }

        public byte getSubscriptionId() {
            return subscriptionId;
        }

        public String getStream() {
            return stream;
        }

        public AtomicInteger getPendingChunks() {
            return pendingChunks;
        }

        public Map<String, String> getSubscriptionProperties() {
            return Collections.unmodifiableMap(subscriptionProperties);
        }

        public NavigableMap<Long, ChunkStatistics> getUnprocessedChunksByOffset() {
            return Collections.unmodifiableNavigableMap(unprocessedChunksByOffset);
        }

    }

    public static class ChunkStatistics {

        private final long offset;
        private AtomicLong processedMessages = new AtomicLong();
        private final long messageCount;
        private final long dataSize;
        private final Map<Long, Message> unprocessedMessagesByOffset;

        public ChunkStatistics(long offset, long messageCount, long dataSize) {
            this(offset, messageCount, dataSize, new ConcurrentHashMap<>());
        }

        public ChunkStatistics(long offset, long messageCount, long dataSize, Map<Long, Message> unprocessedMessagesByOffset) {
            this.offset = offset;
            this.messageCount = messageCount;
            this.dataSize = dataSize;
            this.unprocessedMessagesByOffset = unprocessedMessagesByOffset;
        }

        public long getOffset() {
            return offset;
        }

        public long getMessageCount() {
            return messageCount;
        }

        public long getDataSize() {
            return dataSize;
        }

        public Map<Long, Message> getUnprocessedMessagesByOffset() {
            return Collections.unmodifiableMap(unprocessedMessagesByOffset);
        }

        public boolean isDone() {
            return processedMessages.get() == messageCount && unprocessedMessagesByOffset.isEmpty();
        }
    }

    public Map<String, Set<Byte>> getStreamNameToSubscriptionIdMap() {
        return Collections.unmodifiableMap(streamNameToSubscriptionIdMap);
    }

    public Map<Byte, SubscriptionStatistics> getSubscriptionStatisticsMap() {
        return Collections.unmodifiableMap(subscriptionStatisticsMap);
    }

}

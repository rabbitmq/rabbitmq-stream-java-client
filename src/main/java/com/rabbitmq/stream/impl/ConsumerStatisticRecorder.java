package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.CallbackStreamDataHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.MessageHandlingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConsumerStatisticRecorder implements CallbackStreamDataHandler, MessageHandlingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerStatisticRecorder.class);

    private final String identifier;
    private final AtomicReference<SubscriptionStatistics> subscriptionStatistics = new AtomicReference<>();

    public ConsumerStatisticRecorder(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public void handleSubscribe(
            OffsetSpecification offsetSpecification,
            boolean isInitialSubscription
    ) {
        SubscriptionStatistics localSubscriptionStatistics = this.subscriptionStatistics.get();
        if(localSubscriptionStatistics == null) {
            this.subscriptionStatistics.set(new SubscriptionStatistics(offsetSpecification));
            return;
        }
        if(isInitialSubscription) {
            LOGGER.warn(
                    "handleSubscribe called for stream that already had same associated subscription! "
                    + "identifier={} offsetSpecification={}",
                    this.identifier,
                    offsetSpecification
            );
        }
        localSubscriptionStatistics.offsetSpecification = offsetSpecification;
        localSubscriptionStatistics.pendingChunks.set(0);
        cleanupOldTrackingData(localSubscriptionStatistics);
    }

    private void cleanupOldTrackingData(SubscriptionStatistics subscriptionStatistics) {
        if (!subscriptionStatistics.offsetSpecification.isOffset()) {
            LOGGER.debug("Can't cleanup old tracking data: offsetSpecification is not an offset! "
                    + "identifier={} offsetSpecification={}",
                    this.identifier,
                    subscriptionStatistics.offsetSpecification
            );
            return;
        }
        // Mark messages before the initial offset as handled
        long newSubscriptionInitialOffset = subscriptionStatistics.offsetSpecification.getOffset();
        NavigableMap<Long, ChunkStatistics> chunksHeadMap = subscriptionStatistics.unprocessedChunksByOffset.headMap(newSubscriptionInitialOffset, false);
        Iterator<Map.Entry<Long, ChunkStatistics>> chunksHeadMapEntryIterator = chunksHeadMap.entrySet().iterator();
        while(chunksHeadMapEntryIterator.hasNext()) {
            Map.Entry<Long, ChunkStatistics> chunksHeadMapEntry = chunksHeadMapEntryIterator.next();
            ChunkStatistics chunkStatistics = chunksHeadMapEntry.getValue();
            Iterator<Map.Entry<Long, Message>> chunkMessagesIterator = chunkStatistics.unprocessedMessagesByOffset.entrySet().iterator();
            while(chunkMessagesIterator.hasNext()) {
                Map.Entry<Long, Message> chunkMessageEntry = chunkMessagesIterator.next();
                long messageOffset = chunkMessageEntry.getKey();
                if(messageOffset < newSubscriptionInitialOffset) {
                    chunkMessagesIterator.remove();
                    chunkStatistics.processedMessages.incrementAndGet();
                }
            }
            if(chunkStatistics.isDone()) {
                chunksHeadMapEntryIterator.remove();
            }
        }
    }

    @Override
    public void handleChunk(long offset, long messageCount, long dataSize) {
        SubscriptionStatistics localSubscriptionStatistics = this.subscriptionStatistics.get();
        if(localSubscriptionStatistics == null) {
            LOGGER.warn(
                "handleChunk called for subscription that does not exist! "
                + "identifier={} offset={} messageCount={} dataSize={}",
                this.identifier,
                offset,
                messageCount,
                dataSize
            );
            return;
        }
        localSubscriptionStatistics.pendingChunks.decrementAndGet();
        localSubscriptionStatistics.unprocessedChunksByOffset.put(offset, new ChunkStatistics(offset, messageCount, dataSize));
    }

    @Override
    public void handleMessage(
            long offset,
            long chunkTimestamp,
            long committedChunkId,
            Message message
    ) {
        SubscriptionStatistics localSubscriptionStatistics = this.subscriptionStatistics.get();
        if(localSubscriptionStatistics == null) {
            LOGGER.warn(
                "handleMessage called for subscription that does not exist! "
                + "identifier={} offset={} chunkTimestamp={} committedChunkId={}",
                this.identifier,
                offset,
                chunkTimestamp,
                committedChunkId
            );
            return;
        }
        NavigableMap<Long, ChunkStatistics> subHeadMapByOffset = localSubscriptionStatistics.unprocessedChunksByOffset.headMap(offset, true);
        Map.Entry<Long, ChunkStatistics> lastOffsetToChunkEntry = subHeadMapByOffset.lastEntry();
        if(lastOffsetToChunkEntry == null) {
            LOGGER.warn(
                "handleMessage called but chunk was not found! "
                + "identifier={} offset={} chunkTimestamp={} committedChunkId={}",
                this.identifier,
                offset,
                chunkTimestamp,
                committedChunkId
            );
            return;
        }
        ChunkStatistics chunkStatistics = lastOffsetToChunkEntry.getValue();
        chunkStatistics.unprocessedMessagesByOffset.put(offset, message);
    }

    @Override
    public void handleUnsubscribe() {
        ConsumerStatisticRecorder.SubscriptionStatistics localSubscriptionStatistics = this.subscriptionStatistics.getAndSet(null);
        if(localSubscriptionStatistics == null) {
            LOGGER.warn(
                "handleUnsubscribe called for subscriptionId that does not exist! Identifier: {}", identifier
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
    @Override
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
        if(aggregatedMessageStatistics.subscriptionStatistics.offsetSpecification.isOffset()) {
            long initialOffset = aggregatedMessageStatistics.subscriptionStatistics.offsetSpecification.getOffset();
            // Old tracked message, should already be handled, probably a late acknowledgment of a defunct connection.
            if(aggregatedMessageStatistics.offset < initialOffset) {
                LOGGER.debug("Old message registered as consumed. Identifier={} Message Offset: {}, Start Offset: {}",
                        this.identifier,
                        aggregatedMessageStatistics.offset,
                        initialOffset);
                return true;
            }
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

    public AggregatedMessageStatistics retrieveStatistics(long offset) {
        SubscriptionStatistics localSubscriptionStatistics = this.subscriptionStatistics.get();
        if (localSubscriptionStatistics == null) {
            return null;
        }
        NavigableMap<Long, ChunkStatistics> chunkStatisticsHeadMap = localSubscriptionStatistics.unprocessedChunksByOffset.headMap(offset, true);
        Map.Entry<Long, ChunkStatistics> messageEntry = chunkStatisticsHeadMap.lastEntry();
        ChunkStatistics chunkStatistics = messageEntry == null ? null : messageEntry.getValue();
        return new AggregatedMessageStatistics(offset, localSubscriptionStatistics, chunkStatisticsHeadMap, chunkStatistics, messageEntry);
    }

    public AggregatedMessageStatistics retrieveStatistics(MessageHandler.Context messageContext) {
        return retrieveStatistics(messageContext.offset());
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

        private final AtomicInteger pendingChunks = new AtomicInteger(0);
        private OffsetSpecification offsetSpecification;
        private final NavigableMap<Long, ChunkStatistics> unprocessedChunksByOffset;

        public SubscriptionStatistics(OffsetSpecification offsetSpecification) {
            this(offsetSpecification, new ConcurrentSkipListMap<>());
        }

        public SubscriptionStatistics(OffsetSpecification offsetSpecification,
                                      NavigableMap<Long, ChunkStatistics> unprocessedChunksByOffset) {
            this.offsetSpecification = offsetSpecification;
            this.unprocessedChunksByOffset = unprocessedChunksByOffset;
        }

        public AtomicInteger getPendingChunks() {
            return pendingChunks;
        }

        public OffsetSpecification getOffsetSpecification() {
            return offsetSpecification;
        }

        public NavigableMap<Long, ChunkStatistics> getUnprocessedChunksByOffset() {
            return Collections.unmodifiableNavigableMap(unprocessedChunksByOffset);
        }

    }

    public static class ChunkStatistics {

        private final long offset;
        private final AtomicLong processedMessages = new AtomicLong();
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

    public String getIdentifier() {
        return identifier;
    }

    public SubscriptionStatistics getSubscriptionStatistics() {
        return subscriptionStatistics.get();
    }

    @Override
    public String toString() {
        return "ConsumerStatisticRecorder{" +
                "identifier='" + identifier + '\'' +
                ", subscriptionStatistics=" + subscriptionStatistics.get() +
                '}';
    }
}

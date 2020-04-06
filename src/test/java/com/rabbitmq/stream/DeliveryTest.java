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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class DeliveryTest {

    static final Codec NO_OP_CODEC = new Codec() {
        @Override
        public EncodedMessage encode(Message message) {
            return null;
        }

        @Override
        public Message decode(byte[] data) {
            return null;
        }

        @Override
        public MessageBuilder messageBuilder() {
            return null;
        }
    };

    ByteBuf generateFrameBuffer(int nbRecords, long chunkOffset, int dataSize, Iterable<byte[]> records) {
        ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(1024);
        bb.writeShort(Constants.COMMAND_DELIVER).writeShort(Constants.VERSION_0)
                .writeInt(1) // subscription id
                .writeByte(1) // magic and version
                .writeShort(nbRecords) // num entries
                .writeInt(nbRecords) // num records
                .writeLong(0) // epoch
                .writeLong(chunkOffset) // offset
                .writeInt(0) // CRC
                .writeInt(dataSize); // data size

        for (byte[] record : records) {
            bb.writeInt(record.length).writeBytes(record);
        }
        return bb;
    }

    Iterable<byte[]> generateRecords(int nbRecords) {
        Random random = new Random();
        List<byte[]> records = new ArrayList<>();
        for (int i = 0; i < nbRecords; i++) {
            // <<0=SimpleEntryType:1, Size:31/unsigned,Data:Size/binary>>
            int recordSize = random.nextInt(100) + 1;
            records.add(new byte[recordSize]);
        }
        return records;
    }

    int computeDataSize(Iterable<byte[]> records) {
        int dataSize = 0;
        for (byte[] record : records) {
            dataSize += 4 + record.length;
        }
        return dataSize;
    }

    @Test
    void handleDeliveryShouldFilterRecordsBeforeSubscriptionOffsetAndCallCallbacks() {
        class TestConfig {
            long chunkOffset, subscriptionOffset;

            public TestConfig(long chunkOffset, long subscriptionOffset) {
                this.chunkOffset = chunkOffset;
                this.subscriptionOffset = subscriptionOffset;
            }
        }

        Stream.of(
                new TestConfig(10, 10), // no filtering
                new TestConfig(10, 20), // filtering
                new TestConfig(Long.MAX_VALUE - 20, Long.MAX_VALUE - 10) // large unsigned long
        ).forEach(config -> {
            long chunkOffset = config.chunkOffset;
            long subscriptionOffset = config.subscriptionOffset;

            int nbRecords = (int) (subscriptionOffset - chunkOffset) + 100;
            Iterable<byte[]> records = generateRecords(nbRecords);
            int dataSize = computeDataSize(records);

            ByteBuf bb = generateFrameBuffer(nbRecords, chunkOffset, dataSize, records);

            int frameSize = bb.readableBytes();

            bb.readShort(); // read command key
            bb.readShort(); // read command version

            AtomicInteger chunkCountInCallback = new AtomicInteger();
            AtomicLong messageCountInCallback = new AtomicLong();

            List<Client.SubscriptionOffset> subscriptionOffsets = new ArrayList<>();
            if (chunkOffset != subscriptionOffset) {
                subscriptionOffsets.add(new Client.SubscriptionOffset(1, subscriptionOffset));
            }

            Client.handleDeliver(bb, null,
                    (client, subscriptionId, offset, recordCount, sizeOfData) -> {
                        assertThat(recordCount).isEqualTo(nbRecords);
                        chunkCountInCallback.incrementAndGet();
                    },
                    (subscriptionId, offset, message) -> messageCountInCallback.incrementAndGet(),
                    frameSize, NO_OP_CODEC, subscriptionOffsets);

            assertThat(chunkCountInCallback).hasValue(1);
            assertThat(messageCountInCallback).hasValue(nbRecords - (subscriptionOffset - chunkOffset));
            bb.release();
        });
    }

}

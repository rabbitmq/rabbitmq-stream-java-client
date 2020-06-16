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

package com.rabbitmq.stream.perf;

import com.rabbitmq.stream.Client;
import com.rabbitmq.stream.Constants;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamPerfTestTest {

    static StreamPerfTest.Topology topology() {
        return new StreamPerfTest.MapTopology(
                // order of replicas matter in the sample data
                // the BrokerLocator implementation can sort them for better spreading
                new HashMap<String, com.rabbitmq.stream.Client.StreamMetadata>() {{
                    put("stream1", new Client.StreamMetadata("stream1", Constants.RESPONSE_CODE_OK,
                            new Client.Broker("broker1", 5555),
                            Arrays.asList(new Client.Broker("broker3", 5555), new Client.Broker("broker2", 5555))
                    ));
                    put("stream2", new Client.StreamMetadata("stream2", Constants.RESPONSE_CODE_OK,
                            new Client.Broker("broker2", 5555),
                            Arrays.asList(new Client.Broker("broker3", 5555), new Client.Broker("broker1", 5555))
                    ));
                    put("stream3", new Client.StreamMetadata("stream2", Constants.RESPONSE_CODE_OK,
                            new Client.Broker("broker3", 5555),
                            Arrays.asList(new Client.Broker("broker2", 5555), new Client.Broker("broker1", 5555))
                    ));
                }});
    }

    @Test
    void writeReadLongInByteArray() {
        byte[] array = new byte[8];
        LongStream.of(
                Long.MIN_VALUE, Long.MAX_VALUE,
                1, 128, 256, 33_000, 66_000,
                1_000_000, new Random().nextLong()
        ).forEach(value -> {
            StreamPerfTest.writeLong(array, value);
            assertThat(StreamPerfTest.readLong(array)).isEqualTo(value);
        });
    }

    @Test
    void roundRobinAddressLocator() {
        StreamPerfTest.BrokerLocator locator = new StreamPerfTest.RoundRobinAddressLocator(
                Arrays.asList(
                        new StreamPerfTest.Address("broker1", 5555),
                        new StreamPerfTest.Address("broker2", 5555),
                        new StreamPerfTest.Address("broker3", 5555)
                )
        );

        assertThat(locator.get(null)).extracting("host").isEqualTo("broker1");
        assertThat(locator.get(null)).extracting("host").isEqualTo("broker2");
        assertThat(locator.get(null)).extracting("host").isEqualTo("broker3");
        assertThat(locator.get(null)).extracting("host").isEqualTo("broker1");
        assertThat(locator.get(null)).extracting("host").isEqualTo("broker2");
        assertThat(locator.get(null)).extracting("host").isEqualTo("broker3");
    }

    @Test
    void leaderOnlyBrokerLocator() {
        StreamPerfTest.BrokerLocator locator = new StreamPerfTest.LeaderOnlyBrokerLocator(topology());
        assertThat(locator.get("stream1")).extracting("host").isEqualTo("broker1");
        assertThat(locator.get("stream2")).extracting("host").isEqualTo("broker2");
        assertThat(locator.get("stream3")).extracting("host").isEqualTo("broker3");
    }

    @Test
    void roundRobinReplicaBrokerLocator() {
        StreamPerfTest.BrokerLocator locator = new StreamPerfTest.RoundRobinReplicaBrokerLocator(topology());
        assertThat(locator.get("stream1")).extracting("host").isEqualTo("broker2");
        assertThat(locator.get("stream1")).extracting("host").isEqualTo("broker3");
        assertThat(locator.get("stream1")).extracting("host").isEqualTo("broker2");
        assertThat(locator.get("stream1")).extracting("host").isEqualTo("broker3");

        assertThat(locator.get("stream2")).extracting("host").isEqualTo("broker1");
        assertThat(locator.get("stream2")).extracting("host").isEqualTo("broker3");
        assertThat(locator.get("stream2")).extracting("host").isEqualTo("broker1");
        assertThat(locator.get("stream2")).extracting("host").isEqualTo("broker3");

        assertThat(locator.get("stream3")).extracting("host").isEqualTo("broker1");
        assertThat(locator.get("stream3")).extracting("host").isEqualTo("broker2");
        assertThat(locator.get("stream3")).extracting("host").isEqualTo("broker1");
        assertThat(locator.get("stream3")).extracting("host").isEqualTo("broker2");
    }

}

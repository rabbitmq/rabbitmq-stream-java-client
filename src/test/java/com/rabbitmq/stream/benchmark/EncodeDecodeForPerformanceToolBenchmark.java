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

package com.rabbitmq.stream.benchmark;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.SimpleCodec;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.rabbitmq.stream.perf.StreamPerfTest.writeLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class EncodeDecodeForPerformanceToolBenchmark {

    @Param({"com.rabbitmq.stream.QpidProtonCodec", "com.rabbitmq.stream.SwiftMqCodec", "com.rabbitmq.stream.SimpleCodec"})
    String codecClass;

    @Param({"20"})
    int payloadSize;

    @Param({"nanoTime", "fixed"})
    String timestampSource;

    Codec codec;

    MessageProducingCallback messageProducingCallback;
    TimestampProvider timestampProvider;

    @Setup
    public void setUp() throws Exception {
        codec = (Codec) Class.forName(codecClass).getConstructor().newInstance();

        if (SimpleCodec.class.getName().equals(codecClass)) {
            messageProducingCallback = (timestamp, messageBuilder) -> {
                byte[] payload = new byte[payloadSize];
                writeLong(payload, timestamp);
                return messageBuilder.addData(payload).build();
            };
        } else {
            messageProducingCallback = (timestamp, messageBuilder) -> {
                byte[] payload = new byte[payloadSize];
                return messageBuilder.properties().creationTime(timestamp).messageBuilder()
                        .addData(payload).build();
            };
        }
        if ("fixed".equals(timestampSource)) {
            long fixedValue = System.nanoTime();
            this.timestampProvider = () -> fixedValue;
        } else {
            this.timestampProvider = () -> System.nanoTime();
        }
    }

    @TearDown
    public void tearDown() {

    }

    @Benchmark
    public void encode() {
        messageProducingCallback.create(timestampProvider.get(), codec.messageBuilder());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EncodeDecodeForPerformanceToolBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    private interface MessageProducingCallback {

        Message create(long timestamp, MessageBuilder messageBuilder);

    }

    private interface TimestampProvider {

        long get();

    }

}

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

package com.rabbitmq.stream.benchmark;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.rabbitmq.stream.perf.StreamPerfTest.readLong;
import static com.rabbitmq.stream.perf.StreamPerfTest.writeLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 1, time = 5)
@Fork(1)
@Threads(1)
public class EncodeDecodeForPerformanceToolBenchmark {

    @Param({"com.rabbitmq.stream.codec.QpidProtonCodec", "com.rabbitmq.stream.codec.SwiftMqCodec", "com.rabbitmq.stream.codec.SimpleCodec"})
    String codecClass;

    @Param({"20"})
    int payloadSize;

    @Param({"nanoTime", "fixed"})
    String timestampSource;

    Codec codec;

    TimestampProvider timestampProvider;

    byte[] messageToDecode;

    @Setup
    public void setUp() throws Exception {
        codec = (Codec) Class.forName(codecClass).getConstructor().newInstance();

        if ("fixed".equals(timestampSource)) {
            long fixedValue = System.nanoTime();
            this.timestampProvider = () -> fixedValue;
        } else {
            this.timestampProvider = () -> System.nanoTime();
        }

        byte[] payload = new byte[payloadSize];
        writeLong(payload, timestampProvider.get());
        Codec.EncodedMessage encoded = codec.encode(codec.messageBuilder().addData(payload).build());
        messageToDecode = new byte[encoded.getSize()];
        System.arraycopy(encoded.getData(), 0, messageToDecode, 0, encoded.getSize());
    }

    @Benchmark
    public void encode() {
        byte[] payload = new byte[payloadSize];
        writeLong(payload, timestampProvider.get());
        codec.messageBuilder().addData(payload).build();
    }

    @Benchmark
    public void decode() {
        Message message = codec.decode(messageToDecode);
        long latency = timestampProvider.get() - readLong(message.getBodyAsBinary());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EncodeDecodeForPerformanceToolBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    private interface TimestampProvider {

        long get();

    }

}

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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@Threads(1)
public class EncodingDecodingBenchmark {

    @Param({"com.rabbitmq.stream.codec.QpidProtonCodec", "com.rabbitmq.stream.codec.SwiftMqCodec"})
    String codecClass;

    @Param({"20"})
    int payloadSize;

    Codec codec;
    byte[] payload;

    byte[] messageToDecode;

    @Setup
    public void setUp() throws Exception {
        codec = (Codec) Class.forName(codecClass).getConstructor().newInstance();
        payload = new byte[payloadSize];
        IntStream.range(0, payloadSize).forEach(i -> payload[i] = 0);

        Codec.EncodedMessage encoded = codec.encode(codec.messageBuilder()
                .properties().messageId(1L)
                .messageBuilder()
                .addData(payload).build());

        messageToDecode = new byte[encoded.getSize()];
        System.arraycopy(encoded.getData(), 0, messageToDecode, 0, encoded.getSize());
    }

    @Benchmark
    public void encode() {
        codec.encode(codec.messageBuilder()
                .properties().messageId(1L)
                .messageBuilder()
                .addData(payload).build());
    }

    @Benchmark
    public void decode() {
        codec.decode(messageToDecode);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EncodingDecodingBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}

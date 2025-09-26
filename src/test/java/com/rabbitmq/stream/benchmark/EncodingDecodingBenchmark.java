// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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

    Codec.EncodedMessage encoded =
        codec.encode(
            codec
                .messageBuilder()
                .properties()
                .messageId(1L)
                .messageBuilder()
                .addData(payload)
                .build());

    messageToDecode = new byte[encoded.getSize()];
    System.arraycopy(encoded.getData(), 0, messageToDecode, 0, encoded.getSize());
  }

  @Benchmark
  public void encode() {
    codec.encode(
        codec
            .messageBuilder()
            .properties()
            .messageId(1L)
            .messageBuilder()
            .addData(payload)
            .build());
  }

  @Benchmark
  public void decode() {
    codec.decode(messageToDecode);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(EncodingDecodingBenchmark.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}

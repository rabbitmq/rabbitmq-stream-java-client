// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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
import com.rabbitmq.stream.Message;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 1, time = 5)
@Fork(1)
@Threads(1)
public class EncodeDecodeForPerformanceToolBenchmark {

  @Param({
    "com.rabbitmq.stream.codec.QpidProtonCodec",
    "com.rabbitmq.stream.codec.SwiftMqCodec",
    "com.rabbitmq.stream.codec.SimpleCodec"
  })
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
    Options opt =
        new OptionsBuilder()
            .include(EncodeDecodeForPerformanceToolBenchmark.class.getSimpleName())
            .build();

    new Runner(opt).run();
  }

  private interface TimestampProvider {

    long get();
  }

  private static void writeLong(byte[] array, long value) {
    // from Guava Longs
    for (int i = 7; i >= 0; i--) {
      array[i] = (byte) (value & 0xffL);
      value >>= 8;
    }
  }

  private static long readLong(byte[] array) {
    // from Guava Longs
    return (array[0] & 0xFFL) << 56
        | (array[1] & 0xFFL) << 48
        | (array[2] & 0xFFL) << 40
        | (array[3] & 0xFFL) << 32
        | (array[4] & 0xFFL) << 24
        | (array[5] & 0xFFL) << 16
        | (array[6] & 0xFFL) << 8
        | (array[7] & 0xFFL);
  }
}

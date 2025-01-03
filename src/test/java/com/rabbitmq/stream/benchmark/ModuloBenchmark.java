// Copyright (c) 2021-2025 Broadcom. All Rights Reserved.
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.commons.codec.digest.MurmurHash3;
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
@Measurement(iterations = 3, time = 5)
@Fork(1)
@Threads(1)
public class ModuloBenchmark {

  static final int partitionSize = 3;

  static ConcurrentHashMap<String, IntToIntFunction> MODULOS =
      new ConcurrentHashMap<String, IntToIntFunction>() {
        {
          put("mod", i -> i % partitionSize);
          put("remainderUnsigned", i -> Integer.remainderUnsigned(i, partitionSize));
        }
      };

  @Param({"mod", "remainderUnsigned"})
  String algorithm;

  IntToIntFunction modulo;

  int[] content;

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(ModuloBenchmark.class.getSimpleName())
            .warmupIterations(3)
            .measurementIterations(2)
            .forks(1)
            .build();

    new Runner(opt).run();
  }

  @Setup
  public void setUp() {
    modulo = MODULOS.get(algorithm);
    content =
        IntStream.range(0, 10)
            .mapToObj(i -> "hello-" + i)
            .mapToInt(
                s ->
                    MurmurHash3.hash32x86(
                        s.getBytes(StandardCharsets.UTF_8), 0, s.length(), 104729))
            .toArray();
  }

  @Benchmark
  public void modulo() {
    for (int i : content) {
      modulo.applyAsLong(i);
    }
  }

  @FunctionalInterface
  public interface IntToIntFunction {

    int applyAsLong(int value);
  }
}

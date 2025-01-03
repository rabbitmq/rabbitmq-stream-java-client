// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import java.util.zip.Checksum;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.codec.digest.XXHash32;
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
public class HashAlgorithmBenchmark {

  static ConcurrentHashMap<String, ToIntFunction<String>> HASHES =
      new ConcurrentHashMap<String, ToIntFunction<String>>() {
        {
          put("hashCode", d -> d.hashCode());
          put("murmur3", d -> MurmurHash3.hash32x86(d.getBytes(StandardCharsets.UTF_8)));
          put("murmur2", d -> MurmurHash2.hash32(d));
          put(
              "murmur3-guava",
              d -> {
                HashFunction hashFunction = Hashing.murmur3_32_fixed();
                return hashFunction.hashString(d, StandardCharsets.UTF_8).asInt();
              });
          put(
              "farmhash",
              d -> {
                HashFunction hashFunction = Hashing.farmHashFingerprint64();
                return hashFunction.hashString(d, StandardCharsets.UTF_8).asInt();
              });
          put(
              "xxhash32",
              d -> {
                Checksum hash = new XXHash32();
                byte[] data = d.getBytes(StandardCharsets.UTF_8);
                hash.update(data, 0, data.length);
                return (int) hash.getValue();
              });
        }
      };

  @Param({"hashCode", "farmhash", "murmur2", "murmur3", "murmur3-guava", "xxhash32"})
  String algorithm;

  ToIntFunction<String> hash;

  String content;

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(HashAlgorithmBenchmark.class.getSimpleName())
            .warmupIterations(3)
            .measurementIterations(2)
            .forks(1)
            .build();

    new Runner(opt).run();
  }

  @Setup
  public void setUp() {
    hash = HASHES.get(algorithm);
    content = UUID.randomUUID().toString();
  }

  @Benchmark
  public void hash() {
    hash.applyAsInt(content);
  }
}

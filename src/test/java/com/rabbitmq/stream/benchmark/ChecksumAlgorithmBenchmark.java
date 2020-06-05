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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(1)
@Threads(1)
public class ChecksumAlgorithmBenchmark {

    static Map<String, Supplier<Checksum>> CHECKSUMS = new ConcurrentHashMap<String, Supplier<Checksum>>() {{
        put("crc32", () -> new CRC32());
        put("adler32", () -> new Adler32());
    }};

    @Param({"crc32", "adler32"})
    String algorithm;

    Supplier<Checksum> supplier;

    byte[] content;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ChecksumAlgorithmBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() {
        supplier = CHECKSUMS.get(algorithm);
        content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    @Benchmark
    public void checksum() {
        Checksum checksum = supplier.get();
        checksum.update(content, 0, content.length);
        checksum.getValue();
    }

}

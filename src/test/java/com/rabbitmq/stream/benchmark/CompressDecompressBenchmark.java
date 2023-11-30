// Copyright (c) 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
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

import com.rabbitmq.stream.compression.CompressionCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.openjdk.jmh.profile.GCProfiler;
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
public class CompressDecompressBenchmark {
  @Param({"ZstdJniCompressionCodec"})
  String codecClass;

  CompressionCodec codec;
  byte[] plainData;
  byte[] compressedData;
  ByteBuf compressedDataBb;
  ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(CompressDecompressBenchmark.class.getSimpleName())
            .forks(1)
            .addProfiler(GCProfiler.class)
            .build();

    new Runner(opt).run();
  }

  @Setup
  public void setUp() throws Exception {
    codec =
        (CompressionCodec)
            Class.forName("com.rabbitmq.stream.compression.CompressionUtils$" + codecClass)
                .getConstructor()
                .newInstance();
    plainData =
        IntStream.range(0, 100)
            .mapToObj(i -> UUID.randomUUID().toString())
            .flatMap(v -> Stream.of(v, v))
            .collect(Collectors.joining())
            .getBytes(StandardCharsets.UTF_8);

    int maxCompressedLength = codec.maxCompressedLength(plainData.length);
    ByteBuf bb = allocator.heapBuffer(maxCompressedLength);
    OutputStream compress = codec.compress(new ByteBufOutputStream(bb));
    compress.write(plainData);
    compress.flush();
    compress.close();
    compressedData = new byte[bb.writerIndex()];
    System.arraycopy(bb.array(), 0, compressedData, 0, compressedData.length);
    compressedDataBb = allocator.buffer(compressedData.length);
    compressedDataBb.writeBytes(bb.array(), 0, compressedData.length);
    bb.release();
  }

  @Benchmark
  public void compress() throws Exception {
    int maxCompressedLength = codec.maxCompressedLength(plainData.length);
    ByteBuf bb = allocator.buffer(maxCompressedLength);
    OutputStream compress = codec.compress(new ByteBufOutputStream(bb));
    compress.write(plainData);
    compress.flush();
    compress.close();
    bb.release();
  }

  @Benchmark
  public void decodeReadBytePerByte() throws Exception {
    compressedDataBb.readerIndex(0);
    ByteBuf outBb = allocator.buffer(compressedData.length);
    InputStream inputStream = codec.decompress(new ByteBufInputStream(compressedDataBb));
    int n;
    while (-1 != (n = inputStream.read())) {
      outBb.writeByte(n);
    }

    //    if (outBb.writerIndex() != toCompress.length) {
    //      throw new IllegalStateException();
    //    }
    //    for (int i = 0; i < toCompress.length; i++) {
    //      if (outBb.array()[i] != toCompress[i]) {
    //        throw new IllegalStateException();
    //      }
    //    }

    outBb.release();
  }

  @Benchmark
  public void decodePreAllocatedArray() throws Exception {
    compressedDataBb.readerIndex(0);
    InputStream inputStream = codec.decompress(new ByteBufInputStream(compressedDataBb));
    ByteBuf outBb = allocator.buffer(plainData.length);
    byte[] inBuffer = new byte[compressedData.length];
    int n;
    while (-1 != (n = inputStream.read(inBuffer))) {
      outBb.writeBytes(inBuffer, 0, n);
    }

    //    if (outBb.writerIndex() != toCompress.length) {
    //      throw new IllegalStateException();
    //    }
    //    for (int i = 0; i < toCompress.length; i++) {
    //      if (outBb.array()[i] != toCompress[i]) {
    //        throw new IllegalStateException();
    //      }
    //    }

    outBb.release();
  }

  @Benchmark
  public void decodeWriteInPreAllocatedHeapByteBuf() throws Exception {
    compressedDataBb.readerIndex(0);
    InputStream inputStream = codec.decompress(new ByteBufInputStream(compressedDataBb));
    ByteBuf outBb = allocator.heapBuffer(plainData.length);
    inputStream.read(outBb.array(), 0, plainData.length);

    //    if (read != plainData.length) {
    //      throw new IllegalStateException();
    //    }
    //    outBb.readerIndex(0);
    //    outBb.writerIndex(plainData.length);
    //
    //        if (outBb.writerIndex() != plainData.length) {
    //          throw new IllegalStateException();
    //        }
    //        for (int i = 0; i < plainData.length; i++) {
    //          if (outBb.array()[i] != plainData[i]) {
    //            throw new IllegalStateException();
    //          }
    //        }

    outBb.release();
  }
}

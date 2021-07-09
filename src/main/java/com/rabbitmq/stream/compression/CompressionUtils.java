// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.compression;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import net.jpountz.lz4.LZ4FrameOutputStream.FLG;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.BlockSize;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.Parameters;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyDialect;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

/** Implementation of {@link CompressionCodec}s. */
public final class CompressionUtils {

  private CompressionUtils() {}

  public static class GzipCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      return sourceLength;
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new GZIPOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating GZIP compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new GZIPInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating GZIP compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.GZIP.code;
    }

    @Override
    public String toString() {
      return "GZIP codec (JDK)";
    }
  }

  public static class ZstdJniCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      return (int) Zstd.compressBound(sourceLength);
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new ZstdOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Zstd compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new ZstdInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Zstd compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.ZSTD.code;
    }

    @Override
    public String toString() {
      return "Zstd codec (JNI)";
    }
  }

  public static class Lz4JavaCompressionCodec implements CompressionCodec {

    private static final FLG.Bits[] DEFAULT_FEATURES = new FLG.Bits[] {FLG.Bits.BLOCK_INDEPENDENCE};

    @Override
    public int maxCompressedLength(int sourceLength) {
      // from LZ4Utils.maxCompressedLength(int)
      return sourceLength + sourceLength / 255 + 16;
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new LZ4FrameOutputStream(outputStream, BLOCKSIZE.SIZE_64KB, DEFAULT_FEATURES);
      } catch (IOException e) {
        throw new CompressionException("Error while creating LZ4 compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new LZ4FrameInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating LZ4 compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.LZ4.code;
    }

    @Override
    public String toString() {
      return "LZ4 codec";
    }
  }

  public static class XerialSnappyCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      return Snappy.maxCompressedLength(sourceLength);
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new SnappyFramedOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating snappy compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new SnappyFramedInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating snappy compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.SNAPPY.code;
    }

    @Override
    public String toString() {
      return "Snappy codec (xerial)";
    }
  }

  public static class CommonsCompressGzipCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      return sourceLength;
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new GzipCompressorOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating GZIP compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new GzipCompressorInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating GZIP compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.GZIP.code;
    }

    @Override
    public String toString() {
      return "GZIP codec (Commons Compress)";
    }
  }

  public static class CommonsCompressSnappyCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      // from Xerial's pure Java implementation
      return 32 + sourceLength + sourceLength / 6;
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new FramedSnappyCompressorOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Snappy compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new FramedSnappyCompressorInputStream(
            inputStream, SnappyFramedOutputStream.DEFAULT_BLOCK_SIZE, FramedSnappyDialect.STANDARD);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Snappy compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.SNAPPY.code;
    }

    @Override
    public String toString() {
      return "Snappy codec (Commons Compress)";
    }
  }

  public static class CommonsCompressLz4CompressionCodec implements CompressionCodec {

    private static final Parameters DEFAULT = new Parameters(BlockSize.K64, true, false, false);

    @Override
    public int maxCompressedLength(int sourceLength) {
      // from LZ4Utils.maxCompressedLength(int)
      return sourceLength + sourceLength / 255 + 16;
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new FramedLZ4CompressorOutputStream(outputStream, DEFAULT);
      } catch (IOException e) {
        throw new CompressionException("Error while creating LZ4 compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new FramedLZ4CompressorInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating LZ4 compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.LZ4.code;
    }

    @Override
    public String toString() {
      return "LZ4 codec (Commons Compress)";
    }
  }

  public static class CommonsCompressZstdCompressionCodec implements CompressionCodec {

    @Override
    public int maxCompressedLength(int sourceLength) {
      return (int) Zstd.compressBound(sourceLength);
    }

    @Override
    public OutputStream compress(OutputStream outputStream) {
      try {
        return new ZstdCompressorOutputStream(outputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Zstd compression output stream", e);
      }
    }

    @Override
    public InputStream decompress(InputStream inputStream) {
      try {
        return new ZstdCompressorInputStream(inputStream);
      } catch (IOException e) {
        throw new CompressionException("Error while creating Zstd compression input stream", e);
      }
    }

    @Override
    public byte code() {
      return Compression.ZSTD.code;
    }

    @Override
    public String toString() {
      return "Zstd codec (Commons Compress)";
    }
  }
}

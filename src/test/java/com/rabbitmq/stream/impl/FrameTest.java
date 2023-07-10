// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class FrameTest {

  TestUtils.ClientFactory cf;

  @Test
  void messageTooBigToFitInOneFrameShouldThrowException() {
    try (Client client = cf.get(new Client.ClientParameters().requestedMaxFrameSize(1024))) {
      byte[] binary = new byte[1000];
      Message message =
          new Message() {
            @Override
            public boolean hasPublishingId() {
              return false;
            }

            @Override
            public long getPublishingId() {
              return 0;
            }

            @Override
            public byte[] getBodyAsBinary() {
              return binary;
            }

            @Override
            public Object getBody() {
              return null;
            }

            @Override
            public Properties getProperties() {
              return null;
            }

            @Override
            public Map<String, Object> getApplicationProperties() {
              return null;
            }

            @Override
            public Map<String, Object> getMessageAnnotations() {
              return null;
            }
          };
      List<ThrowableAssert.ThrowingCallable> publishCalls =
          Arrays.asList(() -> client.publish(b(1), Arrays.asList(message)));
      publishCalls.forEach(
          callable -> assertThatThrownBy(callable).isInstanceOf(IllegalArgumentException.class));
    }
  }

  @Test
  void frameTooLargeShouldTriggerCloseFromServer() throws Exception {
    int maxFrameSize = 1024;
    try (Client client =
        cf.get(new Client.ClientParameters().requestedMaxFrameSize(maxFrameSize))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(out);
      int frameSize = 1021;
      dataOutputStream.writeInt(frameSize);
      dataOutputStream.write(new byte[frameSize]);
      client.send(out.toByteArray());
      waitAtMost(10, () -> client.isOpen() == false);
    }
  }

  @Test
  void splitPublishedMessagesToFitMaxFrameSize() {
    int maxFrameSize = 1024;
    class TestDesc {
      final String description;
      final List<Integer> sizes;
      final int expectedCalls;

      public TestDesc(String description, List<Integer> sizes, int expectedCalls) {
        this.description = description;
        this.sizes = sizes;
        this.expectedCalls = expectedCalls;
      }
    }
    List<TestDesc> tests =
        Arrays.asList(
            new TestDesc("1 message that fits in frame", Arrays.asList(512), 1),
            new TestDesc(
                "x messages that fits in their respective frame", Arrays.asList(768, 768, 768), 3),
            new TestDesc(
                "4 messages per frame",
                IntStream.range(0, 10).map(i -> 200).boxed().collect(Collectors.toList()),
                3),
            new TestDesc(
                "8 messages, 4 messages per frame",
                IntStream.range(0, 8).map(i -> 200).boxed().collect(Collectors.toList()),
                2),
            new TestDesc(
                "9 messages, 4 messages per frame",
                IntStream.range(0, 9).map(i -> 200).boxed().collect(Collectors.toList()),
                3));
    try (Client client =
        cf.get(new Client.ClientParameters().requestedMaxFrameSize(maxFrameSize))) {
      ToLongFunction<Object> publishSequenceFunction = publishSequenceFunction();
      tests.forEach(
          test -> {
            Channel channel = Mockito.mock(Channel.class);
            Mockito.when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
            Mockito.when(channel.writeAndFlush(Mockito.any()))
                .thenReturn(Mockito.mock(ChannelFuture.class));

            client.publishInternal(
                Constants.VERSION_1,
                channel,
                b(1),
                test.sizes.stream()
                    .map(size -> new Codec.EncodedMessage(size, new byte[size]))
                    .collect(Collectors.toList()),
                Client.OUTBOUND_MESSAGE_WRITE_CALLBACK,
                publishSequenceFunction);

            ArgumentCaptor<ByteBuf> bbCaptor = ArgumentCaptor.forClass(ByteBuf.class);
            verify(channel, times(test.expectedCalls)).writeAndFlush(bbCaptor.capture());
            bbCaptor
                .getAllValues()
                .forEach(
                    bb -> {
                      assertThat(bb.capacity()).isLessThanOrEqualTo(maxFrameSize);
                      bb.release();
                    });
          });
    }
  }

  static ToLongFunction<Object> publishSequenceFunction() {
    return new ToLongFunction<Object>() {
      private final AtomicLong publishSequence = new AtomicLong(0);

      @Override
      public long applyAsLong(Object value) {
        return publishSequence.getAndIncrement();
      }
    };
  }
}

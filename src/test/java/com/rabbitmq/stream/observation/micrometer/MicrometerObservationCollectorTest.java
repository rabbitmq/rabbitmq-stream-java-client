// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.observation.micrometer;

import static com.rabbitmq.stream.OffsetSpecification.first;
import static com.rabbitmq.stream.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.TestUtils;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.SpansAssert;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;

public class MicrometerObservationCollectorTest {

  private static final byte[] PAYLOAD = "msg".getBytes(StandardCharsets.UTF_8);

  @ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
  private abstract static class IntegrationTest extends SampleTestRunner {

    String stream;
    EventLoopGroup eventLoopGroup;

    EnvironmentBuilder environmentBuilder() {
      return Environment.builder()
          .netty()
          .eventLoopGroup(eventLoopGroup)
          .environmentBuilder()
          .addressResolver(add -> localhost());
    }

    @Override
    public TracingSetup[] getTracingSetup() {
      return new TracingSetup[] {TracingSetup.IN_MEMORY_BRAVE, TracingSetup.ZIPKIN_BRAVE};
    }
  }

  @Nested
  class PublishConsume extends IntegrationTest {

    @Override
    public SampleTestRunnerConsumer yourCode() {
      return (buildingBlocks, meterRegistry) -> {
        try (Environment env =
            environmentBuilder()
                .observationCollector(
                    new MicrometerObservationCollectorBuilder()
                        .registry(getObservationRegistry())
                        .build())
                .build()) {
          Producer producer = env.producerBuilder().stream(stream).build();
          CountDownLatch publishLatch = new CountDownLatch(1);
          producer.send(
              producer.messageBuilder().addData(PAYLOAD).build(),
              status -> publishLatch.countDown());

          assertThat(publishLatch).is(completed());

          CountDownLatch consumeLatch = new CountDownLatch(1);
          env.consumerBuilder().stream(stream)
              .offset(first())
              .messageHandler((ctx, msg) -> consumeLatch.countDown())
              .build();

          assertThat(consumeLatch).is(completed());

          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 2);

          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(2);
          SpanAssert.assertThat(buildingBlocks.getFinishedSpans().get(0))
              .hasNameEqualTo(stream + " publish")
              .hasTag("messaging.destination.name", stream)
              .hasTag("net.protocol.name", "rabbitmq-stream")
              .hasTag("net.protocol.version", "1.0");
          SpanAssert.assertThat(buildingBlocks.getFinishedSpans().get(1))
              .hasNameEqualTo(stream + " process")
              .hasTag("messaging.destination.name", stream)
              .hasTag("messaging.source.name", stream)
              .hasTag("messaging.message.payload_size_bytes", String.valueOf(PAYLOAD.length))
              .hasTag("net.protocol.name", "rabbitmq-stream")
              .hasTag("net.protocol.version", "1.0");
          waitAtMost(
              () ->
                  getMeterRegistry().find("rabbitmq.stream.publish").timer() != null
                      && getMeterRegistry().find("rabbitmq.stream.process").timer() != null);
          getMeterRegistry()
              .get("rabbitmq.stream.publish")
              .tag("messaging.operation", "publish")
              .tag("messaging.system", "rabbitmq")
              .timer();
          getMeterRegistry()
              .get("rabbitmq.stream.process")
              .tag("messaging.operation", "process")
              .tag("messaging.system", "rabbitmq")
              .timer();
        }

        /*
          assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
          waitAtMost(() -> buildingBlocks.getFinishedSpans().size() == 2);
          SpansAssert.assertThat(buildingBlocks.getFinishedSpans()).haveSameTraceId().hasSize(2);
          SpanAssert.assertThat(buildingBlocks.getFinishedSpans().get(0))
              .hasNameEqualTo("metrics.queue publish")
              .hasTag("messaging.rabbitmq.destination.routing_key", "metrics.queue")
              .hasTag("messaging.destination.name", "amq.default")
              .hasTag("messaging.message.payload_size_bytes", String.valueOf(PAYLOAD.length))
              .hasTagWithKey("net.sock.peer.addr")
              .hasTag("net.sock.peer.port", "5672")
              .hasTag("net.protocol.name", "amqp")
              .hasTag("net.protocol.version", "0.9.1");
          SpanAssert.assertThat(buildingBlocks.getFinishedSpans().get(1))
              .hasNameEqualTo("metrics.queue process")
              .hasTag("messaging.rabbitmq.destination.routing_key", "metrics.queue")
              .hasTag("messaging.destination.name", "amq.default")
              .hasTag("messaging.source.name", "metrics.queue")
              .hasTag("messaging.message.payload_size_bytes", String.valueOf(PAYLOAD.length))
              .hasTag("net.protocol.name", "amqp")
              .hasTag("net.protocol.version", "0.9.1");
          waitAtMost(
              () ->
                  getMeterRegistry().find("rabbitmq.publish").timer() != null
                      && getMeterRegistry().find("rabbitmq.process").timer() != null);
          getMeterRegistry()
              .get("rabbitmq.publish")
              .tag("messaging.operation", "publish")
              .tag("messaging.system", "rabbitmq")
              .timer();
          getMeterRegistry()
              .get("rabbitmq.process")
              .tag("messaging.operation", "process")
              .tag("messaging.system", "rabbitmq")
              .timer();
        } finally {
          safeClose(publishConnection);
          safeClose(consumeConnection);
        }

         */
      };
    }
  }
}

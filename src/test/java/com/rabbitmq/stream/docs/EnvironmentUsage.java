// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

package com.rabbitmq.stream.docs;

import com.rabbitmq.stream.*;

import com.rabbitmq.stream.observation.micrometer.MicrometerObservationCollectorBuilder;
import io.micrometer.observation.ObservationRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.FileInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;

public class EnvironmentUsage {

    void environmentCreation() throws Exception {
        // tag::environment-creation[]
        Environment environment = Environment.builder().build();  // <1>
        // ...
        environment.close(); // <2>
        // end::environment-creation[]
    }

    void environmentCreationWithUri() {
        // tag::environment-creation-with-uri[]
        Environment environment = Environment.builder()
                .uri("rabbitmq-stream://guest:guest@localhost:5552/%2f")  // <1>
                .build();
        // end::environment-creation-with-uri[]
    }

    void environmentCreationWithUris() {
        // tag::environment-creation-with-uris[]
        Environment environment = Environment.builder()
                .uris(Arrays.asList(                     // <1>
                        "rabbitmq-stream://host1:5552",
                        "rabbitmq-stream://host2:5552",
                        "rabbitmq-stream://host3:5552")
                )
                .build();
        // end::environment-creation-with-uris[]
    }

    void environmentCreationWithTls() throws Exception {
        // tag::environment-creation-with-tls[]
        X509Certificate certificate;
        try (FileInputStream inputStream =
                    new FileInputStream("/path/to/ca_certificate.pem")) {
            CertificateFactory fact = CertificateFactory.getInstance("X.509");
            certificate = (X509Certificate) fact.generateCertificate(inputStream); // <1>
        }
        SslContext sslContext = SslContextBuilder
            .forClient()
            .trustManager(certificate)  // <2>
            .build();

        Environment environment = Environment.builder()
            .uri("rabbitmq-stream+tls://guest:guest@localhost:5551/%2f")  // <3>
            .tls().sslContext(sslContext)  // <4>
            .environmentBuilder()
            .build();
        // end::environment-creation-with-tls[]
    }

    void environmentCreationWithTlsTrustEverything() throws Exception {
        // tag::environment-creation-with-tls-trust-everything[]
        Environment environment = Environment.builder()
            .uri("rabbitmq-stream+tls://guest:guest@localhost:5551/%2f")
            .tls().trustEverything()  // <1>
            .environmentBuilder()
            .build();
        // end::environment-creation-with-tls-trust-everything[]
    }

    void addressResolver() throws Exception {
        // tag::address-resolver[]
        Address entryPoint = new Address("my-load-balancer", 5552);  // <1>
        Environment environment = Environment.builder()
            .host(entryPoint.host())  // <2>
            .port(entryPoint.port())  // <2>
            .addressResolver(address -> entryPoint)  // <3>
            .locatorConnectionCount(3)  // <4>
            .build();
        // end::address-resolver[]
    }

    void createStream() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation[]
        environment.streamCreator().stream("my-stream").create();  // <1>
        // end::stream-creation[]
    }

    void createStreamWithRetention() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation-retention[]
        environment.streamCreator()
                .stream("my-stream")
                .maxLengthBytes(ByteCapacity.GB(10))  // <1>
                .maxSegmentSizeBytes(ByteCapacity.MB(500))  // <2>
                .create();
        // end::stream-creation-retention[]
    }

    void createStreamWithTimeBasedRetention() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation-time-based-retention[]
        environment.streamCreator()
                .stream("my-stream")
                .maxAge(Duration.ofHours(6))  // <1>
                .maxSegmentSizeBytes(ByteCapacity.MB(500))  // <2>
                .create();
        // end::stream-creation-time-based-retention[]
    }

    void deleteStream() {
        Environment environment = Environment.builder().build();
        // tag::stream-deletion[]
        environment.deleteStream("my-stream");  // <1>
        // end::stream-deletion[]
    }

    void nativeEpoll() {
        // tag::native-epoll[]
        EventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup();  // <1>
        Environment environment = Environment.builder()
            .netty()  // <2>
                .eventLoopGroup(epollEventLoopGroup)  // <3>
                .bootstrapCustomizer(b -> b.channel(EpollSocketChannel.class))  // <4>
                .environmentBuilder()
            .build();
        // end::native-epoll[]
    }

    void micrometerObservation() {
        ObservationRegistry observationRegistry = ObservationRegistry.NOOP;
        // tag::micrometer-observation[]
        Environment environment = Environment.builder()
            .observationCollector(new MicrometerObservationCollectorBuilder()  // <1>
                .registry(observationRegistry).build())  // <2>
            .build();
        // end::micrometer-observation[]
    }

}

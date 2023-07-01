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
package com.rabbitmq.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HashUtilsTest {

  @ParameterizedTest
  @CsvSource({"hello,1321743225", "brave,3825276426", "new,2970740106", "world,2453398188"})
  void murmur3WithDefaultSeed(String input, String unsignedHash) {
    int expectedHash = Integer.parseUnsignedInt(unsignedHash);
    assertThat(HashUtils.MURMUR3.applyAsInt(input)).isEqualTo(expectedHash);
  }

  @ParameterizedTest
  @CsvSource({"hello,3806057185", "brave,619588758", "new,1483300585", "world,1145629360"})
  void murmur3WithSeed(String input, String unsignedHash) {
    int expectedHash = Integer.parseUnsignedInt(unsignedHash);
    HashUtils.Murmur3 hash = new HashUtils.Murmur3(42);
    assertThat(hash.applyAsInt(input)).isEqualTo(expectedHash);
  }
}

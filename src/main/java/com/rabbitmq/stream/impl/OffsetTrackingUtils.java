// Copyright (c) 2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay;
import static com.rabbitmq.stream.impl.AsyncRetry.asyncRetry;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OffsetTrackingUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetTrackingUtils.class);

  private OffsetTrackingUtils() {}

  static long storedOffset(Supplier<Client> clientSupplier, String name, String stream) {
    // the client can be null, so we catch any exception
    Client.QueryOffsetResponse response;
    try {
      response = clientSupplier.get().queryOffset(name, stream);
    } catch (Exception e) {
      throw new IllegalStateException(
          format(
              "Not possible to query offset for name %s on stream %s for now: %s",
              name, stream, e.getMessage()),
          e);
    }
    if (response.isOk()) {
      return response.getOffset();
    } else if (response.getResponseCode() == Constants.RESPONSE_CODE_NO_OFFSET) {
      throw new NoOffsetException(
          format(
              "No offset stored for name %s on stream %s (%s)",
              name, stream, Utils.formatConstant(response.getResponseCode())));
    } else {
      throw new StreamException(
          format(
              "QueryOffset for name %s on stream %s returned an error (%s)",
              name, stream, Utils.formatConstant(response.getResponseCode())),
          response.getResponseCode());
    }
  }

  static void waitForOffsetToBeStored(
      String caller,
      ScheduledExecutorService scheduledExecutorService,
      LongSupplier offsetSupplier,
      String name,
      String stream,
      long expectedStoredOffset) {
    String reference = String.format("{stream=%s/name=%s}", stream, name);
    CompletableFuture<Boolean> storedTask =
        asyncRetry(
                () -> {
                  try {
                    long lastStoredOffset = offsetSupplier.getAsLong();
                    boolean stored = lastStoredOffset == expectedStoredOffset;
                    LOGGER.debug(
                        "Last stored offset from {} on {} is {}, expecting {}",
                        caller,
                        reference,
                        lastStoredOffset,
                        expectedStoredOffset);
                    if (!stored) {
                      throw new IllegalStateException();
                    } else {
                      return true;
                    }
                  } catch (StreamException e) {
                    if (e.getCode() == Constants.RESPONSE_CODE_NO_OFFSET) {
                      LOGGER.debug(
                          "No stored offset for {} on {}, expecting {}",
                          caller,
                          reference,
                          expectedStoredOffset);
                      throw new IllegalStateException();
                    } else {
                      throw e;
                    }
                  }
                })
            .description(
                "Last stored offset for %s on %s must be %d",
                caller, reference, expectedStoredOffset)
            .delayPolicy(fixedWithInitialDelay(ofMillis(200), ofMillis(200)))
            .retry(exception -> exception instanceof IllegalStateException)
            .scheduler(scheduledExecutorService)
            .build();

    try {
      storedTask.get(10, TimeUnit.SECONDS);
      LOGGER.debug("Offset {} stored ({}, {})", expectedStoredOffset, caller, reference);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException e) {
      LOGGER.warn("Error while checking offset has been stored", e);
      storedTask.cancel(true);
    }
  }
}

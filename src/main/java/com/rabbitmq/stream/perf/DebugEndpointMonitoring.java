// Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.perf;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import picocli.CommandLine.Option;

class DebugEndpointMonitoring implements Monitoring {

  @Option(
      names = {"--monitoring"},
      description = "Enable HTTP endpoint for monitoring and debugging",
      defaultValue = "false")
  private boolean monitoring;

  @Override
  public void configure(MonitoringContext context) {
    if (monitoring) {
      PlainTextThreadDumpFormatter formatter = new PlainTextThreadDumpFormatter();
      context.addHttpEndpoint(
          "threaddump",
          new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
              ThreadInfo[] threadInfos =
                  ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
              exchange.getResponseHeaders().set("Content-Type", "text/plain");
              byte[] content = formatter.format(threadInfos).getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, content.length);
              try (OutputStream out = exchange.getResponseBody()) {
                out.write(content);
              }
            }
          });
    }
  }

  // from Spring Boot's PlainTextThreadDumpFormatter
  private static class PlainTextThreadDumpFormatter {

    String format(ThreadInfo[] threads) {
      StringWriter dump = new StringWriter();
      PrintWriter writer = new PrintWriter(dump);
      writePreamble(writer);
      for (ThreadInfo info : threads) {
        writeThread(writer, info);
      }
      return dump.toString();
    }

    private void writePreamble(PrintWriter writer) {
      DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      writer.println(dateFormat.format(LocalDateTime.now()));
      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
      writer.printf(
          "Full thread dump %s (%s %s):%n",
          runtime.getVmName(), runtime.getVmVersion(), System.getProperty("java.vm.info"));
      writer.println();
    }

    private void writeThread(PrintWriter writer, ThreadInfo info) {
      writer.printf("\"%s\" - Thread t@%d%n", info.getThreadName(), info.getThreadId());
      writer.printf("   %s: %s%n", Thread.State.class.getCanonicalName(), info.getThreadState());
      writeStackTrace(writer, info, info.getLockedMonitors());
      writer.println();
      writeLockedOwnableSynchronizers(writer, info);
      writer.println();
    }

    private void writeStackTrace(
        PrintWriter writer, ThreadInfo info, MonitorInfo[] lockedMonitors) {
      int depth = 0;
      for (StackTraceElement element : info.getStackTrace()) {
        writeStackTraceElement(
            writer, element, info, lockedMonitorsForDepth(lockedMonitors, depth), depth == 0);
        depth++;
      }
    }

    private List<MonitorInfo> lockedMonitorsForDepth(MonitorInfo[] lockedMonitors, int depth) {
      return Stream.of(lockedMonitors)
          .filter((lockedMonitor) -> lockedMonitor.getLockedStackDepth() == depth)
          .collect(Collectors.toList());
    }

    private void writeStackTraceElement(
        PrintWriter writer,
        StackTraceElement element,
        ThreadInfo info,
        List<MonitorInfo> lockedMonitors,
        boolean firstElement) {
      writer.printf("\tat %s%n", element.toString());
      LockInfo lockInfo = info.getLockInfo();
      if (firstElement && lockInfo != null) {
        if (element.getClassName().equals(Object.class.getName())
            && element.getMethodName().equals("wait")) {
          writer.printf("\t- waiting on %s%n", format(lockInfo));
        } else {
          String lockOwner = info.getLockOwnerName();
          if (lockOwner != null) {
            writer.printf(
                "\t- waiting to lock %s owned by \"%s\" t@%d%n",
                format(lockInfo), lockOwner, info.getLockOwnerId());
          } else {
            writer.printf("\t- parking to wait for %s%n", format(lockInfo));
          }
        }
      }
      writeMonitors(writer, lockedMonitors);
    }

    private String format(LockInfo lockInfo) {
      return String.format("<%x> (a %s)", lockInfo.getIdentityHashCode(), lockInfo.getClassName());
    }

    private void writeMonitors(PrintWriter writer, List<MonitorInfo> lockedMonitorsAtCurrentDepth) {
      for (MonitorInfo lockedMonitor : lockedMonitorsAtCurrentDepth) {
        writer.printf("\t- locked %s%n", format(lockedMonitor));
      }
    }

    private void writeLockedOwnableSynchronizers(PrintWriter writer, ThreadInfo info) {
      writer.println("   Locked ownable synchronizers:");
      LockInfo[] lockedSynchronizers = info.getLockedSynchronizers();
      if (lockedSynchronizers == null || lockedSynchronizers.length == 0) {
        writer.println("\t- None");
      } else {
        for (LockInfo lockedSynchronizer : lockedSynchronizers) {
          writer.printf("\t- Locked %s%n", format(lockedSynchronizer));
        }
      }
    }
  }
}

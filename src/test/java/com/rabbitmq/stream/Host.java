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
package com.rabbitmq.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

public class Host {

  public static final String DOCKER_PREFIX = "DOCKER:";

  private static String capture(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    StringBuilder buff = new StringBuilder();
    while ((line = br.readLine()) != null) {
      buff.append(line).append("\n");
    }
    return buff.toString();
  }

  private static Process executeCommand(String command) throws IOException {
    Process pr = executeCommandProcess(command);

    int ev = waitForExitValue(pr);
    if (ev != 0) {
      String stdout = capture(pr.getInputStream());
      String stderr = capture(pr.getErrorStream());
      throw new IOException(
          "unexpected command exit value: "
              + ev
              + "\ncommand: "
              + command
              + "\n"
              + "\nstdout:\n"
              + stdout
              + "\nstderr:\n"
              + stderr
              + "\n");
    }
    return pr;
  }

  public static String hostname() throws IOException {
    Process process = executeCommand("hostname");
    return capture(process.getInputStream()).trim();
  }

  private static int waitForExitValue(Process pr) {
    while (true) {
      try {
        pr.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
    return pr.exitValue();
  }

  private static Process executeCommandProcess(String command) throws IOException {
    String[] finalCommand;
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      finalCommand = new String[4];
      finalCommand[0] = "C:\\winnt\\system32\\cmd.exe";
      finalCommand[1] = "/y";
      finalCommand[2] = "/c";
      finalCommand[3] = command;
    } else {
      finalCommand = new String[3];
      finalCommand[0] = "/bin/sh";
      finalCommand[1] = "-c";
      finalCommand[2] = command;
    }
    return Runtime.getRuntime().exec(finalCommand);
  }

  public static Process rabbitmqctl(String command) throws IOException {
    return executeCommand(rabbitmqctlCommand() + " " + command);
  }

  public static Process killConnection(String connectionName) throws IOException {
    return rabbitmqctl("eval 'rabbit_stream:kill_connection(\"" + connectionName + "\").'");
  }

  public static Process killStreamLeaderProcess(String stream) throws IOException {
    return rabbitmqctl(
        "eval 'exit(rabbit_stream_manager:lookup_leader(<<\"/\">>, <<\""
            + stream
            + "\">>),kill).'");
  }

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      throw new IllegalStateException("Please define the rabbitmqctl.bin system property");
    }
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  public static AutoCloseable diskAlarm() throws Exception {
    return new CallableAutoCloseable(
        () -> {
          setDiskAlarm();
          return null;
        },
        () -> {
          clearDiskAlarm();
          return null;
        });
  }

  public static AutoCloseable memoryAlarm() throws Exception {
    return new CallableAutoCloseable(
        () -> {
          setMemoryAlarm();
          return null;
        },
        () -> {
          clearMemoryAlarm();
          return null;
        });
  }

  private static void setDiskAlarm() throws IOException {
    setResourceAlarm("disk");
  }

  private static void clearDiskAlarm() throws IOException {
    clearResourceAlarm("disk");
  }

  private static void setMemoryAlarm() throws IOException {
    setResourceAlarm("memory");
  }

  private static void clearMemoryAlarm() throws IOException {
    clearResourceAlarm("memory");
  }

  private static void setResourceAlarm(String source) throws IOException {
    rabbitmqctl("eval 'rabbit_alarm:set_alarm({{resource_limit, " + source + ", node()}, []}).'");
  }

  private static void clearResourceAlarm(String source) throws IOException {
    rabbitmqctl("eval 'rabbit_alarm:clear_alarm({resource_limit, " + source + ", node()}).'");
  }

  public static boolean isOnDocker() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      throw new IllegalStateException("Please define the rabbitmqctl.bin system property");
    }
    return rabbitmqCtl.startsWith(DOCKER_PREFIX);
  }

  private static final class CallableAutoCloseable implements AutoCloseable {

    private final Callable<Void> end;

    private CallableAutoCloseable(Callable<Void> start, Callable<Void> end) throws Exception {
      this.end = end;
      start.call();
    }

    @Override
    public void close() throws Exception {
      this.end.call();
    }
  }
}

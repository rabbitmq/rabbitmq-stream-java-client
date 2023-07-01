// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import static java.lang.String.format;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Host {

  private static final String DOCKER_PREFIX = "DOCKER:";

  private static final Gson GSON = new Gson();

  public static String capture(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    StringBuilder buff = new StringBuilder();
    while ((line = br.readLine()) != null) {
      buff.append(line).append("\n");
    }
    return buff.toString();
  }

  private static Process executeCommand(String command) throws IOException {
    return executeCommand(command, false);
  }

  private static Process executeCommand(String command, boolean ignoreError) throws IOException {
    Process pr = executeCommandProcess(command);
    int ev = waitForExitValue(pr);
    if (ev != 0 && !ignoreError) {
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
      finalCommand[0] = "C:\\Windows\\system32\\cmd.exe";
      finalCommand[1] = "/y";
      finalCommand[2] = "/c";
      finalCommand[3] = command.replaceAll("\"", "\"\"\"").replaceAll("'", "\"");
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

  public static Process rabbitmqctlIgnoreError(String command) throws IOException {
    return executeCommand(rabbitmqctlCommand() + " " + command, true);
  }

  public static Process killConnection(String connectionName) {
    try {
      List<ConnectionInfo> cs = listConnections();
      if (cs.stream().filter(c -> connectionName.equals(c.clientProvidedName())).count() != 1) {
        throw new IllegalArgumentException(
            format(
                "Could not find 1 connection '%s' in stream connections: %s",
                connectionName,
                cs.stream()
                    .map(ConnectionInfo::clientProvidedName)
                    .collect(Collectors.joining(", "))));
      }
      return rabbitmqctl("eval 'rabbit_stream:kill_connection(\"" + connectionName + "\").'");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<ConnectionInfo> listConnections() {
    try {
      Process process =
          rabbitmqctl("list_stream_connections --formatter json conn_name,client_properties");
      return toConnectionInfoList(capture(process.getInputStream()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static List<ConnectionInfo> toConnectionInfoList(String json) {
    return GSON.fromJson(json, new TypeToken<List<ConnectionInfo>>() {}.getType());
  }

  public static Process killStreamLeaderProcess(String stream) throws IOException {
    return rabbitmqctl(
        "eval 'case rabbit_stream_manager:lookup_leader(<<\"/\">>, <<\""
            + stream
            + "\">>) of {ok, Pid} -> exit(Pid, kill); Pid -> exit(Pid, kill) end.'");
  }

  public static void setEnv(String parameter, String value) throws IOException {
    rabbitmqctl(format("eval 'application:set_env(rabbitmq_stream, %s, %s).'", parameter, value));
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

  public static class ConnectionInfo {

    private String conn_name;
    private List<List<String>> client_properties;

    public String name() {
      return this.conn_name;
    }

    public String clientProvidedName() {
      return client_properties.stream()
          .filter(p -> "connection_name".equals(p.get(0)))
          .findFirst()
          .get()
          .get(2);
    }

    @Override
    public String toString() {
      return "ConnectionInfo{"
          + "conn_name='"
          + conn_name
          + '\''
          + ", client_properties="
          + client_properties
          + '}';
    }
  }
}

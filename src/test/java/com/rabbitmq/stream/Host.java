// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Host {

  private static final Logger LOGGER = LoggerFactory.getLogger(Host.class);

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

  private static Process executeCommand(String command) {
    return executeCommand(command, false);
  }

  private static Process executeCommand(String command, boolean ignoreError) {
    try {
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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  static Process rabbitmqStreams(String command) {
    return executeCommand(rabbitmqStreamsCommand() + " " + command);
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
          rabbitmqctl("list_stream_connections -q --formatter table conn_name,client_properties");
      List<ConnectionInfo> connectionInfoList = Collections.emptyList();
      if (process.exitValue() != 0) {
        LOGGER.warn(
            "Error while trying to list stream connections. Standard output: {}, error output: {}",
            capture(process.getInputStream()),
            capture(process.getErrorStream()));
        return connectionInfoList;
      }
      String content = capture(process.getInputStream());
      String[] lines = content.split(System.getProperty("line.separator"));
      if (lines.length > 1) {
        connectionInfoList = new ArrayList<>(lines.length - 1);
        for (int i = 1; i < lines.length; i++) {
          String line = lines[i];
          String[] fields = line.split("\t");
          String connectionName = fields[0];
          Map<String, String> clientProperties = Collections.emptyMap();
          if (fields.length > 1 && fields[1].length() > 1) {
            clientProperties = buildClientProperties(fields);
          }
          connectionInfoList.add(new ConnectionInfo(connectionName, clientProperties));
        }
      }
      return connectionInfoList;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> buildClientProperties(String[] fields) {
    String clientPropertiesString = fields[1];
    clientPropertiesString =
        clientPropertiesString
            .replace("[", "")
            .replace("]", "")
            .replace("},{", "|")
            .replace("{", "")
            .replace("}", "");
    Map<String, String> clientProperties = new LinkedHashMap<>();
    String[] clientPropertyEntries = clientPropertiesString.split("\\|");
    for (String clientPropertyEntry : clientPropertyEntries) {
      String[] clientProperty = clientPropertyEntry.split("\",\"");
      clientProperties.put(
          clientProperty[0].substring(1),
          clientProperty[1].substring(0, clientProperty[1].length() - 1));
    }
    return clientProperties;
  }

  static List<ConnectionInfo> toConnectionInfoList(String json) {
    return GSON.fromJson(json, new TypeToken<List<ConnectionInfo>>() {}.getType());
  }

  public static void restartStream(String stream) {
    rabbitmqStreams(" restart_stream " + stream);
  }

  public static Process killStreamLeaderProcess(String stream) {
    try {
      return rabbitmqctl(
          "eval 'case rabbit_stream_manager:lookup_leader(<<\"/\">>, <<\""
              + stream
              + "\">>) of {ok, Pid} -> exit(Pid, kill); Pid -> exit(Pid, kill) end.'");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void addUser(String username, String password) throws IOException {
    rabbitmqctl(format("add_user %s %s", username, password));
  }

  public static void setPermissions(String username, List<String> permissions) throws IOException {
    setPermissions(username, "/", permissions);
  }

  public static void setPermissions(String username, String vhost, String permission)
      throws IOException {
    setPermissions(username, vhost, asList(permission, permission, permission));
  }

  public static void setPermissions(String username, String vhost, List<String> permissions)
      throws IOException {
    if (permissions.size() != 3) {
      throw new IllegalArgumentException();
    }
    rabbitmqctl(
        format(
            "set_permissions --vhost %s %s '%s' '%s' '%s'",
            vhost, username, permissions.get(0), permissions.get(1), permissions.get(2)));
  }

  public static void changePassword(String username, String newPassword) throws IOException {
    rabbitmqctl(format("change_password %s %s", username, newPassword));
  }

  public static void deleteUser(String username) throws IOException {
    rabbitmqctl(format("delete_user %s", username));
  }

  public static void addVhost(String vhost) throws IOException {
    rabbitmqctl("add_vhost " + vhost);
  }

  public static void deleteVhost(String vhost) throws Exception {
    rabbitmqctl("delete_vhost " + vhost);
  }

  public static void setEnv(String parameter, String value) throws IOException {
    rabbitmqctl(format("eval 'application:set_env(rabbitmq_stream, %s, %s).'", parameter, value));
  }

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      rabbitmqCtl = DOCKER_PREFIX + "rabbitmq";
    }
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  private static String rabbitmqStreamsCommand() {
    String rabbitmqctl = rabbitmqctlCommand();
    int lastIndex = rabbitmqctl.lastIndexOf("rabbitmqctl");
    if (lastIndex == -1) {
      throw new IllegalArgumentException("Not a valid rabbitqmctl command: " + rabbitmqctl);
    }
    return rabbitmqctl.substring(0, lastIndex) + "rabbitmq-streams";
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

    private final String name;
    private final Map<String, String> clientProperties;

    public ConnectionInfo(String name, Map<String, String> clientProperties) {
      this.name = name;
      this.clientProperties = clientProperties;
    }

    public String name() {
      return this.name;
    }

    public String clientProvidedName() {
      return this.clientProperties.get("connection_name");
    }

    @Override
    public String toString() {
      return "ConnectionInfo{"
          + "name='"
          + name
          + '\''
          + ", clientProperties="
          + clientProperties
          + '}';
    }
  }
}

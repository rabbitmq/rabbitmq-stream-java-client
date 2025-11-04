// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Cli {

  private static final String DOCKER_PREFIX = "DOCKER:";

  private static final Gson GSON = new Gson();

  private static final Map<String, String> DOCKER_NODES_TO_CONTAINERS =
      Map.of(
          "rabbit@node0", "rabbitmq0",
          "rabbit@node1", "rabbitmq1",
          "rabbit@node2", "rabbitmq2");

  private static ProcessState executeCommand(String command) {
    return executeCommand(command, false);
  }

  private static ProcessState executeCommand(String command, boolean ignoreError) {
    Process pr = executeCommandProcess(command);
    InputStreamPumpState inputState = new InputStreamPumpState(pr.getInputStream());
    InputStreamPumpState errorState = new InputStreamPumpState(pr.getErrorStream());

    int ev = waitForExitValue(pr, inputState, errorState);
    inputState.pump();
    errorState.pump();
    if (ev != 0 && !ignoreError) {
      throw new RuntimeException(
          "unexpected command exit value: "
              + ev
              + "\ncommand: "
              + command
              + "\n"
              + "\nstdout:\n"
              + inputState.buffer.toString()
              + "\nstderr:\n"
              + errorState.buffer.toString()
              + "\n");
    }
    return new ProcessState(inputState);
  }

  public static String hostname() {
    return executeCommand("hostname").output();
  }

  private static int waitForExitValue(
      Process pr, InputStreamPumpState inputState, InputStreamPumpState errorState) {
    while (true) {
      try {
        inputState.pump();
        errorState.pump();
        pr.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
    return pr.exitValue();
  }

  private static Process executeCommandProcess(String command) {
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
    try {
      return Runtime.getRuntime().exec(finalCommand);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static ProcessState rabbitmqctl(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command);
  }

  static ProcessState rabbitmqStreams(String command) {
    return executeCommand(rabbitmqStreamsCommand() + " " + command);
  }

  public static ProcessState rabbitmqctlIgnoreError(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command, true);
  }

  public static ProcessState killConnection(String connectionName) {
    List<ConnectionInfo> cs = listConnections();
    String pid =
        cs.stream()
            .filter(c -> connectionName.equals(c.clientProvidedName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        format(
                            "Could not find 1 connection '%s' in stream connections: %s",
                            connectionName,
                            cs.stream()
                                .map(ConnectionInfo::clientProvidedName)
                                .collect(Collectors.joining(", ")))))
            .pid;
    return rabbitmqctl(String.format("eval 'exit(rabbit_misc:string_to_pid(\"%s\"), kill).'", pid));
  }

  public static List<ConnectionInfo> listConnections() {
    ProcessState process =
        rabbitmqctl("list_stream_connections -q --formatter table conn_name,pid,client_properties");
    List<ConnectionInfo> connectionInfoList = Collections.emptyList();
    String content = process.output();
    String[] lines = content.split(System.lineSeparator());
    if (lines.length > 1) {
      connectionInfoList = new ArrayList<>(lines.length - 1);
      for (int i = 1; i < lines.length; i++) {
        String line = lines[i];
        String[] fields = line.split("\t");
        String connectionName = fields[0];
        String pid = fields[1];
        Map<String, String> clientProperties = Collections.emptyMap();
        if (fields.length > 2 && fields[2].length() > 1) {
          clientProperties = buildClientProperties(fields);
        }
        connectionInfoList.add(new ConnectionInfo(connectionName, pid, clientProperties));
      }
    }
    return connectionInfoList;
  }

  public static List<ConnectionInfo> listLocatorConnections() {
    return listConnections().stream()
        .filter(c -> c.clientProvidedName() != null && c.clientProvidedName().contains("-locator-"))
        .collect(Collectors.toList());
  }

  public static List<ConnectionInfo> listConsumerConnections() {
    return listConnections().stream()
        .filter(
            c -> c.clientProvidedName() != null && c.clientProvidedName().contains("-consumer-"))
        .collect(Collectors.toList());
  }

  public static List<ConnectionInfo> listProducerConnections() {
    return listConnections().stream()
        .filter(
            c -> c.clientProvidedName() != null && c.clientProvidedName().contains("-producer-"))
        .collect(Collectors.toList());
  }

  private static Map<String, String> buildClientProperties(String[] fields) {
    String clientPropertiesString = fields[2];
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

  public static List<SubscriptionInfo> listGroupConsumers(String stream, String reference) {
    ProcessState process =
        rabbitmqStreams(
            format(
                "list_stream_group_consumers -q --stream %s --reference %s "
                    + "--formatter table subscription_id,state",
                stream, reference));

    List<SubscriptionInfo> itemList = Collections.emptyList();
    String content = process.output();
    String[] lines = content.split(System.lineSeparator());
    if (lines.length > 1) {
      itemList = new ArrayList<>(lines.length - 1);
      for (int i = 1; i < lines.length; i++) {
        String line = lines[i];
        String[] fields = line.split("\t");
        String id = fields[0];
        String state = fields[1].replace("\"", "");
        itemList.add(new SubscriptionInfo(Integer.parseInt(id), state));
      }
    }
    return itemList;
  }

  public static void restartStream(String stream) {
    rabbitmqStreams(" restart_stream " + stream);
  }

  public static String streamStatus(String stream) {
    return rabbitmqStreams(" stream_status --formatter table " + stream).output();
  }

  public static void killStreamLeaderProcess(String stream) {
    rabbitmqctl(
        "eval 'case rabbit_stream_manager:lookup_leader(<<\"/\">>, <<\""
            + stream
            + "\">>) of {ok, Pid} -> exit(Pid, kill); Pid -> exit(Pid, kill) end.'");
  }

  public static void addUser(String username, String password) {
    rabbitmqctl(format("add_user %s %s", username, password));
  }

  public static void setPermissions(String username, List<String> permissions) {
    setPermissions(username, "/", permissions);
  }

  public static void setPermissions(String username, String vhost, String permission) {
    setPermissions(username, vhost, asList(permission, permission, permission));
  }

  public static void setPermissions(String username, String vhost, List<String> permissions) {
    if (permissions.size() != 3) {
      throw new IllegalArgumentException();
    }
    rabbitmqctl(
        format(
            "set_permissions --vhost %s %s '%s' '%s' '%s'",
            vhost, username, permissions.get(0), permissions.get(1), permissions.get(2)));
  }

  public static void clearPermissions(String username) {
    rabbitmqctl(format("clear_permissions --vhost %s %s", "/", username));
  }

  public static void changePassword(String username, String newPassword) {
    rabbitmqctl(format("change_password %s %s", username, newPassword));
  }

  public static void deleteUser(String username) {
    rabbitmqctl(format("delete_user %s", username));
  }

  public static void addVhost(String vhost) {
    rabbitmqctl("add_vhost " + vhost);
  }

  public static void deleteVhost(String vhost) {
    rabbitmqctl("delete_vhost " + vhost);
  }

  public static void setEnv(String parameter, String value) {
    rabbitmqctl(format("eval 'application:set_env(rabbitmq_stream, %s, %s).'", parameter, value));
  }

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = rabbitmqctlBin();
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  private static String rabbitmqctlBin() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      rabbitmqCtl = DOCKER_PREFIX + "rabbitmq";
    }
    return rabbitmqCtl;
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

  private static void clearResourceAlarm(String source) {
    rabbitmqctl("eval 'rabbit_alarm:clear_alarm({resource_limit, " + source + ", node()}).'");
  }

  public static boolean isOnDocker() {
    return rabbitmqctlBin().startsWith(DOCKER_PREFIX);
  }

  public static List<String> nodes() {
    List<String> clusterNodes = new ArrayList<>();
    clusterNodes.add(rabbitmqctl("eval 'node().'").output().trim());
    List<String> nodes =
        Arrays.stream(
                rabbitmqctl("eval 'nodes().'")
                    .output()
                    .replace("[", "")
                    .replace("]", "")
                    .split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    clusterNodes.addAll(nodes);
    return List.copyOf(clusterNodes);
  }

  public static void restartNode(String node) {
    String container = nodeToDockerContainer(node);
    String dockerCommand = "docker exec " + container + " ";
    String rabbitmqUpgradeCommand = dockerCommand + "rabbitmq-upgrade ";
    executeCommand(rabbitmqUpgradeCommand + "await_online_quorum_plus_one -t 300");
    executeCommand(rabbitmqUpgradeCommand + "drain");
    executeCommand("docker stop " + container);
    executeCommand("docker start " + container);
    String otherContainer =
        DOCKER_NODES_TO_CONTAINERS.values().stream()
            .filter(c -> !c.endsWith(container))
            .findAny()
            .get();
    executeCommand(
        "docker exec "
            + otherContainer
            + " rabbitmqctl await_online_nodes "
            + DOCKER_NODES_TO_CONTAINERS.size());
    executeCommand(dockerCommand + "rabbitmqctl status");
  }

  public static void rebalance() {
    rabbitmqQueues("rebalance all");
  }

  static ProcessState rabbitmqQueues(String command) {
    return executeCommand(rabbitmqQueuesCommand() + " " + command);
  }

  private static String rabbitmqQueuesCommand() {
    String rabbitmqctl = rabbitmqctlCommand();
    int lastIndex = rabbitmqctl.lastIndexOf("rabbitmqctl");
    if (lastIndex == -1) {
      throw new IllegalArgumentException("Not a valid rabbitqmctl command: " + rabbitmqctl);
    }
    return rabbitmqctl.substring(0, lastIndex) + "rabbitmq-queues";
  }

  private static String nodeToDockerContainer(String node) {
    String containerId = DOCKER_NODES_TO_CONTAINERS.get(node);
    if (containerId == null) {
      throw new IllegalArgumentException("No container for node " + node);
    }
    return containerId;
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

    private final String name, pid;
    private final Map<String, String> clientProperties;

    public ConnectionInfo(String name, String pid, Map<String, String> clientProperties) {
      this.name = name;
      this.pid = pid;
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

  public static final class SubscriptionInfo {

    private final int id;
    private final String state;

    public SubscriptionInfo(int id, String state) {
      this.id = id;
      this.state = state;
    }

    public int id() {
      return this.id;
    }

    public String state() {
      return this.state;
    }

    @Override
    public String toString() {
      return "SubscriptionInfo{id='" + id + '\'' + ", state='" + state + '\'' + '}';
    }
  }

  public static class ProcessState {

    private final InputStreamPumpState inputState;

    ProcessState(InputStreamPumpState inputState) {
      this.inputState = inputState;
    }

    public String output() {
      return inputState.buffer.toString();
    }
  }

  private static class InputStreamPumpState {

    private final BufferedReader reader;
    private final StringBuilder buffer;

    private InputStreamPumpState(InputStream in) {
      this.reader = new BufferedReader(new InputStreamReader(in));
      this.buffer = new StringBuilder();
    }

    void pump() {
      String line;
      while (true) {
        try {
          if ((line = reader.readLine()) == null) break;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        buffer.append(line).append("\n");
      }
    }
  }
}

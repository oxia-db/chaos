/*
 * Copyright © 2026 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.oxia.chaos.testcase.ephemeral;

import io.oxia.chaos.util.OxiaExceptionUtils;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.PutOption;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An owner child process and the parent-side handle that drives it.
 *
 * <p>{@link #start} launches {@link #main} on the current JVM and classpath. The parent drives it
 * over a line protocol: each request on stdin receives exactly one response on stdout, and closing
 * stdin closes the client gracefully. Killing the process instead stops session heartbeats so Oxia
 * must expire the session on its own. The child logs to stderr.
 *
 * <ul>
 *   <li>{@code PUT <key> <base64-value>} replies {@code OK <session-id> <client-identifier>}
 *   <li>{@code DELETE <key>} replies {@code OK <true|false>}
 *   <li>{@code GET <key>} replies {@code OK NONE} or {@code OK <base64-value> <session-id>
 *       <client-identifier>}, using {@code -} for missing metadata
 *   <li>failures reply {@code ERR <retryable> <base64-message>}
 * </ul>
 */
final class Owner implements AutoCloseable {

  private static final String READY = "READY";
  private static final Set<PutOption> EPHEMERAL = Set.of(PutOption.AsEphemeralRecord);
  private static final List<String> JVM_OPTIONS = List.of("-Xmx128m", "-XX:+UseSerialGC");

  private final String clientIdentifier;
  private final Process process;
  private final PrintStream requests;
  private final BufferedReader responses;

  private Owner(final String clientIdentifier, final Process process) {
    this.clientIdentifier = clientIdentifier;
    this.process = process;
    this.requests = new PrintStream(process.getOutputStream(), true, StandardCharsets.UTF_8);
    this.responses =
        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
  }

  /** Starts an owner on the current JVM and classpath and waits until its client is connected. */
  static Owner start(
      final String serviceAddress,
      final String namespace,
      final String clientIdentifier,
      final Duration sessionTimeout,
      final Duration requestTimeout) {
    final List<String> command =
        new ArrayList<>(
            List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString()));
    command.addAll(JVM_OPTIONS);
    command.addAll(
        List.of(
            "-cp",
            System.getProperty("java.class.path"),
            Owner.class.getName(),
            serviceAddress,
            namespace,
            clientIdentifier,
            Long.toString(sessionTimeout.toMillis()),
            Long.toString(requestTimeout.toMillis())));
    final Process process;
    try {
      process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    } catch (final IOException error) {
      throw new UncheckedIOException("unable to start ephemeral owner " + clientIdentifier, error);
    }
    final Owner owner = new Owner(clientIdentifier, process);
    final String ready = owner.readLine();
    if (!READY.equals(ready)) {
      owner.close();
      throw new IllegalStateException(
          "ephemeral owner " + clientIdentifier + " failed to start: " + ready);
    }
    return owner;
  }

  public String clientIdentifier() {
    return clientIdentifier;
  }

  public long pid() {
    return process.pid();
  }

  /** Writes an ephemeral record and returns the session that owns it. */
  public synchronized OwnedVersion put(final String key, final byte[] value) {
    final String[] reply = request("PUT " + key + " " + Base64.getEncoder().encodeToString(value));
    return new OwnedVersion(sessionId(reply[1]), identifier(reply[2]));
  }

  public synchronized boolean delete(final String key) {
    return Boolean.parseBoolean(request("DELETE " + key)[1]);
  }

  public synchronized Optional<OwnedValue> get(final String key) {
    final String[] reply = request("GET " + key);
    if ("NONE".equals(reply[1])) {
      return Optional.empty();
    }
    return Optional.of(
        new OwnedValue(
            Base64.getDecoder().decode(reply[1]),
            new OwnedVersion(sessionId(reply[2]), identifier(reply[3]))));
  }

  /**
   * Closes stdin so the owner closes its client, which deletes its sessions immediately.
   *
   * @return false when the owner had to be killed because it did not exit within the timeout
   */
  public synchronized boolean closeGracefully(final Duration timeout) throws InterruptedException {
    requests.close();
    if (process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      return true;
    }
    process.destroyForcibly().waitFor();
    return false;
  }

  /** Kills the owner without closing its client, leaving Oxia to expire its sessions. */
  public synchronized void crash() throws InterruptedException {
    process.destroyForcibly().waitFor();
  }

  public boolean isAlive() {
    return process.isAlive();
  }

  @Override
  public void close() {
    process.destroyForcibly();
  }

  private String[] request(final String request) {
    if (!process.isAlive()) {
      throw new IllegalStateException("ephemeral owner " + clientIdentifier + " is not running");
    }
    requests.println(request);
    final String reply = readLine();
    if (reply == null) {
      throw new IllegalStateException(
          "ephemeral owner " + clientIdentifier + " exited during: " + request);
    }
    final String[] parts = reply.split(" ");
    if ("ERR".equals(parts[0])) {
      throw new OwnerOperationException(
          clientIdentifier,
          Boolean.parseBoolean(parts[1]),
          new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8));
    }
    return parts;
  }

  private String readLine() {
    try {
      return responses.readLine();
    } catch (final IOException error) {
      throw new UncheckedIOException(
          "unable to read from ephemeral owner " + clientIdentifier, error);
    }
  }

  private static Optional<Long> sessionId(final String value) {
    return "-".equals(value) ? Optional.empty() : Optional.of(Long.parseLong(value));
  }

  private static Optional<String> identifier(final String value) {
    return "-".equals(value) ? Optional.empty() : Optional.of(value);
  }

  /**
   * Child entry point. Arguments: service address, namespace, client identifier, session and
   * request timeouts in milliseconds.
   */
  public static void main(final String[] args) throws Exception {
    if (args.length != 5) {
      throw new IllegalArgumentException(
          "usage: <service-address> <namespace> <client-identifier> <session-timeout-ms>"
              + " <request-timeout-ms>");
    }
    final PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
    final BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    try (final SyncOxiaClient client =
        OxiaClientBuilder.create(args[0])
            .namespace(args[1])
            .clientIdentifier(args[2])
            .sessionTimeout(Duration.ofMillis(Long.parseLong(args[3])))
            .requestTimeout(Duration.ofMillis(Long.parseLong(args[4])))
            .syncClient()) {
      out.println(READY);
      String line;
      while ((line = in.readLine()) != null) {
        out.println(handle(client, line));
      }
    }
  }

  static String handle(final SyncOxiaClient client, final String line) {
    final String[] parts = line.split(" ");
    try {
      return switch (parts[0]) {
        case "PUT" -> {
          final PutResult result =
              client.put(parts[1], Base64.getDecoder().decode(parts[2]), EPHEMERAL);
          yield "OK " + metadata(result.version());
        }
        case "DELETE" -> "OK " + client.delete(parts[1]);
        case "GET" -> {
          final GetResult result = client.get(parts[1]);
          yield result == null
              ? "OK NONE"
              : "OK "
                  + Base64.getEncoder().encodeToString(result.value())
                  + " "
                  + metadata(result.version());
        }
        default -> throw new IllegalArgumentException("unknown request: " + parts[0]);
      };
    } catch (final Exception error) {
      return "ERR "
          + OxiaExceptionUtils.isRetryable(error)
          + " "
          + Base64.getEncoder()
              .encodeToString(String.valueOf(error).getBytes(StandardCharsets.UTF_8));
    }
  }

  private static String metadata(final Version version) {
    return version.sessionId().map(String::valueOf).orElse("-")
        + " "
        + version.clientIdentifier().orElse("-");
  }

  /** Session metadata reported by Oxia for an ephemeral record. */
  record OwnedVersion(Optional<Long> sessionId, Optional<String> clientIdentifier) {}

  /** A value read by an owner together with its session metadata. */
  record OwnedValue(byte[] value, OwnedVersion version) {}

  /** An operation the owner attempted failed inside the child process. */
  static final class OwnerOperationException extends RuntimeException {

    private final boolean retryable;

    OwnerOperationException(
        final String clientIdentifier, final boolean retryable, final String message) {
      super("ephemeral owner " + clientIdentifier + " failed: " + message);
      this.retryable = retryable;
    }

    public boolean retryable() {
      return retryable;
    }
  }
}

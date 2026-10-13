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
package io.oxia.chaos.cmd;

import io.oxia.chaos.util.OxiaExceptionUtils;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.PutOption;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Set;

/**
 * Internal child process that owns ephemeral records for the ephemeral case.
 *
 * <p>The parent runner drives it over a line protocol: each request on stdin receives exactly one
 * response on stdout, and closing stdin closes the client gracefully. Killing the process instead
 * stops session heartbeats so Oxia must expire the session on its own. Logs go to stderr.
 *
 * <ul>
 *   <li>{@code PUT <key> <base64-value>} replies {@code OK <session-id> <client-identifier>}
 *   <li>{@code DELETE <key>} replies {@code OK <true|false>}
 *   <li>{@code GET <key>} replies {@code OK NONE} or {@code OK <base64-value> <session-id>
 *       <client-identifier>}, using {@code -} for missing metadata
 *   <li>failures reply {@code ERR <retryable> <base64-message>}
 * </ul>
 */
public final class EphemeralOwner {

  public static final String READY = "READY";
  public static final String CLOSED = "CLOSED";
  private static final Set<PutOption> EPHEMERAL = Set.of(PutOption.AsEphemeralRecord);

  private EphemeralOwner() {}

  /** Arguments: service address, namespace, client identifier, session and request timeouts. */
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
    out.println(CLOSED);
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
}

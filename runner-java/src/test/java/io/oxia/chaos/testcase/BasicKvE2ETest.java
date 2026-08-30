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
package io.oxia.chaos.testcase;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.MemoryInferenceStore;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class BasicKvE2ETest {

  private static final int OXIA_PORT = 6648;

  @Container
  private static final GenericContainer<?> OXIA =
      new GenericContainer<>(DockerImageName.parse("oxia/oxia:0.16.8"))
          .withExposedPorts(OXIA_PORT)
          .withCommand(
              "/oxia/bin/oxia",
              "standalone",
              "--public-addr",
              "0.0.0.0:" + OXIA_PORT,
              "--data-dir",
              "/tmp/oxia/data",
              "--wal-dir",
              "/tmp/oxia/wal")
          .waitingFor(Wait.forListeningPort())
          .withStartupTimeout(Duration.ofMinutes(2));

  @Test
  void runsBasicKvAgainstOxiaAndCleansUpItsKeys() throws Exception {
    final String runId = "testcontainers-e2e";
    final String serviceAddress = OXIA.getHost() + ":" + OXIA.getMappedPort(OXIA_PORT);
    final Options options =
        new Options(
            Options.BASIC_KV,
            serviceAddress,
            Duration.ofSeconds(3),
            100,
            500,
            100,
            Duration.ofMillis(500),
            7L);
    final InferenceStore inference = new MemoryInferenceStore();
    final OpenTelemetry openTelemetry = OpenTelemetry.noop();

    try (final RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, Options.BASIC_KV, inference::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new BasicKv(options, runId, client, openTelemetry, inference, metrics, Duration.ofSeconds(30))
          .run();

      final String runPrefix = "/oxia-chaos/runs/" + runId + "/keys/key-";
      assertThat(inference.size()).isZero();
      assertThat(client.list(runPrefix, runPrefix + "zz")).isEmpty();
    }
  }
}

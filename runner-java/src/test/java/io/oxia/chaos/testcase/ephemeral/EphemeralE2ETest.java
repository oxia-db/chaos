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

import static io.oxia.chaos.observability.MetricTestUtils.successes;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.EphemeralModel;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Duration;
import java.util.Collection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class EphemeralE2ETest {

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void runsEphemeralWithCrashedAndClosedOwnersAndLeavesNoRecords() throws Exception {
    final String runId = "testcontainers-ephemeral";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.EPHEMERAL,
            serviceAddress,
            Duration.ofSeconds(12),
            40,
            200,
            100,
            Duration.ofSeconds(2),
            211L);
    final Ephemeral.Settings settings =
        new Ephemeral.Settings(
            Duration.ofSeconds(3),
            Duration.ofSeconds(5),
            Duration.ofSeconds(4),
            Duration.ofSeconds(5));
    final EphemeralModel model = new EphemeralModel();
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();

    try (provider;
        final RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, Options.EPHEMERAL, model::size);
        final SyncOxiaClient observer =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new Ephemeral(
              options,
              runId,
              "default",
              observer,
              openTelemetry,
              model,
              metrics,
              Duration.ofSeconds(30),
              settings)
          .run();

      final Collection<MetricData> collected = reader.collectAllMetrics();
      assertThat(successes(collected, "owner-crash")).isPositive();
      assertThat(successes(collected, "owner-close")).isPositive();
      assertThat(successes(collected, "ephemeral-put")).isPositive();
      assertThat(model.size()).isZero();
      for (final String slot : new String[] {"a", "b"}) {
        final String prefix = "/oxia-chaos/runs/" + runId + "/ephemeral/" + slot + "/key-";
        assertThat(observer.list(prefix, prefix + "~")).isEmpty();
      }
    }
  }
}

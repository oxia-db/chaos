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
package io.oxia.chaos.testcase.versioning;

import static io.oxia.chaos.observability.MetricTestUtils.successes;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.VersionModel;
import io.oxia.chaos.observability.Metrics;
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
class VersioningE2ETest {

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void racesConditionalWritesAndLeavesNoRecords() throws Exception {
    final String runId = "testcontainers-versioning";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.VERSIONING,
            serviceAddress,
            Duration.ofSeconds(8),
            64,
            0,
            50,
            Duration.ofSeconds(2),
            307L);
    final VersionModel model = new VersionModel();
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();

    try (provider;
        final Metrics metrics = new Metrics(openTelemetry, Options.VERSIONING, model::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new Versioning(options, runId, client, openTelemetry, model, metrics, Duration.ofSeconds(30))
          .run();

      final Collection<MetricData> collected = reader.collectAllMetrics();
      for (final String operation :
          new String[] {
            "get", "create", "conditional-put", "conditional-delete", "stale-put", "stale-delete"
          }) {
        assertThat(successes(collected, operation)).as(operation).isPositive();
      }
      final String prefix = "/oxia-chaos/runs/" + runId + "/versioning/key-";
      assertThat(client.list(prefix, prefix + "~")).isEmpty();
    }
  }
}

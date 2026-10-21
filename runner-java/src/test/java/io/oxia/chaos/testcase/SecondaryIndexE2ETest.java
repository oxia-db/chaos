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

import static io.oxia.chaos.observability.MetricTestUtils.successes;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.SecondaryIndexModel;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class SecondaryIndexE2ETest {

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void runsSecondaryIndexAndLeavesNoIndexEntries() throws Exception {
    final String runId = "testcontainers-secondary-index";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.SECONDARY_INDEX,
            serviceAddress,
            Duration.ofSeconds(8),
            80,
            300,
            100,
            Duration.ofSeconds(2),
            307L);
    final SecondaryIndexModel model = new SecondaryIndexModel();
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();

    try (provider;
        final RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, Options.SECONDARY_INDEX, model::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new SecondaryIndex(
              options, runId, client, openTelemetry, model, metrics, Duration.ofSeconds(30))
          .run();

      final Collection<MetricData> collected = reader.collectAllMetrics();
      for (final String operation :
          new String[] {"put", "delete", "index-get", "index-range-scan"}) {
        assertThat(successes(collected, operation)).as(operation).isPositive();
      }
      assertThat(model.size()).isZero();
      final String secondaryPrefix = "oxia-chaos-" + runId + "-";
      for (final String indexName : SecondaryIndex.INDEXES) {
        assertThat(indexScan(client, indexName, secondaryPrefix, secondaryPrefix + "~")).isEmpty();
      }
    }
  }

  /** Pins the index semantics that the case's reference model follows. */
  @Test
  void keepsOneEntryPerSecondaryKeyAndDropsReplacedOnes() throws Exception {
    try (final SyncOxiaClient client =
        OxiaClientBuilder.create(OXIA.getServiceAddress()).namespace("default").syncClient()) {
      final byte[] value = "v".getBytes(StandardCharsets.UTF_8);
      client.put(
          "/semantics/k1",
          value,
          Set.of(PutOption.SecondaryIndex("idx", "s1"), PutOption.SecondaryIndex("idx", "s2")));
      client.put("/semantics/k2", value, Set.of(PutOption.SecondaryIndex("idx", "s1")));

      assertThat(indexScan(client, "idx", "s0", "s9"))
          .containsExactlyInAnyOrder("/semantics/k1", "/semantics/k1", "/semantics/k2");

      client.put("/semantics/k1", value, Set.of(PutOption.SecondaryIndex("idx", "s3")));
      assertThat(indexScan(client, "idx", "s0", "s9"))
          .containsExactlyInAnyOrder("/semantics/k1", "/semantics/k2");
      assertThat(indexScan(client, "idx", "s1", "s3")).containsExactly("/semantics/k2");

      client.deleteRange("/semantics/", "/semantics/~");
      assertThat(indexScan(client, "idx", "s0", "s9")).isEmpty();
    }
  }

  private static List<String> indexScan(
      final SyncOxiaClient client, final String indexName, final String start, final String end) {
    final List<String> keys = new ArrayList<>();
    try (final CloseableIterable<GetResult> results =
        client.rangeScan(start, end, Set.of(RangeScanOption.UseIndex(indexName)))) {
      for (final GetResult result : results) {
        keys.add(result.key());
      }
    }
    return keys;
  }
}

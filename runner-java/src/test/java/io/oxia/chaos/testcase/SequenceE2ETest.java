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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.SequenceModel;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class SequenceE2ETest {

  private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("operation");
  private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void runsSequenceAgainstThreeShardOxiaAndCleansUpItsKeys() throws Exception {
    final String runId = "testcontainers-sequence";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.SEQUENCE,
            serviceAddress,
            Duration.ofSeconds(8),
            40,
            300,
            100,
            Duration.ofSeconds(2),
            211L);
    final SequenceModel model = new SequenceModel();
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();

    try (provider;
        final RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, Options.SEQUENCE, model::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new Sequence(options, runId, client, openTelemetry, model, metrics, Duration.ofSeconds(30))
          .run();

      final Collection<MetricData> collected = reader.collectAllMetrics();
      assertThat(successes(collected, "sequence-put")).isPositive();
      assertThat(successes(collected, "get")).isPositive();
      assertThat(successes(collected, "delete")).isPositive();
      assertThat(model.size()).isZero();
      final String runPrefix = "/oxia-chaos/runs/" + runId + "/sequence/";
      assertThat(client.list(runPrefix, runPrefix + "~")).isEmpty();
    }
  }

  /** Pins the server behavior the case relies on. */
  @Test
  void assignsKeysFromTheLastExistingKeyAndPublishesTheLastKey() throws Exception {
    final String prefix = "/oxia-chaos/runs/testcontainers-sequence-probe/sequence/p-00";
    final LinkedBlockingQueue<String> updates = new LinkedBlockingQueue<>();

    try (final SyncOxiaClient client =
        OxiaClientBuilder.create(OXIA.getServiceAddress()).namespace("default").syncClient()) {
      final String first = put(client, prefix, List.of(2L, 0L));
      final String second = put(client, prefix, List.of(1L, 3L));
      assertThat(first).isEqualTo(SequenceModel.key(prefix, new long[] {2, 0}));
      assertThat(second).isEqualTo(SequenceModel.key(prefix, new long[] {3, 3}));

      try (final Closeable ignored =
          client.getSequenceUpdates(
              prefix, updates::add, Set.of(GetSequenceUpdatesOption.PartitionKey(prefix)))) {
        // A new subscriber gets the current last key first
        assertThat(updates.poll(10, TimeUnit.SECONDS)).isEqualTo(second);

        // Trimming the head keeps the base; deleting the tail would move it back
        assertThat(client.delete(first, Set.of(DeleteOption.PartitionKey(prefix)))).isTrue();
        final String third = put(client, prefix, List.of(1L, 0L));
        assertThat(third).isEqualTo(SequenceModel.key(prefix, new long[] {4, 3}));
        // Updates are latest-value: the subscriber may skip keys, but not go back
        assertThat(updates.poll(10, TimeUnit.SECONDS)).isEqualTo(third);
      }
      client.deleteRange(prefix + "-", prefix + "-~");
    }
  }

  private static String put(
      final SyncOxiaClient client, final String prefix, final List<Long> deltas) throws Exception {
    return client
        .put(
            prefix,
            String.format(Locale.ROOT, "value-%s", deltas).getBytes(StandardCharsets.UTF_8),
            Set.of(PutOption.PartitionKey(prefix), PutOption.SequenceKeysDeltas(deltas)))
        .key();
  }

  private static long successes(final Collection<MetricData> metrics, final String operation) {
    return metrics.stream()
        .filter(metric -> metric.getName().equals("oxia.chaos.operations"))
        .flatMap(metric -> metric.getLongSumData().getPoints().stream())
        .filter(
            point ->
                operation.equals(point.getAttributes().get(OPERATION))
                    && "success".equals(point.getAttributes().get(OUTCOME)))
        .mapToLong(point -> point.getValue())
        .sum();
  }
}

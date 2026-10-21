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
package io.oxia.chaos.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

class RunnerMetricsTest {

  @Test
  void usesOxiaDataServerLatencyBuckets() {
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();
    try (provider;
        final RunnerMetrics metrics = new RunnerMetrics(openTelemetry, "basic-kv", () -> 0)) {
      metrics.recordOperation("put", "success", 0.006);
      metrics.recordCheckpoint("periodic", "success", 0.25);

      final Collection<MetricData> collected = reader.collectAllMetrics();
      assertThat(boundaries(collected, "oxia.chaos.operation.duration"))
          .containsExactlyElementsOf(RunnerMetrics.OXIA_LATENCY_BUCKETS_SECONDS);
      assertThat(boundaries(collected, "oxia.chaos.checkpoint.duration"))
          .containsExactlyElementsOf(RunnerMetrics.OXIA_LATENCY_BUCKETS_SECONDS);
    }
  }

  private static MetricData metric(final Collection<MetricData> metrics, final String metricName) {
    return metrics.stream()
        .filter(metric -> metric.getName().equals(metricName))
        .findFirst()
        .orElseThrow();
  }

  private static List<Double> boundaries(
      final Collection<MetricData> metrics, final String metricName) {
    return metric(metrics, metricName)
        .getHistogramData()
        .getPoints()
        .iterator()
        .next()
        .getBoundaries();
  }
}

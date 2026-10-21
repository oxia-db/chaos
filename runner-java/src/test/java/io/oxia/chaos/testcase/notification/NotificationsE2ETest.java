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
package io.oxia.chaos.testcase.notification;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.MemoryInferenceStore;
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
class NotificationsE2ETest {

  private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("operation");
  private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");
  private static final AttributeKey<String> KIND = AttributeKey.stringKey("checkpoint.kind");

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void matchesTheNotificationsOfEveryWriteOnThreeShardsAndCleansUp() throws Exception {
    final String runId = "testcontainers-notification";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.NOTIFICATION,
            serviceAddress,
            Duration.ofSeconds(6),
            60,
            300,
            20,
            Duration.ofSeconds(1),
            211L);
    final InferenceStore inference = new MemoryInferenceStore();
    final InMemoryMetricReader reader = InMemoryMetricReader.create();
    final SdkMeterProvider provider =
        SdkMeterProvider.builder().registerMetricReader(reader).build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(provider).build();

    try (provider;
        final Metrics metrics = new Metrics(openTelemetry, Options.NOTIFICATION, inference::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new Notifications(
              options, runId, client, openTelemetry, inference, metrics, Duration.ofSeconds(30))
          .run();

      final Collection<MetricData> collected = reader.collectAllMetrics();
      assertThat(successes(collected, OPERATION, "put")).isPositive();
      assertThat(successes(collected, OPERATION, "delete")).isPositive();
      assertThat(successes(collected, OPERATION, "delete-range")).isPositive();
      assertThat(successes(collected, KIND, "periodic")).isPositive();
      assertThat(successes(collected, KIND, "final")).isOne();
      final String prefix = "/oxia-chaos/runs/" + runId + "/notification/key-";
      assertThat(inference.size()).isZero();
      assertThat(client.list(prefix, prefix + "~")).isEmpty();
    }
  }

  private static long successes(
      final Collection<MetricData> metrics, final AttributeKey<String> key, final String value) {
    return metrics.stream()
        .filter(
            metric ->
                metric.getName().equals("oxia.chaos.operations")
                    || metric.getName().equals("oxia.chaos.checkpoints"))
        .flatMap(metric -> metric.getLongSumData().getPoints().stream())
        .filter(
            point ->
                value.equals(point.getAttributes().get(key))
                    && "success".equals(point.getAttributes().get(OUTCOME)))
        .mapToLong(point -> point.getValue())
        .sum();
  }
}

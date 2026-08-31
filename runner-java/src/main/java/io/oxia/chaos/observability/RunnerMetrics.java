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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.List;
import java.util.function.LongSupplier;

/** Shared, low-cardinality metric instruments used by all runner cases. */
public final class RunnerMetrics implements AutoCloseable {

  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.observability";
  // Keep the Oxia data-server latency buckets, converting its milliseconds to seconds.
  static final List<Double> OXIA_LATENCY_BUCKETS_SECONDS =
      List.of(
          0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0,
          5.0, 10.0, 20.0, 50.0);

  private final Attributes caseAttributes;
  private final LongCounter operations;
  private final DoubleHistogram operationDuration;
  private final LongCounter checkpoints;
  private final DoubleHistogram checkpointDuration;
  private final LongCounter safetyViolations;
  private final ObservableLongGauge referenceKeys;

  public RunnerMetrics(
      final OpenTelemetry openTelemetry,
      final String caseName,
      final LongSupplier referenceKeyCount) {
    caseAttributes = Attributes.builder().put("case", caseName).build();
    final var meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE);
    operations =
        meter
            .counterBuilder("oxia.chaos.operations")
            .setDescription("Completed chaos runner operations")
            .setUnit("{operation}")
            .build();
    operationDuration =
        meter
            .histogramBuilder("oxia.chaos.operation.duration")
            .setDescription("Chaos runner operation duration")
            .setUnit("s")
            .setExplicitBucketBoundariesAdvice(OXIA_LATENCY_BUCKETS_SECONDS)
            .build();
    checkpoints =
        meter
            .counterBuilder("oxia.chaos.checkpoints")
            .setDescription("Completed full-state checkpoints")
            .setUnit("{checkpoint}")
            .build();
    checkpointDuration =
        meter
            .histogramBuilder("oxia.chaos.checkpoint.duration")
            .setDescription("Full-state checkpoint duration")
            .setUnit("s")
            .setExplicitBucketBoundariesAdvice(OXIA_LATENCY_BUCKETS_SECONDS)
            .build();
    safetyViolations =
        meter
            .counterBuilder("oxia.chaos.safety.violations")
            .setDescription("Detected correctness violations")
            .setUnit("{violation}")
            .build();
    referenceKeys =
        meter
            .gaugeBuilder("oxia.chaos.reference.keys")
            .ofLongs()
            .setDescription("Keys currently held by the inference store")
            .setUnit("{key}")
            .buildWithCallback(
                (final var measurement) ->
                    measurement.record(referenceKeyCount.getAsLong(), caseAttributes));
  }

  public void recordOperation(
      final String operation, final String outcome, final double durationSeconds) {
    final Attributes attributes =
        Attributes.builder()
            .putAll(caseAttributes)
            .put("operation", operation)
            .put("outcome", outcome)
            .build();
    operations.add(1, attributes);
    operationDuration.record(durationSeconds, attributes);
  }

  public void recordCheckpoint(
      final String kind, final String outcome, final double durationSeconds) {
    final Attributes attributes =
        Attributes.builder()
            .putAll(caseAttributes)
            .put("checkpoint.kind", kind)
            .put("outcome", outcome)
            .build();
    checkpoints.add(1, attributes);
    checkpointDuration.record(durationSeconds, attributes);
  }

  public void recordSafetyViolation() {
    safetyViolations.add(1, caseAttributes);
  }

  @Override
  public void close() {
    referenceKeys.close();
  }
}

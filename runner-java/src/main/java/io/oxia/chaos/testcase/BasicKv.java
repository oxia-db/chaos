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

import static io.oxia.chaos.util.Timing.addSaturated;
import static io.oxia.chaos.util.Timing.elapsedMillis;
import static io.oxia.chaos.util.Timing.elapsedSeconds;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.KeyValue;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.GuardUtils;
import io.oxia.chaos.util.KeyGenerator;
import io.oxia.chaos.util.RangeUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.grpc.OxiaStatusException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The basic key/value correctness case, including its fixed operation distribution. */
public final class BasicKv {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicKv.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.basic-kv";
  private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
  private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(5);
  private static final double PUT_THRESHOLD = 0.20;
  private static final double GET_THRESHOLD = PUT_THRESHOLD + 0.15;
  private static final double FLOOR_THRESHOLD = GET_THRESHOLD + 0.05;
  private static final double CEILING_THRESHOLD = FLOOR_THRESHOLD + 0.05;
  private static final double LOWER_THRESHOLD = CEILING_THRESHOLD + 0.05;
  private static final double HIGHER_THRESHOLD = LOWER_THRESHOLD + 0.05;
  private static final double DELETE_THRESHOLD = HIGHER_THRESHOLD + 0.08;
  private static final double DELETE_RANGE_THRESHOLD = DELETE_THRESHOLD + 0.0025;
  private static final double RANGE_SCAN_THRESHOLD = DELETE_RANGE_THRESHOLD + 0.03;
  static final double TOTAL_WEIGHT = RANGE_SCAN_THRESHOLD + 0.02;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final InferenceStore inference;
  private final RunnerMetrics metrics;
  private final Tracer tracer;
  private final RetryPolicy<Object> retryPolicy;
  private final KeyGenerator keyGenerator;
  private final ValueGenerator valueGenerator;

  private long operationCount;
  private long checkpointCount;

  public BasicKv(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final InferenceStore inference,
      final RunnerMetrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.inference = inference;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.retryPolicy =
        RetryPolicy.builder()
            .handleIf(error -> OxiaStatusException.from(error).isRetryable())
            .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
            .withMaxAttempts(-1)
            .withMaxDuration(retryTimeout)
            .onRetry(
                event -> {
                  final OxiaStatusException error =
                      OxiaStatusException.from(event.getLastException());
                  LOGGER
                      .atWarn()
                      .addKeyValue("attempt", event.getAttemptCount())
                      .addKeyValue("status", error.getStatusCode())
                      .log("Retrying current Oxia operation");
                })
            .onRetriesExceeded(
                event -> {
                  final OxiaStatusException error = OxiaStatusException.from(event.getException());
                  LOGGER
                      .atError()
                      .addKeyValue("attempts", event.getAttemptCount())
                      .addKeyValue("status", error.getStatusCode())
                      .log("Current Oxia operation exhausted its retry window");
                })
            .build();
    this.keyGenerator = new KeyGenerator(runId);
    this.valueGenerator = new ValueGenerator(options.seed());
  }

  /** Runs warmup, the timed workload, a final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    Span runSpan =
        tracer
            .spanBuilder("basic-kv.run")
            .setAttribute("test.case", Options.BASIC_KV)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    Throwable failure = null;
    try (Scope ignored = runSpan.makeCurrent()) {
      warmup();
      executeWorkload();
      checkpoint(Checkpoint.FINAL);
      runSpan.setStatus(StatusCode.OK);
    } catch (CorrectnessViolationException error) {
      failure = error;
      metrics.recordSafetyViolation();
      runSpan.recordException(error).setStatus(StatusCode.ERROR, error.getMessage());
      throw error;
    } catch (InterruptedException error) {
      failure = error;
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "interrupted");
      throw error;
    } catch (RuntimeException error) {
      failure = error;
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "execution failure");
      throw error;
    } finally {
      try {
        cleanup();
      } catch (RuntimeException cleanupError) {
        if (failure != null) {
          failure.addSuppressed(cleanupError);
        } else {
          throw cleanupError;
        }
      } finally {
        runSpan.end();
      }
    }

    LOGGER
        .atInfo()
        .addKeyValue("run_id", runId)
        .addKeyValue("operations", operationCount)
        .addKeyValue("checkpoints", checkpointCount)
        .log("basic-kv completed");
  }

  private void warmup() {
    Span span = tracer.spanBuilder("basic-kv.warmup").startSpan();
    long started = System.nanoTime();
    try (Scope ignored = span.makeCurrent()) {
      Failsafe.with(retryPolicy)
          .run(
              () ->
                  GuardUtils.putReferenceGuard(
                      client, inference, keyGenerator.lowerGuardKey(), "lower"));
      for (int index = 0; index < options.keyCount(); index++) {
        final int keyIndex = index;
        final String key = keyGenerator.key(keyIndex);
        final byte[] value = valueGenerator.warmup(keyIndex);
        Failsafe.with(retryPolicy)
            .run(
                () -> {
                  client.put(key, value);
                  inference.put(key, value);
                });
      }
      Failsafe.with(retryPolicy)
          .run(
              () ->
                  GuardUtils.putReferenceGuard(
                      client, inference, keyGenerator.upperGuardKey(), "upper"));
      span.setAttribute("test.keys", options.keyCount()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("keys", options.keyCount())
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("basic-kv warmup completed");
    } catch (RuntimeException error) {
      span.recordException(error).setStatus(StatusCode.ERROR);
      throw error;
    } finally {
      span.end();
    }
  }

  private void executeWorkload() throws InterruptedException {
    long started = System.nanoTime();
    long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    RatePacer pacer = new RatePacer(options.rate());
    SplittableRandom random = new SplittableRandom(options.seed());

    while (System.nanoTime() < deadline) {
      int generated = 0;
      while (generated < options.batchSize() && System.nanoTime() < deadline) {
        pacer.beforeOperation();
        if (System.nanoTime() >= deadline) {
          break;
        }
        executeNext(random);
        operationCount++;
        generated++;
      }

      long now = System.nanoTime();
      if (now >= nextCheckpoint && now < deadline) {
        checkpoint(Checkpoint.PERIODIC);
        nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
      }
    }
  }

  private void executeNext(SplittableRandom random) {
    Operation operation = selectOperation(random.nextDouble(TOTAL_WEIGHT));
    int index = random.nextInt(options.keyCount());
    String key = keyGenerator.key(index);

    switch (operation) {
      case PUT ->
          observe(
              operation,
              key,
              () -> {
                byte[] value = valueGenerator.next(index, operationCount);
                client.put(key, value);
                inference.put(key, value);
              });
      case GET ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonEqual));
      case FLOOR ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonFloor));
      case CEILING ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonCeiling));
      case LOWER ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonLower));
      case HIGHER ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonHigher));
      case DELETE -> observe(operation, key, () -> executeDelete(operation, key));
      case DELETE_RANGE, RANGE_SCAN, LIST -> {
        String endKey = keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
        switch (operation) {
          case DELETE_RANGE ->
              observe(
                  operation,
                  key,
                  () -> {
                    client.deleteRange(key, endKey);
                    inference.deleteRange(key, endKey);
                  });
          case RANGE_SCAN -> observe(operation, key, () -> verifyRangeScan(operation, key, endKey));
          case LIST -> observe(operation, key, () -> verifyList(operation, key, endKey));
          default -> throw new IllegalStateException("unreachable range operation");
        }
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void observe(Operation operation, String key, Runnable action) {
    long started = System.nanoTime();
    String outcome = "success";
    String operationLabel = operation.label();
    Span span =
        tracer
            .spanBuilder("basic-kv." + operationLabel)
            .setAttribute("db.operation.name", operationLabel)
            .setAttribute("db.key", key)
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      Failsafe.with(retryPolicy).run(action::run);
      span.setStatus(StatusCode.OK);
    } catch (RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordOperation(operationLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  private void verifyGet(Operation operation, String key, GetOption option) {
    Optional<KeyValue> expected = expectedGet(operation, key);
    GetResult result = client.get(key, Set.of(option));
    Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, expected.toString(), actual.toString());
    }
  }

  private Optional<KeyValue> expectedGet(Operation operation, String key) {
    return switch (operation) {
      case GET -> inference.get(key);
      case FLOOR -> inference.floor(key);
      case CEILING -> inference.ceiling(key);
      case LOWER -> inference.lower(key);
      case HIGHER -> inference.higher(key);
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    };
  }

  private void executeDelete(Operation operation, String key) {
    boolean expected = inference.get(key).isPresent();
    boolean actual = client.delete(key);
    if (expected != actual) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, Boolean.toString(expected), Boolean.toString(actual));
    }
    inference.delete(key);
  }

  private void verifyRangeScan(Operation operation, String key, String endKey) {
    List<KeyValue> expected = inference.range(key, endKey);
    List<KeyValue> actual = new ArrayList<>();
    try (CloseableIterable<GetResult> results = client.rangeScan(key, endKey)) {
      for (GetResult result : results) {
        actual.add(new KeyValue(result.key(), result.value()));
      }
    }
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
    }
  }

  private void verifyList(Operation operation, String key, String endKey) {
    List<String> expected = inference.list(key, endKey);
    List<String> actual = client.list(key, endKey);
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
    }
  }

  private void checkpoint(Checkpoint checkpoint) {
    long started = System.nanoTime();
    String outcome = "success";
    String checkpointLabel = checkpoint.label();
    Span span =
        tracer
            .spanBuilder("basic-kv.checkpoint")
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      String firstKey = keyGenerator.lowerGuardKey();
      String afterLastKey = keyGenerator.afterUpperGuardKey();
      List<KeyValue> expected = inference.range(firstKey, afterLastKey);
      final List<KeyValue> actual = new ArrayList<>();
      Failsafe.with(retryPolicy)
          .run(
              () -> {
                actual.clear();
                try (CloseableIterable<GetResult> results =
                    client.rangeScan(firstKey, afterLastKey)) {
                  for (final GetResult result : results) {
                    actual.add(new KeyValue(result.key(), result.value()));
                  }
                }
              });
      if (!expected.equals(actual)) {
        throw CorrectnessViolationException.checkpointMismatch(
            SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
      }
      checkpointCount++;
      span.setAttribute("checkpoint.keys", expected.size()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("kind", checkpointLabel)
          .addKeyValue("keys", expected.size())
          .addKeyValue("operations", operationCount)
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("basic-kv checkpoint passed");
    } catch (RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(checkpointLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  private void cleanup() {
    Failsafe.with(retryPolicy)
        .run(
            () ->
                client.deleteRange(
                    keyGenerator.lowerGuardKey(), keyGenerator.afterUpperGuardKey()));
    inference.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("basic-kv cleanup completed");
  }

  static Operation selectOperation(double selected) {
    if (selected < PUT_THRESHOLD) {
      return Operation.PUT;
    }
    if (selected < GET_THRESHOLD) {
      return Operation.GET;
    }
    if (selected < FLOOR_THRESHOLD) {
      return Operation.FLOOR;
    }
    if (selected < CEILING_THRESHOLD) {
      return Operation.CEILING;
    }
    if (selected < LOWER_THRESHOLD) {
      return Operation.LOWER;
    }
    if (selected < HIGHER_THRESHOLD) {
      return Operation.HIGHER;
    }
    if (selected < DELETE_THRESHOLD) {
      return Operation.DELETE;
    }
    if (selected < DELETE_RANGE_THRESHOLD) {
      return Operation.DELETE_RANGE;
    }
    if (selected < RANGE_SCAN_THRESHOLD) {
      return Operation.RANGE_SCAN;
    }
    return Operation.LIST;
  }
}

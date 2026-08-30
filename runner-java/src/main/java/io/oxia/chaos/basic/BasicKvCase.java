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
package io.oxia.chaos.basic;

import static io.oxia.chaos.util.Timing.addSaturated;
import static io.oxia.chaos.util.Timing.elapsedMillis;
import static io.oxia.chaos.util.Timing.elapsedSeconds;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.runner.RunnerConfig;
import io.oxia.chaos.runner.RunnerMetrics;
import io.oxia.chaos.state.MemoryStateStore;
import io.oxia.chaos.state.MemoryStateStore.KeyValue;
import io.oxia.chaos.util.RatePacer;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.GetOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The basic key/value correctness case, including its fixed operation distribution. */
public final class BasicKvCase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicKvCase.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.basic-kv";
  private static final double PUT_LIMIT = 0.20;
  private static final double GET_LIMIT = PUT_LIMIT + 0.15;
  private static final double FLOOR_LIMIT = GET_LIMIT + 0.05;
  private static final double CEILING_LIMIT = FLOOR_LIMIT + 0.05;
  private static final double LOWER_LIMIT = CEILING_LIMIT + 0.05;
  private static final double HIGHER_LIMIT = LOWER_LIMIT + 0.05;
  private static final double DELETE_LIMIT = HIGHER_LIMIT + 0.08;
  private static final double DELETE_RANGE_LIMIT = DELETE_LIMIT + 0.0025;
  private static final double RANGE_SCAN_LIMIT = DELETE_RANGE_LIMIT + 0.03;
  static final double TOTAL_WEIGHT = RANGE_SCAN_LIMIT + 0.02;

  private final RunnerConfig config;
  private final String runId;
  private final SyncOxiaClient client;
  private final MemoryStateStore state;
  private final RunnerMetrics metrics;
  private final Tracer tracer;
  private final String keyPrefix;
  private final String firstKey;
  private final String afterLastKey;

  private long operationCount;
  private long checkpointCount;

  public BasicKvCase(
      RunnerConfig config,
      String runId,
      SyncOxiaClient client,
      OpenTelemetry openTelemetry,
      MemoryStateStore state,
      RunnerMetrics metrics) {
    this.config = config;
    this.runId = runId;
    this.client = client;
    this.state = state;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.keyPrefix = "/oxia-chaos/runs/" + runId + "/keys/key-";
    this.firstKey = lowerGuardKey();
    this.afterLastKey = afterUpperGuardKey();
  }

  /** Runs warmup, the timed workload, a final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    Span runSpan =
        tracer
            .spanBuilder("basic-kv.run")
            .setAttribute("test.case", RunnerConfig.BASIC_KV)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", config.seed())
            .setAttribute("test.key_count", config.keyCount())
            .startSpan();

    Throwable failure = null;
    try (Scope ignored = runSpan.makeCurrent()) {
      warmup();
      executeWorkload();
      checkpoint("final");
      runSpan.setStatus(StatusCode.OK);
    } catch (CorrectnessViolation error) {
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
      putReferenceGuard(lowerGuardKey(), "lower");
      for (int index = 0; index < config.keyCount(); index++) {
        String key = key(index);
        byte[] value = warmupValue(index);
        client.put(key, value);
        state.put(key, value);
      }
      putReferenceGuard(upperGuardKey(), "upper");
      span.setAttribute("test.keys", config.keyCount()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("keys", config.keyCount())
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("basic-kv warmup completed");
    } catch (RuntimeException error) {
      span.recordException(error).setStatus(StatusCode.ERROR);
      throw error;
    } finally {
      span.end();
    }
  }

  private void putReferenceGuard(String key, String name) {
    byte[] value = ("guard-" + name).getBytes(StandardCharsets.UTF_8);
    client.put(key, value);
    state.put(key, value);
  }

  private void executeWorkload() throws InterruptedException {
    long started = System.nanoTime();
    long deadline = addSaturated(started, config.duration().toNanos());
    long nextCheckpoint = addSaturated(started, config.checkpointInterval().toNanos());
    RatePacer pacer = new RatePacer(config.rate());
    SplittableRandom random = new SplittableRandom(config.seed());

    while (System.nanoTime() < deadline) {
      int generated = 0;
      while (generated < config.batchSize() && System.nanoTime() < deadline) {
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
        checkpoint("periodic");
        nextCheckpoint = addSaturated(System.nanoTime(), config.checkpointInterval().toNanos());
      }
    }
  }

  private void executeNext(SplittableRandom random) {
    String operation = selectOperation(random.nextDouble(TOTAL_WEIGHT));
    int index = random.nextInt(config.keyCount());
    String key = key(index);

    switch (operation) {
      case "put" ->
          observe(
              operation,
              key,
              () -> {
                byte[] value = nextValue(index);
                client.put(key, value);
                state.put(key, value);
              });
      case "get" ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonEqual));
      case "floor" ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonFloor));
      case "ceiling" ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonCeiling));
      case "lower" ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonLower));
      case "higher" ->
          observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonHigher));
      case "delete" -> observe(operation, key, () -> executeDelete(operation, key));
      case "delete-range", "range-scan", "list" -> {
        String endKey = key(nextRangeEnd(random, index));
        switch (operation) {
          case "delete-range" ->
              observe(
                  operation,
                  key,
                  () -> {
                    client.deleteRange(key, endKey);
                    state.deleteRange(key, endKey);
                  });
          case "range-scan" ->
              observe(operation, key, () -> verifyRangeScan(operation, key, endKey));
          case "list" -> observe(operation, key, () -> verifyList(operation, key, endKey));
          default -> throw new IllegalStateException("unreachable range operation");
        }
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void observe(String operation, String key, Runnable action) {
    long started = System.nanoTime();
    String outcome = "success";
    Span span =
        tracer
            .spanBuilder("basic-kv." + operation)
            .setAttribute("db.operation.name", operation)
            .setAttribute("db.key", key)
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      action.run();
      span.setStatus(StatusCode.OK);
    } catch (RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordOperation(operation, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  private void verifyGet(String operation, String key, GetOption option) {
    Optional<KeyValue> expected = expectedGet(operation, key);
    GetResult result = client.get(key, Set.of(option));
    Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw violation(operation, key, expected.toString(), actual.toString());
    }
  }

  private Optional<KeyValue> expectedGet(String operation, String key) {
    return switch (operation) {
      case "get" -> state.get(key);
      case "floor" -> state.floor(key);
      case "ceiling" -> state.ceiling(key);
      case "lower" -> state.lower(key);
      case "higher" -> state.higher(key);
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    };
  }

  private void executeDelete(String operation, String key) {
    boolean expected = state.get(key).isPresent();
    boolean actual = client.delete(key);
    if (expected != actual) {
      throw violation(operation, key, Boolean.toString(expected), Boolean.toString(actual));
    }
    state.delete(key);
  }

  private void verifyRangeScan(String operation, String key, String endKey) {
    List<KeyValue> expected = state.range(key, endKey);
    List<KeyValue> actual = new ArrayList<>();
    try (CloseableIterable<GetResult> results = client.rangeScan(key, endKey)) {
      for (GetResult result : results) {
        actual.add(new KeyValue(result.key(), result.value()));
      }
    }
    if (!expected.equals(actual)) {
      throw violation(operation, key, summarize(expected), summarize(actual));
    }
  }

  private void verifyList(String operation, String key, String endKey) {
    List<String> expected = state.list(key, endKey);
    List<String> actual = client.list(key, endKey);
    if (!expected.equals(actual)) {
      throw violation(operation, key, summarize(expected), summarize(actual));
    }
  }

  private void checkpoint(String kind) {
    long started = System.nanoTime();
    String outcome = "success";
    Span span =
        tracer.spanBuilder("basic-kv.checkpoint").setAttribute("checkpoint.kind", kind).startSpan();
    try (Scope ignored = span.makeCurrent()) {
      List<KeyValue> expected = state.range(firstKey, afterLastKey);
      List<KeyValue> actual = new ArrayList<>();
      try (CloseableIterable<GetResult> results = client.rangeScan(firstKey, afterLastKey)) {
        for (GetResult result : results) {
          actual.add(new KeyValue(result.key(), result.value()));
        }
      }
      if (!expected.equals(actual)) {
        throw new CorrectnessViolation(
            "checkpoint mismatch: expected="
                + summarize(expected)
                + ", actual="
                + summarize(actual));
      }
      checkpointCount++;
      span.setAttribute("checkpoint.keys", expected.size()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("kind", kind)
          .addKeyValue("keys", expected.size())
          .addKeyValue("operations", operationCount)
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("basic-kv checkpoint passed");
    } catch (RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(kind, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  private void cleanup() {
    client.deleteRange(firstKey, afterLastKey);
    state.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("basic-kv cleanup completed");
  }

  private CorrectnessViolation violation(
      String operation, String key, String expected, String actual) {
    return new CorrectnessViolation(
        "operation mismatch: operation="
            + operation
            + ", key="
            + key
            + ", expected="
            + expected
            + ", actual="
            + actual);
  }

  static String selectOperation(double selected) {
    if (selected < PUT_LIMIT) {
      return "put";
    }
    if (selected < GET_LIMIT) {
      return "get";
    }
    if (selected < FLOOR_LIMIT) {
      return "floor";
    }
    if (selected < CEILING_LIMIT) {
      return "ceiling";
    }
    if (selected < LOWER_LIMIT) {
      return "lower";
    }
    if (selected < HIGHER_LIMIT) {
      return "higher";
    }
    if (selected < DELETE_LIMIT) {
      return "delete";
    }
    if (selected < DELETE_RANGE_LIMIT) {
      return "delete-range";
    }
    if (selected < RANGE_SCAN_LIMIT) {
      return "range-scan";
    }
    return "list";
  }

  private int nextRangeEnd(SplittableRandom random, int start) {
    int maximumLength = maximumRangeLength(config.keyCount());
    int length = 1 + random.nextInt(maximumLength);
    return Math.min(config.keyCount(), start + length);
  }

  static int maximumRangeLength(int keyCount) {
    return Math.max(1, keyCount / 100);
  }

  private String key(int index) {
    return keyPrefix + String.format(Locale.ROOT, "%08d", index);
  }

  private String lowerGuardKey() {
    return keyPrefix + "-guard";
  }

  private String upperGuardKey() {
    return keyPrefix + "z-guard";
  }

  private String afterUpperGuardKey() {
    return keyPrefix + "zz";
  }

  private byte[] warmupValue(int index) {
    return String.format(Locale.ROOT, "warmup-%016x-%08d", config.seed(), index)
        .getBytes(StandardCharsets.UTF_8);
  }

  private byte[] nextValue(int index) {
    return String.format(
            Locale.ROOT, "value-%016x-%08d-%016x", config.seed(), index, operationCount)
        .getBytes(StandardCharsets.UTF_8);
  }

  private static String summarize(List<?> values) {
    int displayed = Math.min(values.size(), 5);
    return "size=" + values.size() + ", first=" + values.subList(0, displayed);
  }

  /** Signals a safety failure distinct from an infrastructure or availability failure. */
  public static final class CorrectnessViolation extends RuntimeException {
    public CorrectnessViolation(String message) {
      super(message);
    }
  }
}

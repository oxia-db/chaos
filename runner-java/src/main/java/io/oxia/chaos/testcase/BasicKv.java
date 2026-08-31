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
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.KeyValue;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.GuardUtils;
import io.oxia.chaos.util.KeyGenerator;
import io.oxia.chaos.util.OxiaExceptionUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
            .handleIf(OxiaExceptionUtils::isRetryable)
            .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
            .withMaxAttempts(-1)
            .withMaxDuration(retryTimeout)
            .onRetry(
                (final var event) -> {
                  final OxiaStatusException error =
                      OxiaExceptionUtils.status(event.getLastException());
                  LOGGER
                      .atWarn()
                      .addKeyValue("attempt", event.getAttemptCount())
                      .addKeyValue("status", error.getStatusCode())
                      .log("Retrying current Oxia operation");
                })
            .onRetriesExceeded(
                (final var event) -> {
                  final OxiaStatusException error = OxiaExceptionUtils.status(event.getException());
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
    final Span runSpan =
        tracer
            .spanBuilder("basic-kv.run")
            .setAttribute("test.case", Options.BASIC_KV)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    Throwable failure = null;
    try (final Scope ignored = runSpan.makeCurrent()) {
      warmup();
      executeWorkload();
      checkpoint(Checkpoint.FINAL);
      runSpan.setStatus(StatusCode.OK);
    } catch (final CorrectnessViolationException error) {
      failure = error;
      metrics.recordSafetyViolation();
      runSpan.recordException(error).setStatus(StatusCode.ERROR, error.getMessage());
      throw error;
    } catch (final InterruptedException error) {
      failure = error;
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "interrupted");
      throw error;
    } catch (final RuntimeException error) {
      failure = error;
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "execution failure");
      throw error;
    } finally {
      try {
        cleanup();
      } catch (final RuntimeException cleanupError) {
        if (failure != null) {
          ExceptionUtils.addSuppressedIfDistinct(failure, cleanupError);
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
    final Span span = tracer.spanBuilder("basic-kv.warmup").setNoParent().startSpan();
    final long started = System.nanoTime();
    try (final Scope ignored = Context.root().makeCurrent()) {
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
    } catch (final RuntimeException error) {
      span.recordException(error).setStatus(StatusCode.ERROR);
      throw error;
    } finally {
      span.end();
    }
  }

  private void executeWorkload() throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());

    final Thread.Builder.OfVirtual threads = Thread.ofVirtual().name("oxia-chaos-operation-", 0);
    try (final ExecutorService executor =
        Context.taskWrapping(Executors.newThreadPerTaskExecutor(threads.factory()))) {
      while (System.nanoTime() < deadline) {
        final BatchType batchType = selectBatch(random.nextDouble(TOTAL_WEIGHT));
        final int maximumBatchSize =
            batchType == BatchType.WRITE
                ? Math.min(options.batchSize(), options.keyCount())
                : options.batchSize();
        final List<Callable<Void>> operations = new ArrayList<>(maximumBatchSize);
        final List<Runnable> inferenceUpdates = new ArrayList<>(maximumBatchSize);
        final Set<Integer> writeKeys = new HashSet<>(maximumBatchSize);
        int generated = 0;
        while (generated < maximumBatchSize && System.nanoTime() < deadline) {
          pacer.beforeOperation();
          if (System.nanoTime() >= deadline) {
            break;
          }
          final long sequence = operationCount + generated;
          addNextOperation(batchType, random, sequence, writeKeys, operations, inferenceUpdates);
          generated++;
        }

        executeConcurrentBatch(executor, operations);
        for (final Runnable inferenceUpdate : inferenceUpdates) {
          inferenceUpdate.run();
        }
        operationCount += generated;

        final long now = System.nanoTime();
        if (now >= nextCheckpoint && now < deadline) {
          checkpoint(Checkpoint.PERIODIC);
          nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
        }
      }
    }
  }

  private void addNextOperation(
      final BatchType batchType,
      final SplittableRandom random,
      final long sequence,
      final Set<Integer> writeKeys,
      final List<Callable<Void>> operations,
      final List<Runnable> inferenceUpdates) {
    switch (batchType) {
      case WRITE -> addWriteOperation(random, sequence, writeKeys, operations, inferenceUpdates);
      case READ -> addReadOperation(random, operations);
      case DELETE_RANGE -> addDeleteRangeOperation(random, operations, inferenceUpdates);
      default -> throw new IllegalStateException("unreachable batch type: " + batchType);
    }
  }

  private void addWriteOperation(
      final SplittableRandom random,
      final long sequence,
      final Set<Integer> writeKeys,
      final List<Callable<Void>> operations,
      final List<Runnable> inferenceUpdates) {
    final Operation operation = selectOperationForBatch(BatchType.WRITE, random);
    final int index = nextUniqueWriteIndex(random, writeKeys);
    final String key = keyGenerator.key(index);

    switch (operation) {
      case PUT -> {
        final byte[] value = valueGenerator.next(index, sequence);
        operations.add(
            () -> {
              observe(operation, key, () -> client.put(key, value));
              return null;
            });
        inferenceUpdates.add(() -> inference.put(key, value));
      }
      case DELETE -> {
        final boolean expected = inference.get(key).isPresent();
        operations.add(
            () -> {
              observe(operation, key, () -> verifyDelete(operation, key, expected));
              return null;
            });
        inferenceUpdates.add(() -> inference.delete(key));
      }
      default -> throw new IllegalStateException("unreachable write operation: " + operation);
    }
  }

  private void addReadOperation(
      final SplittableRandom random, final List<Callable<Void>> operations) {
    final Operation operation = selectOperationForBatch(BatchType.READ, random);
    final int index = random.nextInt(options.keyCount());
    final String key = keyGenerator.key(index);

    switch (operation) {
      case GET ->
          operations.add(
              () -> {
                observe(operation, key, () -> verifyGet(operation, key, GetOption.ComparisonEqual));
                return null;
              });
      case FLOOR, CEILING, LOWER, HIGHER ->
          operations.add(
              () -> {
                observe(operation, key, () -> verifyGet(operation, key, comparisonFor(operation)));
                return null;
              });
      case RANGE_SCAN, LIST -> {
        final String endKey =
            keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
        switch (operation) {
          case RANGE_SCAN ->
              operations.add(
                  () -> {
                    observe(operation, key, () -> verifyRangeScan(operation, key, endKey));
                    return null;
                  });
          case LIST ->
              operations.add(
                  () -> {
                    observe(operation, key, () -> verifyList(operation, key, endKey));
                    return null;
                  });
          default -> throw new IllegalStateException("unreachable range operation");
        }
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void addDeleteRangeOperation(
      final SplittableRandom random,
      final List<Callable<Void>> operations,
      final List<Runnable> inferenceUpdates) {
    final int index = random.nextInt(options.keyCount());
    final String key = keyGenerator.key(index);
    final String endKey = keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
    operations.add(
        () -> {
          observe(Operation.DELETE_RANGE, key, () -> client.deleteRange(key, endKey));
          return null;
        });
    inferenceUpdates.add(() -> inference.deleteRange(key, endKey));
  }

  private int nextUniqueWriteIndex(final SplittableRandom random, final Set<Integer> writeKeys) {
    int index;
    do {
      index = random.nextInt(options.keyCount());
    } while (!writeKeys.add(index));
    return index;
  }

  private static GetOption comparisonFor(final Operation operation) {
    return switch (operation) {
      case FLOOR -> GetOption.ComparisonFloor;
      case CEILING -> GetOption.ComparisonCeiling;
      case LOWER -> GetOption.ComparisonLower;
      case HIGHER -> GetOption.ComparisonHigher;
      default -> throw new IllegalArgumentException("not an ordered get operation: " + operation);
    };
  }

  static void executeConcurrentBatch(
      final ExecutorService executor, final List<Callable<Void>> operations)
      throws InterruptedException {
    final List<Future<Void>> batch = executor.invokeAll(operations);
    Throwable failure = null;
    for (final Future<Void> future : batch) {
      try {
        future.get();
      } catch (final ExecutionException error) {
        final Throwable cause = error.getCause() == null ? error : error.getCause();
        if (failure == null) {
          failure = cause;
        } else {
          ExceptionUtils.addSuppressedIfDistinct(failure, cause);
        }
      }
    }

    if (failure instanceof final RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof final Error fatal) {
      throw fatal;
    }
    if (failure != null) {
      throw new IllegalStateException("concurrent operation failed", failure);
    }
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String operationLabel = operation.label();
    final Span span =
        tracer
            .spanBuilder("basic-kv." + operationLabel)
            .setNoParent()
            .setAttribute("db.operation.name", operationLabel)
            .setAttribute("db.key", key)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      Failsafe.with(retryPolicy).run(action::run);
      span.setStatus(StatusCode.OK);
    } catch (final RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordOperation(operationLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  private void verifyGet(final Operation operation, final String key, final GetOption option) {
    final Optional<KeyValue> expected = expectedGet(operation, key);
    final GetResult result = client.get(key, Set.of(option));
    final Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, expected.toString(), actual.toString());
    }
  }

  private Optional<KeyValue> expectedGet(final Operation operation, final String key) {
    return switch (operation) {
      case GET -> inference.get(key);
      case FLOOR -> inference.floor(key);
      case CEILING -> inference.ceiling(key);
      case LOWER -> inference.lower(key);
      case HIGHER -> inference.higher(key);
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    };
  }

  private void verifyDelete(final Operation operation, final String key, final boolean expected) {
    final boolean actual = client.delete(key);
    if (expected != actual) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, Boolean.toString(expected), Boolean.toString(actual));
    }
  }

  private void verifyRangeScan(final Operation operation, final String key, final String endKey) {
    final List<KeyValue> expected = inference.range(key, endKey);
    final List<KeyValue> actual = new ArrayList<>();
    try (final CloseableIterable<GetResult> results = client.rangeScan(key, endKey)) {
      for (final GetResult result : results) {
        actual.add(new KeyValue(result.key(), result.value()));
      }
    }
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
    }
  }

  private void verifyList(final Operation operation, final String key, final String endKey) {
    final List<String> expected = inference.list(key, endKey);
    final List<String> actual = client.list(key, endKey);
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
    }
  }

  private void checkpoint(final Checkpoint checkpoint) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder("basic-kv.checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      final String firstKey = keyGenerator.lowerGuardKey();
      final String afterLastKey = keyGenerator.afterUpperGuardKey();
      final List<KeyValue> expected = inference.range(firstKey, afterLastKey);
      final List<KeyValue> actual = new ArrayList<>();
      Failsafe.with(retryPolicy)
          .run(
              () -> {
                actual.clear();
                try (final CloseableIterable<GetResult> results =
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
    } catch (final RuntimeException error) {
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

  static BatchType selectBatch(final double selected) {
    return batchType(selectOperation(selected));
  }

  private static Operation selectOperationForBatch(
      final BatchType batchType, final SplittableRandom random) {
    if (batchType == BatchType.DELETE_RANGE) {
      return Operation.DELETE_RANGE;
    }

    Operation operation;
    do {
      operation = selectOperation(random.nextDouble(TOTAL_WEIGHT));
    } while (batchType(operation) != batchType);
    return operation;
  }

  private static BatchType batchType(final Operation operation) {
    return switch (operation) {
      case PUT, DELETE -> BatchType.WRITE;
      case GET, FLOOR, CEILING, LOWER, HIGHER, RANGE_SCAN, LIST -> BatchType.READ;
      case DELETE_RANGE -> BatchType.DELETE_RANGE;
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    };
  }

  static Operation selectOperation(final double selected) {
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

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
package io.oxia.chaos.testcase.basickv;

import static io.oxia.chaos.testcase.basickv.Constants.CEILING_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.DELETE_RANGE_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.DELETE_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.FLOOR_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.GET_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.HIGHER_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.LOWER_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.OPERATION_ATTEMPT_TIMEOUT;
import static io.oxia.chaos.testcase.basickv.Constants.PUT_THRESHOLD;
import static io.oxia.chaos.testcase.basickv.Constants.TOTAL_WEIGHT;
import static io.oxia.chaos.util.KeyValueUtils.sortedByKey;
import static io.oxia.chaos.util.Timing.addSaturated;
import static io.oxia.chaos.util.Timing.elapsedMillis;

import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.ViolationException;
import io.oxia.chaos.observability.CaseTelemetry;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.CaseContext;
import io.oxia.chaos.testcase.TestCase;
import io.oxia.chaos.util.BatchUtils;
import io.oxia.chaos.util.DeleteUtils;
import io.oxia.chaos.util.GuardUtils;
import io.oxia.chaos.util.KeyGenerator;
import io.oxia.chaos.util.KeyValue;
import io.oxia.chaos.util.RangeUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.GetOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/** The basic key/value correctness case, including its fixed operation distribution. */
@Slf4j
public final class BasicKv implements TestCase {

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final Tracer tracer;
  private final CaseTelemetry telemetry;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final KeyGenerator keyGenerator;
  private final ValueGenerator valueGenerator;
  // What Oxia must hold: every acknowledged write applied in order.
  private final ConcurrentSkipListMap<String, byte[]> reference = new ConcurrentSkipListMap<>();

  private long operationCount;
  private long checkpointCount;

  public BasicKv(final CaseContext context) {
    this.options = context.options();
    this.runId = context.runId();
    this.client = context.client();
    this.tracer = context.tracer(Options.BASIC_KV);
    this.telemetry = new CaseTelemetry(tracer, context.metrics(), Options.BASIC_KV);
    this.retryPolicy = RetryUtils.retryPolicy(context.retryTimeout());
    this.attemptTimeout =
        RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, context.retryTimeout());
    this.keyGenerator = new KeyGenerator(runId);
    this.valueGenerator = new ValueGenerator(options.seed());
    context.metrics().observeReferenceKeys(reference::size);
  }

  @Override
  public String name() {
    return Options.BASIC_KV;
  }

  /** Writes the guards and every key. */
  @Override
  public void bootstrap() {
    final Span span = tracer.spanBuilder("basic-kv.warmup").setNoParent().startSpan();
    final long started = System.nanoTime();
    try (final Scope ignored = Context.root().makeCurrent()) {
      runWithRetry(
          () ->
              GuardUtils.putReferenceGuard(
                  client, reference, keyGenerator.lowerGuardKey(), "lower"));
      for (int index = 0; index < options.keyCount(); index++) {
        final int keyIndex = index;
        final String key = keyGenerator.key(keyIndex);
        final byte[] value = valueGenerator.warmup(keyIndex);
        runWithRetry(
            () -> {
              client.put(key, value);
              reference.put(key, value);
            });
      }
      runWithRetry(
          () ->
              GuardUtils.putReferenceGuard(
                  client, reference, keyGenerator.upperGuardKey(), "upper"));
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

  @Override
  public void execute() throws InterruptedException {
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
        final List<Runnable> referenceUpdates = new ArrayList<>(maximumBatchSize);
        final Set<Integer> writeKeys = new HashSet<>(maximumBatchSize);
        int generated = 0;
        while (generated < maximumBatchSize && System.nanoTime() < deadline) {
          pacer.beforeOperation();
          if (System.nanoTime() >= deadline) {
            break;
          }
          final long sequence = operationCount + generated;
          addNextOperation(batchType, random, sequence, writeKeys, operations, referenceUpdates);
          generated++;
        }

        BatchUtils.executeConcurrentBatch(executor, operations);
        for (final Runnable referenceUpdate : referenceUpdates) {
          referenceUpdate.run();
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
      final List<Runnable> referenceUpdates) {
    switch (batchType) {
      case WRITE -> addWriteOperation(random, sequence, writeKeys, operations, referenceUpdates);
      case READ -> addReadOperation(random, operations);
      case DELETE_RANGE -> addDeleteRangeOperation(random, operations, referenceUpdates);
      default -> throw new IllegalStateException("unreachable batch type: " + batchType);
    }
  }

  private void addWriteOperation(
      final SplittableRandom random,
      final long sequence,
      final Set<Integer> writeKeys,
      final List<Callable<Void>> operations,
      final List<Runnable> referenceUpdates) {
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
        referenceUpdates.add(() -> reference.put(key, value));
      }
      case DELETE -> {
        final boolean expected = reference.containsKey(key);
        operations.add(
            () -> {
              final AtomicBoolean attempted = new AtomicBoolean();
              observe(operation, key, () -> verifyDelete(operation, key, expected, attempted));
              return null;
            });
        referenceUpdates.add(() -> reference.remove(key));
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
      case RANGE_SCAN -> {
        final String endKey =
            keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
        operations.add(
            () -> {
              observe(operation, key, () -> verifyRangeScan(operation, key, endKey));
              return null;
            });
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void addDeleteRangeOperation(
      final SplittableRandom random,
      final List<Callable<Void>> operations,
      final List<Runnable> referenceUpdates) {
    final int index = random.nextInt(options.keyCount());
    final String key = keyGenerator.key(index);
    final String endKey = keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
    operations.add(
        () -> {
          observe(Operation.DELETE_RANGE, key, () -> client.deleteRange(key, endKey));
          return null;
        });
    referenceUpdates.add(() -> reference.subMap(key, endKey).clear());
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

  private void observe(final Operation operation, final String key, final Runnable action) {
    telemetry.observe(operation, key, () -> runWithRetry(action));
  }

  private void verifyGet(final Operation operation, final String key, final GetOption option) {
    final Optional<KeyValue> expected = expectedGet(operation, key);
    final GetResult result = client.get(key, Set.of(option));
    final Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw ViolationException.operationMismatch(
          operation, key, expected.toString(), actual.toString());
    }
  }

  private Optional<KeyValue> expectedGet(final Operation operation, final String key) {
    return switch (operation) {
      case GET -> Optional.ofNullable(reference.get(key)).map(value -> new KeyValue(key, value));
      case FLOOR -> keyValue(reference.floorEntry(key));
      case CEILING -> keyValue(reference.ceilingEntry(key));
      case LOWER -> keyValue(reference.lowerEntry(key));
      case HIGHER -> keyValue(reference.higherEntry(key));
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    };
  }

  private static Optional<KeyValue> keyValue(final Map.Entry<String, byte[]> entry) {
    return entry == null
        ? Optional.empty()
        : Optional.of(new KeyValue(entry.getKey(), entry.getValue()));
  }

  private void verifyDelete(
      final Operation operation,
      final String key,
      final boolean expected,
      final AtomicBoolean attempted) {
    final boolean retry = attempted.getAndSet(true);
    DeleteUtils.verifyDelete(operation, key, expected, client.delete(key), retry);
  }

  private void verifyRangeScan(final Operation operation, final String key, final String endKey) {
    final List<KeyValue> expected =
        reference.subMap(key, endKey).entrySet().stream()
            .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
            .toList();
    final List<KeyValue> unorderedActual = new ArrayList<>();
    try (final CloseableIterable<GetResult> results = client.rangeScan(key, endKey)) {
      for (final GetResult result : results) {
        unorderedActual.add(new KeyValue(result.key(), result.value()));
      }
    }
    final List<KeyValue> actual = sortedByKey(unorderedActual);
    if (!expected.equals(actual)) {
      throw ViolationException.operationMismatch(
          operation, key, SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
    }
  }

  private void checkpoint(final Checkpoint checkpoint) {
    final long started = System.nanoTime();
    telemetry.checkpoint(
        checkpoint,
        () -> {
          final String firstKey = keyGenerator.lowerGuardKey();
          final String afterLastKey = keyGenerator.afterUpperGuardKey();
          final List<KeyValue> expected =
              reference.subMap(firstKey, afterLastKey).entrySet().stream()
                  .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
                  .toList();
          final List<KeyValue> unorderedActual = new ArrayList<>();
          runWithRetry(
              () -> {
                unorderedActual.clear();
                try (final CloseableIterable<GetResult> results =
                    client.rangeScan(firstKey, afterLastKey)) {
                  for (final GetResult result : results) {
                    unorderedActual.add(new KeyValue(result.key(), result.value()));
                  }
                }
              });
          final List<KeyValue> actual = sortedByKey(unorderedActual);
          if (!expected.equals(actual)) {
            throw ViolationException.checkpointMismatch(
                SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
          }
          checkpointCount++;
          LOGGER
              .atInfo()
              .addKeyValue("run_id", runId)
              .addKeyValue("kind", checkpoint.label())
              .addKeyValue("keys", expected.size())
              .addKeyValue("operations", operationCount)
              .addKeyValue("duration_ms", elapsedMillis(started))
              .log("basic-kv checkpoint passed");
          return expected.size();
        });
  }

  @Override
  public void verify() {
    checkpoint(Checkpoint.FINAL);
  }

  /** Deletes every key the case wrote, guards included. */
  @Override
  public void shutdown() {
    runWithRetry(
        () -> client.deleteRange(keyGenerator.lowerGuardKey(), keyGenerator.afterUpperGuardKey()));
    reference.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("basic-kv cleanup completed");
  }

  @Override
  public Map<String, Object> summary() {
    return Map.of("operations", operationCount, "checkpoints", checkpointCount);
  }

  static BatchType selectBatch(final double selected) {
    return batchType(selectOperation(selected));
  }

  private void runWithRetry(final Runnable action) {
    RetryUtils.run(retryPolicy, attemptTimeout, action);
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
      case GET, FLOOR, CEILING, LOWER, HIGHER, RANGE_SCAN -> BatchType.READ;
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
    return Operation.RANGE_SCAN;
  }
}

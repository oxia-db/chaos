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
import io.oxia.chaos.obser.Telemetry;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.CaseContext;
import io.oxia.chaos.testcase.TestCase;
import io.oxia.chaos.util.BatchUtils;
import io.oxia.chaos.util.DeleteUtils;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
  private final Telemetry telemetry;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final KeyGenerator keyGenerator;
  private final ValueGenerator valueGenerator;
  // What Oxia must hold: every acknowledged write applied in order.
  private final ConcurrentSkipListMap<String, byte[]> reference;

  private long operationCount;
  private long checkpointCount;

  public BasicKv(final CaseContext context) {
    this.options = context.options();
    this.runId = context.runId();
    this.client = context.client();
    this.tracer = context.tracer(Options.BASIC_KV);
    this.telemetry = new Telemetry(tracer, context.metrics(), Options.BASIC_KV);
    this.retryPolicy = RetryUtils.retryPolicy(context.retryTimeout());
    this.attemptTimeout =
        RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, context.retryTimeout());
    this.keyGenerator = new KeyGenerator(runId);
    this.valueGenerator = new ValueGenerator(options.seed());
    this.reference = new ConcurrentSkipListMap<>();
    context.metrics().observeReferenceKeys(reference::size);
  }

  @Override
  public String name() {
    return Options.BASIC_KV;
  }

  /** Runs warmup, the timed workload and a final checkpoint. */
  @Override
  public void start() throws InterruptedException {
    doWarmUp();
    doWorkload();
    doCheckpoint(Checkpoint.FINAL);
    LOGGER
        .atInfo()
        .addKeyValue("run_id", runId)
        .addKeyValue("operations", operationCount)
        .addKeyValue("checkpoints", checkpointCount)
        .log("basic-kv completed");
  }

  /** Writes the guards and every key. */
  private void doWarmUp() {
    final Span span = tracer.spanBuilder("basic-kv.warmup").setNoParent().startSpan();
    final long started = System.nanoTime();
    try (final Scope ignored = Context.root().makeCurrent()) {
      // The guards around the keys keep ordered gets and scans inside the keys of this run
      final Map<String, byte[]> initial = new LinkedHashMap<>();
      initial.put(keyGenerator.lowerGuardKey(), "guard-lower".getBytes(StandardCharsets.UTF_8));
      for (int index = 0; index < options.keyCount(); index++) {
        initial.put(keyGenerator.key(index), valueGenerator.warmup(index));
      }
      initial.put(keyGenerator.upperGuardKey(), "guard-upper".getBytes(StandardCharsets.UTF_8));
      initial.forEach(
          (final String key, final byte[] value) ->
              RetryUtils.run(
                  retryPolicy,
                  attemptTimeout,
                  () -> {
                    client.put(key, value);
                    reference.put(key, value);
                  }));
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

  private void doWorkload() throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());

    final Thread.Builder.OfVirtual threads = Thread.ofVirtual().name("oxia-chaos-operation-", 0);
    try (final ExecutorService executor =
        Context.taskWrapping(Executors.newThreadPerTaskExecutor(threads.factory()))) {
      while (System.nanoTime() < deadline) {
        final BatchType batchType = batchType(selectOperation(random.nextDouble(TOTAL_WEIGHT)));
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
          switch (batchType) {
            case WRITE ->
                addWriteOperation(random, sequence, writeKeys, operations, referenceUpdates);
            case READ -> addReadOperation(random, operations);
            case DELETE_RANGE -> {
              final int index = random.nextInt(options.keyCount());
              final String key = keyGenerator.key(index);
              final String endKey =
                  keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
              operations.add(
                  () -> {
                    telemetry.observe(
                        Operation.DELETE_RANGE,
                        key,
                        () ->
                            RetryUtils.run(
                                retryPolicy,
                                attemptTimeout,
                                () -> client.deleteRange(key, endKey)));
                    return null;
                  });
              referenceUpdates.add(() -> reference.subMap(key, endKey).clear());
            }
            default -> throw new IllegalStateException("unreachable batch type: " + batchType);
          }
          generated++;
        }

        BatchUtils.executeConcurrentBatch(executor, operations);
        for (final Runnable referenceUpdate : referenceUpdates) {
          referenceUpdate.run();
        }
        operationCount += generated;

        final long now = System.nanoTime();
        if (now >= nextCheckpoint && now < deadline) {
          doCheckpoint(Checkpoint.PERIODIC);
          nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
        }
      }
    }
  }

  private void addWriteOperation(
      final SplittableRandom random,
      final long sequence,
      final Set<Integer> writeKeys,
      final List<Callable<Void>> operations,
      final List<Runnable> referenceUpdates) {
    final Operation operation = selectOperationForBatch(BatchType.WRITE, random);
    int unique;
    do {
      unique = random.nextInt(options.keyCount());
    } while (!writeKeys.add(unique));
    final int index = unique;
    final String key = keyGenerator.key(index);

    switch (operation) {
      case PUT -> {
        final byte[] value = valueGenerator.next(index, sequence);
        operations.add(
            () -> {
              telemetry.observe(
                  operation,
                  key,
                  () -> RetryUtils.run(retryPolicy, attemptTimeout, () -> client.put(key, value)));
              return null;
            });
        referenceUpdates.add(() -> reference.put(key, value));
      }
      case DELETE -> {
        final boolean expected = reference.containsKey(key);
        operations.add(
            () -> {
              final AtomicBoolean attempted = new AtomicBoolean();
              telemetry.observe(
                  operation,
                  key,
                  () ->
                      RetryUtils.run(
                          retryPolicy,
                          attemptTimeout,
                          () -> {
                            // Marked before the call, so an attempt that fails still makes the
                            // next one a retry
                            final boolean retry = attempted.getAndSet(true);
                            DeleteUtils.verifyDelete(
                                operation, key, expected, client.delete(key), retry);
                          }));
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
      case GET, FLOOR, CEILING, LOWER, HIGHER ->
          operations.add(
              () -> {
                telemetry.observe(
                    operation,
                    key,
                    () ->
                        RetryUtils.run(
                            retryPolicy, attemptTimeout, () -> verifyGet(operation, key)));
                return null;
              });
      case RANGE_SCAN -> {
        final String endKey =
            keyGenerator.key(RangeUtils.nextEnd(random, index, options.keyCount()));
        operations.add(
            () -> {
              telemetry.observe(
                  operation,
                  key,
                  () ->
                      RetryUtils.run(
                          retryPolicy,
                          attemptTimeout,
                          () -> verifyRangeScan(operation, key, endKey)));
              return null;
            });
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void verifyGet(final Operation operation, final String key) {
    final GetOption option;
    final Map.Entry<String, byte[]> entry;
    switch (operation) {
      case GET -> {
        option = GetOption.ComparisonEqual;
        final byte[] value = reference.get(key);
        entry = value == null ? null : Map.entry(key, value);
      }
      case FLOOR -> {
        option = GetOption.ComparisonFloor;
        entry = reference.floorEntry(key);
      }
      case CEILING -> {
        option = GetOption.ComparisonCeiling;
        entry = reference.ceilingEntry(key);
      }
      case LOWER -> {
        option = GetOption.ComparisonLower;
        entry = reference.lowerEntry(key);
      }
      case HIGHER -> {
        option = GetOption.ComparisonHigher;
        entry = reference.higherEntry(key);
      }
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    }
    final Optional<KeyValue> expected =
        entry == null
            ? Optional.empty()
            : Optional.of(new KeyValue(entry.getKey(), entry.getValue()));
    final GetResult result = client.get(key, Set.of(option));
    final Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw ViolationException.operationMismatch(
          operation, key, expected.toString(), actual.toString());
    }
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

  private void doCheckpoint(final Checkpoint checkpoint) {
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
          RetryUtils.run(
              retryPolicy,
              attemptTimeout,
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

  /** Deletes every key the case wrote, guards included. */
  @Override
  public void close() {
    RetryUtils.run(
        retryPolicy,
        attemptTimeout,
        () -> client.deleteRange(keyGenerator.lowerGuardKey(), keyGenerator.afterUpperGuardKey()));
    reference.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("basic-kv cleanup completed");
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

  static BatchType batchType(final Operation operation) {
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

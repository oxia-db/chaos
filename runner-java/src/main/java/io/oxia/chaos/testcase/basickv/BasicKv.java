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
import io.oxia.chaos.testcase.basickv.deleterange.DeleteRange;
import io.oxia.chaos.testcase.basickv.read.OrderedGet;
import io.oxia.chaos.testcase.basickv.read.RangeScan;
import io.oxia.chaos.testcase.basickv.write.Delete;
import io.oxia.chaos.testcase.basickv.write.Put;
import io.oxia.chaos.util.BatchUtils;
import io.oxia.chaos.util.KeyGenerator;
import io.oxia.chaos.util.KeyValue;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

/** The basic key/value correctness case, including its fixed operation distribution. */
@Slf4j
public final class BasicKv implements TestCase {

  private static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);

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
  private final List<OperationStrategy> strategies;

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
    this.strategies =
        strategies(
            new Workload(client, reference, keyGenerator, valueGenerator, options.keyCount()));
    context.metrics().observeReferenceKeys(reference::size);
  }

  /** The operations of the workload, each with its weight. */
  static List<OperationStrategy> strategies(final Workload workload) {
    return List.of(
        new Put(0.20, workload),
        new OrderedGet(Operation.GET, 0.15, workload),
        new OrderedGet(Operation.FLOOR, 0.05, workload),
        new OrderedGet(Operation.CEILING, 0.05, workload),
        new OrderedGet(Operation.LOWER, 0.05, workload),
        new OrderedGet(Operation.HIGHER, 0.05, workload),
        new Delete(0.08, workload),
        new DeleteRange(0.0025, workload),
        new RangeScan(0.05, workload));
  }

  /** Picks the strategy that {@code selected}, in {@code [0, total weight)}, falls on. */
  static OperationStrategy select(final List<OperationStrategy> strategies, final double selected) {
    double threshold = 0;
    for (final OperationStrategy strategy : strategies) {
      threshold += strategy.weight();
      if (selected < threshold) {
        return strategy;
      }
    }
    return strategies.getLast();
  }

  static double totalWeight(final List<OperationStrategy> strategies) {
    return strategies.stream().mapToDouble(OperationStrategy::weight).sum();
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
    final double totalWeight = totalWeight(strategies);

    final Thread.Builder.OfVirtual threads = Thread.ofVirtual().name("oxia-chaos-operation-", 0);
    try (final ExecutorService executor =
        Context.taskWrapping(Executors.newThreadPerTaskExecutor(threads.factory()))) {
      while (System.nanoTime() < deadline) {
        final BatchType batchType = select(strategies, random.nextDouble(totalWeight)).batchType();
        // The writes of a batch are on distinct keys, so their order doesn't matter
        final int maximumBatchSize =
            batchType == BatchType.WRITE
                ? Math.min(options.batchSize(), options.keyCount())
                : options.batchSize();
        final List<Callable<Void>> operations = new ArrayList<>(maximumBatchSize);
        final List<ReferenceUpdate> referenceUpdates = new ArrayList<>(maximumBatchSize);
        final Set<Integer> writeKeys = new HashSet<>(maximumBatchSize);
        int generated = 0;
        while (generated < maximumBatchSize && System.nanoTime() < deadline) {
          pacer.beforeOperation();
          if (System.nanoTime() >= deadline) {
            break;
          }
          OperationStrategy strategy;
          do {
            strategy = select(strategies, random.nextDouble(totalWeight));
          } while (strategy.batchType() != batchType);
          int index;
          do {
            index = random.nextInt(options.keyCount());
          } while (batchType == BatchType.WRITE && !writeKeys.add(index));
          final Operation operation = strategy.operation();
          final Planned planned = strategy.plan(random, index, operationCount + generated);
          operations.add(
              () -> {
                telemetry.observe(
                    operation,
                    planned.key(),
                    () -> RetryUtils.run(retryPolicy, attemptTimeout, planned.attempt()::run));
                return null;
              });
          referenceUpdates.add(planned.referenceUpdate());
          generated++;
        }

        BatchUtils.executeConcurrentBatch(executor, operations);
        for (final ReferenceUpdate referenceUpdate : referenceUpdates) {
          referenceUpdate.apply();
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
}

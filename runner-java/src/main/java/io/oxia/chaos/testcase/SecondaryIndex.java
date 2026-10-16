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

import static io.oxia.chaos.util.KeyValueUtils.sortedByKey;
import static io.oxia.chaos.util.Timing.addSaturated;
import static io.oxia.chaos.util.Timing.elapsedMillis;
import static io.oxia.chaos.util.Timing.elapsedSeconds;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.KeyValue;
import io.oxia.chaos.inference.SecondaryIndexModel;
import io.oxia.chaos.inference.SecondaryIndexModel.IndexedRecord;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.RangeUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that secondary indexes follow their records.
 *
 * <p>Puts give records zero to two secondary keys on each of two indexes, from a key space smaller
 * than the record count, so secondary keys are shared. Updates change or drop them, and deletes and
 * delete-ranges remove the records. Index gets, lists, and range scans must then return exactly the
 * entries of the current records: no entry of a dropped key or a deleted record, and none missing.
 * Operations run one at a time, so each read has a single expected answer.
 *
 * <p>Index gets only use the equal comparison: for the other comparisons, the Java client picks the
 * result of a multi-shard get by primary key rather than by secondary key.
 */
public final class SecondaryIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryIndex.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.secondary-index";
  private static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  static final List<String> INDEXES = List.of("bucket", "label");
  static final int MAX_SECONDARY_KEYS_PER_INDEX = 2;
  private static final double PUT_THRESHOLD = 0.30;
  private static final double DELETE_THRESHOLD = PUT_THRESHOLD + 0.08;
  private static final double DELETE_RANGE_THRESHOLD = DELETE_THRESHOLD + 0.01;
  private static final double GET_THRESHOLD = DELETE_RANGE_THRESHOLD + 0.15;
  private static final double INDEX_GET_THRESHOLD = GET_THRESHOLD + 0.20;
  private static final double INDEX_LIST_THRESHOLD = INDEX_GET_THRESHOLD + 0.13;
  static final double TOTAL_WEIGHT = INDEX_LIST_THRESHOLD + 0.13;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final SecondaryIndexModel model;
  private final RunnerMetrics metrics;
  private final Tracer tracer;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final ValueGenerator valueGenerator;
  private final SplittableRandom random;
  private final String prefix;
  private final String secondaryPrefix;
  private final int secondaryKeyCount;

  private long operationCount;
  private long checkpointCount;

  public SecondaryIndex(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final SecondaryIndexModel model,
      final RunnerMetrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.model = model;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.retryPolicy = RetryUtils.retryPolicy(retryTimeout);
    this.attemptTimeout = RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, retryTimeout);
    this.valueGenerator = new ValueGenerator(options.seed());
    this.random = new SplittableRandom(options.seed());
    this.prefix = "/oxia-chaos/runs/" + runId + "/secondary-index/key-";
    // Secondary keys have no '/', so both key sortings order them like strings
    this.secondaryPrefix = "oxia-chaos-" + runId + "-";
    this.secondaryKeyCount = secondaryKeyCount(options.keyCount());
  }

  /** Runs warmup, the timed workload, a final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    final Span runSpan =
        tracer
            .spanBuilder("secondary-index.run")
            .setAttribute("test.case", Options.SECONDARY_INDEX)
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
        .log("secondary-index completed");
  }

  private void warmup() {
    final Span span = tracer.spanBuilder("secondary-index.warmup").setNoParent().startSpan();
    final long started = System.nanoTime();
    try (final Scope ignored = Context.root().makeCurrent()) {
      for (int index = 0; index < options.keyCount(); index++) {
        final String key = key(index);
        final byte[] value = valueGenerator.warmup(index);
        final Map<String, Set<String>> secondary = nextSecondaryKeys();
        runWithRetry(() -> put(key, value, secondary));
        model.put(key, value, secondary);
      }
      span.setAttribute("test.keys", options.keyCount()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("keys", options.keyCount())
          .addKeyValue("secondary_keys", secondaryKeyCount)
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("secondary-index warmup completed");
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

    while (System.nanoTime() < deadline) {
      pacer.beforeOperation();
      if (System.nanoTime() >= deadline) {
        break;
      }
      executeOperation(selectOperation(random.nextDouble(TOTAL_WEIGHT)));
      operationCount++;

      final long now = System.nanoTime();
      if (now >= nextCheckpoint && now < deadline) {
        checkpoint(Checkpoint.PERIODIC);
        nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
      }
    }
  }

  private void executeOperation(final Operation operation) {
    switch (operation) {
      case PUT -> {
        final int index = random.nextInt(options.keyCount());
        final String key = key(index);
        final byte[] value = valueGenerator.next(index, operationCount);
        final Map<String, Set<String>> secondary = nextSecondaryKeys();
        observe(operation, key, () -> put(key, value, secondary));
        model.put(key, value, secondary);
      }
      case DELETE -> {
        final String key = key(random.nextInt(options.keyCount()));
        final boolean expected = model.get(key).isPresent();
        final AtomicBoolean attempted = new AtomicBoolean();
        observe(
            operation,
            key,
            () -> {
              final boolean retry = attempted.getAndSet(true);
              final boolean actual = client.delete(key);
              // A failed attempt has an unknown outcome: the server may have applied it, so a
              // retry of a delete expected to find the key may find it already deleted.
              if (expected != actual && !(retry && expected)) {
                throw CorrectnessViolationException.operationMismatch(
                    operation, key, Boolean.toString(expected), Boolean.toString(actual));
              }
            });
        model.delete(key);
      }
      case DELETE_RANGE -> {
        final int index = random.nextInt(options.keyCount());
        final String key = key(index);
        final String endKey = key(RangeUtils.nextEnd(random, index, options.keyCount()));
        observe(operation, key, () -> client.deleteRange(key, endKey));
        model.deleteRange(key, endKey);
      }
      case GET -> {
        final String key = key(random.nextInt(options.keyCount()));
        observe(operation, key, () -> verifyGet(key));
      }
      case INDEX_GET -> {
        final String indexName = INDEXES.get(random.nextInt(INDEXES.size()));
        final String secondaryKey = secondaryKey(random.nextInt(secondaryKeyCount));
        observe(operation, secondaryKey, () -> verifyIndexGet(indexName, secondaryKey));
      }
      case INDEX_LIST, INDEX_RANGE_SCAN -> {
        final String indexName = INDEXES.get(random.nextInt(INDEXES.size()));
        final int index = random.nextInt(secondaryKeyCount);
        final String start = secondaryKey(index);
        final String end = secondaryKey(RangeUtils.nextEnd(random, index, secondaryKeyCount));
        observe(
            operation,
            start,
            () -> {
              if (operation == Operation.INDEX_LIST) {
                verifyIndexList(indexName, start, end);
              } else {
                verifyIndexRangeScan(indexName, start, end);
              }
            });
      }
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void verifyGet(final String key) {
    final byte[] expected = model.get(key).map(IndexedRecord::value).orElse(null);
    final GetResult result = client.get(key);
    final byte[] actual = result == null ? null : result.value();
    if (!Arrays.equals(expected, actual)) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.GET, key, describe(expected), describe(actual));
    }
  }

  /** Secondary keys aren't unique: the get may return any record that has the key. */
  private void verifyIndexGet(final String indexName, final String secondaryKey) {
    final Set<String> expected = model.primaries(indexName, secondaryKey);
    final GetResult result = client.get(secondaryKey, Set.of(GetOption.UseIndex(indexName)));
    final boolean matches =
        result == null
            ? expected.isEmpty()
            : expected.contains(result.key())
                && Arrays.equals(model.get(result.key()).orElseThrow().value(), result.value());
    if (!matches) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.INDEX_GET,
          indexName + ":" + secondaryKey,
          "one of " + SummaryUtils.summarize(expected.stream().sorted().toList()),
          result == null ? "none" : new KeyValue(result.key(), result.value()).toString());
    }
  }

  private void verifyIndexList(final String indexName, final String start, final String end) {
    final List<String> expected =
        model.indexRange(indexName, start, end).stream().map(IndexedRecord::key).sorted().toList();
    final List<String> actual =
        client.list(start, end, Set.of(ListOption.UseIndex(indexName))).stream().sorted().toList();
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.INDEX_LIST,
          indexName + ":[" + start + ", " + end + ")",
          SummaryUtils.summarize(expected),
          SummaryUtils.summarize(actual));
    }
  }

  private void verifyIndexRangeScan(final String indexName, final String start, final String end) {
    final List<KeyValue> expected =
        sortedByKey(
            model.indexRange(indexName, start, end).stream()
                .map((final IndexedRecord record) -> new KeyValue(record.key(), record.value()))
                .toList());
    final List<KeyValue> unorderedActual = new ArrayList<>();
    try (final CloseableIterable<GetResult> results =
        client.rangeScan(start, end, Set.of(RangeScanOption.UseIndex(indexName)))) {
      for (final GetResult result : results) {
        unorderedActual.add(new KeyValue(result.key(), result.value()));
      }
    }
    final List<KeyValue> actual = sortedByKey(unorderedActual);
    if (!expected.equals(actual)) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.INDEX_RANGE_SCAN,
          indexName + ":[" + start + ", " + end + ")",
          SummaryUtils.summarize(expected),
          SummaryUtils.summarize(actual));
    }
  }

  /** Compares the records and every entry of both indexes with the model. */
  private void checkpoint(final Checkpoint checkpoint) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder("secondary-index.checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      final String endKey = prefix + "~";
      final List<KeyValue> expected =
          sortedByKey(
              model.range(prefix, endKey).stream()
                  .map((final IndexedRecord record) -> new KeyValue(record.key(), record.value()))
                  .toList());
      final List<KeyValue> unorderedActual = new ArrayList<>();
      runWithRetry(
          () -> {
            unorderedActual.clear();
            try (final CloseableIterable<GetResult> results = client.rangeScan(prefix, endKey)) {
              for (final GetResult result : results) {
                unorderedActual.add(new KeyValue(result.key(), result.value()));
              }
            }
          });
      final List<KeyValue> actual = sortedByKey(unorderedActual);
      if (!expected.equals(actual)) {
        throw CorrectnessViolationException.checkpointMismatch(
            SummaryUtils.summarize(expected), SummaryUtils.summarize(actual));
      }
      final String secondaryEnd = secondaryPrefix + "~";
      for (final String indexName : INDEXES) {
        runWithRetry(() -> verifyIndexList(indexName, secondaryPrefix, secondaryEnd));
        runWithRetry(() -> verifyIndexRangeScan(indexName, secondaryPrefix, secondaryEnd));
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
          .log("secondary-index checkpoint passed");
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
    // Deleting the records also deletes their index entries
    runWithRetry(() -> client.deleteRange(prefix, prefix + "~"));
    model.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("secondary-index cleanup completed");
  }

  private void put(final String key, final byte[] value, final Map<String, Set<String>> secondary) {
    try {
      client.put(key, value, putOptions(secondary));
    } catch (final UnexpectedVersionIdException | KeyAlreadyExistsException error) {
      throw new IllegalStateException("unconditional put failed its condition: " + key, error);
    }
  }

  /**
   * Picks zero to {@link #MAX_SECONDARY_KEYS_PER_INDEX} secondary keys for each index. Two picks
   * may be the same key, which gives the record a single entry for it.
   */
  private Map<String, Set<String>> nextSecondaryKeys() {
    final Map<String, Set<String>> secondary = new LinkedHashMap<>();
    for (final String indexName : INDEXES) {
      final int count = random.nextInt(MAX_SECONDARY_KEYS_PER_INDEX + 1);
      final Set<String> keys = new LinkedHashSet<>();
      for (int pick = 0; pick < count; pick++) {
        keys.add(secondaryKey(random.nextInt(secondaryKeyCount)));
      }
      secondary.put(indexName, keys);
    }
    return secondary;
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String operationLabel = operation.label();
    final Span span =
        tracer
            .spanBuilder("secondary-index." + operationLabel)
            .setNoParent()
            .setAttribute("db.operation.name", operationLabel)
            .setAttribute("db.key", key)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      runWithRetry(action);
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

  private void runWithRetry(final Runnable action) {
    Failsafe.with(retryPolicy, attemptTimeout).run(action::run);
  }

  private String key(final int index) {
    return prefix + String.format(Locale.ROOT, "%08d", index);
  }

  private String secondaryKey(final int index) {
    return secondaryPrefix + String.format(Locale.ROOT, "%06d", index);
  }

  static Set<PutOption> putOptions(final Map<String, Set<String>> secondary) {
    final Set<PutOption> putOptions = new HashSet<>();
    for (final Map.Entry<String, Set<String>> entry : secondary.entrySet()) {
      for (final String secondaryKey : entry.getValue()) {
        putOptions.add(PutOption.SecondaryIndex(entry.getKey(), secondaryKey));
      }
    }
    return putOptions;
  }

  /** Uses fewer secondary keys than records, so records share them. */
  static int secondaryKeyCount(final int keyCount) {
    return Math.max(1, keyCount / 4);
  }

  static Operation selectOperation(final double selected) {
    if (selected < PUT_THRESHOLD) {
      return Operation.PUT;
    }
    if (selected < DELETE_THRESHOLD) {
      return Operation.DELETE;
    }
    if (selected < DELETE_RANGE_THRESHOLD) {
      return Operation.DELETE_RANGE;
    }
    if (selected < GET_THRESHOLD) {
      return Operation.GET;
    }
    if (selected < INDEX_GET_THRESHOLD) {
      return Operation.INDEX_GET;
    }
    if (selected < INDEX_LIST_THRESHOLD) {
      return Operation.INDEX_LIST;
    }
    return Operation.INDEX_RANGE_SCAN;
  }

  private static String describe(final byte[] value) {
    return value == null ? "none" : Base64.getEncoder().encodeToString(value);
  }
}

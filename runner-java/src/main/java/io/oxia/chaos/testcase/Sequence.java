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
import dev.failsafe.Timeout;
import dev.failsafe.function.CheckedSupplier;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.SequenceModel;
import io.oxia.chaos.observability.CaseTelemetry;
import io.oxia.chaos.observability.Metrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.DeleteUtils;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks server-assigned sequence keys and the sequence updates that follow them.
 *
 * <p>Each prefix is one sequence, routed to a single shard by its partition key, with one writer
 * that appends records with random deltas and trims them from the head like a queue consumer, and
 * one subscriber of its sequence updates. Some prefixes use two-part sequence keys. The writer
 * checks that every key the server assigns adds the deltas to the last key, and the subscriber
 * checks that the updates only move forward, to assigned keys, and catch up with the last key.
 */
public final class Sequence {

  private static final Logger LOGGER = LoggerFactory.getLogger(Sequence.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.sequence";
  private static final int PREFIX_COUNT = 8;
  private static final int MAX_DELTA = 3;
  private static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration UPDATES_POLL_INTERVAL = Duration.ofSeconds(1);
  // Longer than the client can back off before it subscribes again, up to about 72 seconds per
  // attempt, plus a further shorter backoff
  private static final Duration UPDATES_DEADLINE = Duration.ofMinutes(3);
  private static final double BURST_PROBABILITY = 0.10;
  private static final int MAX_BURST = 4;
  private static final double PUT_THRESHOLD = 0.50;
  private static final double GET_THRESHOLD = PUT_THRESHOLD + 0.30;
  static final double TOTAL_WEIGHT = GET_THRESHOLD + 0.20;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final SequenceModel model;
  private final Metrics metrics;
  private final Tracer tracer;
  private final CaseTelemetry telemetry;
  private final Duration retryTimeout;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final ValueGenerator valueGenerator;
  private final List<String> prefixes;
  private final int prefixCapacity;

  private long operationCount;
  private long checkpointCount;
  private long adoptedCount;
  private long burstCount;
  private long valueCount;

  public Sequence(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final SequenceModel model,
      final Metrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.model = model;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.telemetry = new CaseTelemetry(tracer, metrics, Options.SEQUENCE);
    this.retryTimeout = retryTimeout;
    this.retryPolicy = RetryUtils.retryPolicy(retryTimeout);
    this.attemptTimeout = RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, retryTimeout);
    this.valueGenerator = new ValueGenerator(options.seed());
    final List<String> created = new ArrayList<>(PREFIX_COUNT);
    for (int index = 0; index < PREFIX_COUNT; index++) {
      // Equal-length names: no prefix is the start of another one's keys
      final String prefix =
          String.format(Locale.ROOT, "/oxia-chaos/runs/%s/sequence/p-%02d", runId, index);
      model.register(prefix, parts(index));
      created.add(prefix);
    }
    this.prefixes = List.copyOf(created);
    this.prefixCapacity = Math.max(2, options.keyCount() / PREFIX_COUNT);
  }

  /** Runs the timed workload, the final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    final Span runSpan =
        tracer
            .spanBuilder("sequences.run")
            .setAttribute("test.case", Options.SEQUENCE)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    final List<Closeable> subscriptions = new ArrayList<>(PREFIX_COUNT);
    Throwable failure = null;
    try (final Scope ignored = runSpan.makeCurrent()) {
      for (final String prefix : prefixes) {
        subscriptions.add(
            client.getSequenceUpdates(
                prefix,
                (final String key) -> model.delivered(prefix, key),
                Set.of(GetSequenceUpdatesOption.PartitionKey(prefix))));
      }
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
        cleanup(subscriptions);
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
        .addKeyValue("adopted_keys", adoptedCount)
        .addKeyValue("bursts", burstCount)
        .log("sequence completed");
  }

  private void executeWorkload() throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());

    while (System.nanoTime() < deadline) {
      pacer.beforeOperation();
      if (System.nanoTime() >= deadline) {
        break;
      }
      final int index = random.nextInt(PREFIX_COUNT);
      final String prefix = prefixes.get(index);
      final int live = model.liveCount(prefix);
      final Operation selected = selectOperation(random.nextDouble(TOTAL_WEIGHT));
      final Operation operation =
          switch (selected) {
            case SEQUENCE_PUT -> live >= prefixCapacity ? Operation.DELETE : selected;
            case GET -> live == 0 ? Operation.SEQUENCE_PUT : selected;
            default -> live < 2 ? Operation.SEQUENCE_PUT : selected;
          };
      switch (operation) {
        case SEQUENCE_PUT -> append(prefix, index, random);
        case GET -> {
          final String key = model.keyAt(prefix, random.nextInt(live)).orElseThrow();
          observe(operation, key, () -> verifyGet(prefix, key));
        }
        case DELETE -> trim(prefix);
        default -> throw new IllegalStateException("unreachable operation: " + operation);
      }
      validateDeliveries(prefix);
      operationCount++;

      final long now = System.nanoTime();
      if (now >= nextCheckpoint && now < deadline) {
        checkpoint(Checkpoint.PERIODIC);
        nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
      }
    }
  }

  /**
   * Appends one record, or occasionally several concurrent ones with the same deltas, so that the
   * client can send them to the server in one batch.
   */
  private void append(final String prefix, final int index, final SplittableRandom random)
      throws InterruptedException {
    final long[] deltas = new long[parts(index)];
    deltas[0] = 1 + random.nextInt(MAX_DELTA);
    for (int part = 1; part < deltas.length; part++) {
      deltas[part] = random.nextInt(MAX_DELTA + 1);
    }
    final int count =
        random.nextDouble() < BURST_PROBABILITY ? 2 + random.nextInt(MAX_BURST - 1) : 1;
    final Set<PutOption> putOptions =
        Set.of(
            PutOption.PartitionKey(prefix),
            PutOption.SequenceKeysDeltas(LongStream.of(deltas).boxed().toList()));
    final List<byte[]> values = new ArrayList<>(count);
    for (int put = 0; put < count; put++) {
      values.add(valueGenerator.next(index, valueCount++));
    }
    final List<AtomicInteger> attempts = new ArrayList<>(count);
    final List<Future<PutResult>> results = new ArrayList<>(count);
    try (final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (final byte[] value : values) {
        final AtomicInteger attempted = new AtomicInteger();
        attempts.add(attempted);
        results.add(
            executor.submit(
                () -> {
                  final AtomicReference<PutResult> result = new AtomicReference<>();
                  observe(
                      Operation.SEQUENCE_PUT,
                      prefix,
                      () ->
                          result.set(
                              retried(
                                  () -> {
                                    attempted.incrementAndGet();
                                    return client.put(prefix, value, putOptions);
                                  })));
                  return result.get();
                }));
      }
    }
    final Map<String, byte[]> acknowledged = new LinkedHashMap<>();
    final List<byte[]> failedValues = new ArrayList<>();
    int failedAttempts = 0;
    for (int put = 0; put < count; put++) {
      final PutResult result;
      try {
        result = results.get(put).get();
      } catch (final ExecutionException error) {
        if (error.getCause() instanceof final RuntimeException runtime) {
          throw runtime;
        }
        throw new IllegalStateException("sequence put failed", error.getCause());
      }
      acknowledged.put(result.key(), values.get(put));
      // Each failed attempt has an unknown outcome: it may have appended a record already
      final int failed = attempts.get(put).get() - 1;
      failedAttempts += failed;
      if (failed > 0) {
        failedValues.add(values.get(put));
      }
    }
    if (acknowledged.size() != count) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.SEQUENCE_PUT,
          prefix,
          count + " distinct keys",
          String.join(", ", acknowledged.keySet()));
    }
    final String expected = SequenceModel.key(prefix, model.next(prefix, deltas));
    final int maxFailed = failedAttempts;
    final List<String> unacknowledged =
        model
            .assign(prefix, deltas, failedAttempts, acknowledged)
            .orElseThrow(
                () ->
                    CorrectnessViolationException.operationMismatch(
                        Operation.SEQUENCE_PUT,
                        prefix,
                        "consecutive keys from "
                            + expected
                            + " after up to "
                            + maxFailed
                            + " applied failed puts",
                        String.join(", ", acknowledged.keySet())));
    // The positions left free hold the records of failed attempts, with one of their values
    for (final String key : unacknowledged) {
      final GetResult record =
          retried(() -> client.get(key, Set.of(GetOption.PartitionKey(prefix))));
      if (record == null
          || failedValues.stream()
              .noneMatch((final byte[] value) -> Arrays.equals(value, record.value()))) {
        throw CorrectnessViolationException.operationMismatch(
            Operation.SEQUENCE_PUT,
            key,
            "a record of a failed put",
            record == null ? "absent" : text(record.value()));
      }
      model.adopt(prefix, key, record.value());
    }
    if (count > 1) {
      burstCount++;
    }
    if (!unacknowledged.isEmpty()) {
      adoptedCount += unacknowledged.size();
      LOGGER
          .atWarn()
          .addKeyValue("prefix", prefix)
          .addKeyValue("keys", unacknowledged)
          .log("failed sequence puts were applied");
    }
  }

  private void trim(final String prefix) {
    final String key = model.trimmable(prefix).orElseThrow();
    observe(
        Operation.DELETE,
        key,
        () -> {
          final AtomicBoolean attempted = new AtomicBoolean();
          RetryUtils.run(
              retryPolicy,
              attemptTimeout,
              () -> {
                final boolean retry = attempted.getAndSet(true);
                final boolean deleted;
                try {
                  deleted = client.delete(key, Set.of(DeleteOption.PartitionKey(prefix)));
                } catch (final UnexpectedVersionIdException error) {
                  throw new IllegalStateException(
                      "unconditional delete failed its condition", error);
                }
                DeleteUtils.verifyDelete(Operation.DELETE, key, true, deleted, retry);
              });
          model.delete(prefix, key);
        });
  }

  private void verifyGet(final String prefix, final String key) {
    final byte[] expected = model.get(prefix, key).orElseThrow();
    final GetResult result = retried(() -> client.get(key, Set.of(GetOption.PartitionKey(prefix))));
    if (result == null || !Arrays.equals(expected, result.value())) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.GET, key, text(expected), result == null ? "absent" : text(result.value()));
    }
  }

  private void validateDeliveries(final String prefix) {
    final Optional<String> violation = model.validateDeliveries(prefix);
    if (violation.isPresent()) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.SEQUENCE_UPDATE, prefix, "non-decreasing assigned keys", violation.get());
    }
  }

  private void checkpoint(final Checkpoint checkpoint) throws InterruptedException {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder("sequences.checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      for (final String prefix : prefixes) {
        verifyPrefix(prefix);
        awaitUpdates(prefix);
      }
      checkpointCount++;
      span.setAttribute("checkpoint.keys", model.size()).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("kind", checkpointLabel)
          .addKeyValue("keys", model.size())
          .addKeyValue("operations", operationCount)
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("sequence checkpoint passed");
    } catch (final RuntimeException | InterruptedException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(checkpointLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  /** Compares every record of the prefix with the model. */
  private void verifyPrefix(final String prefix) {
    final NavigableMap<String, byte[]> expected = model.live(prefix);
    final NavigableMap<String, byte[]> actual =
        retried(
            () -> {
              final NavigableMap<String, byte[]> records = new TreeMap<>();
              try (final CloseableIterable<GetResult> results =
                  client.rangeScan(
                      prefix + "-", prefix + "-~", Set.of(RangeScanOption.PartitionKey(prefix)))) {
                for (final GetResult result : results) {
                  records.put(result.key(), result.value());
                }
              }
              return records;
            });
    if (!expected.keySet().equals(actual.keySet())
        || expected.entrySet().stream()
            .anyMatch(
                (final Map.Entry<String, byte[]> entry) ->
                    !Arrays.equals(entry.getValue(), actual.get(entry.getKey())))) {
      throw CorrectnessViolationException.checkpointMismatch(
          prefix + ": " + describe(expected), describe(actual));
    }
  }

  /**
   * Waits until the sequence updates deliver the last assigned key. Updates that never catch up are
   * a liveness failure, not a violation: the subscriber may be behind while the shard is
   * unavailable, and the server only sends the latest key, so it never sent a wrong one.
   */
  private void awaitUpdates(final String prefix) throws InterruptedException {
    final Optional<String> tail = model.tail(prefix);
    if (tail.isEmpty()) {
      return;
    }
    final long deadline = addSaturated(System.nanoTime(), UPDATES_DEADLINE.toNanos());
    while (true) {
      validateDeliveries(prefix);
      if (model.latestDelivered(prefix).equals(tail)) {
        return;
      }
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
            "sequence updates of "
                + prefix
                + " did not deliver the last key "
                + tail.get()
                + " within "
                + UPDATES_DEADLINE
                + "; latest delivered: "
                + model.latestDelivered(prefix).orElse("none"));
      }
      Thread.sleep(UPDATES_POLL_INTERVAL);
    }
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    telemetry.observe(operation, key, action);
  }

  /** Retries a read. A check that can report a violation runs through {@link RetryUtils#run}. */
  private <T> T retried(final CheckedSupplier<T> action) {
    return Failsafe.with(retryPolicy, attemptTimeout).get(action);
  }

  private void cleanup(final List<Closeable> subscriptions) {
    for (final Closeable subscription : subscriptions) {
      try {
        subscription.close();
      } catch (final IOException error) {
        LOGGER.atWarn().setCause(error).log("failed to close sequence updates");
      }
    }
    for (final String prefix : prefixes) {
      retried(
          () -> {
            client.deleteRange(
                prefix + "-", prefix + "-~", Set.of(DeleteRangeOption.PartitionKey(prefix)));
            return null;
          });
    }
    model.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("sequence cleanup completed");
  }

  /** Even prefixes use one-part sequence keys, odd ones two-part keys. */
  static int parts(final int index) {
    return index % 2 == 0 ? 1 : 2;
  }

  static Operation selectOperation(final double selected) {
    if (selected < PUT_THRESHOLD) {
      return Operation.SEQUENCE_PUT;
    }
    if (selected < GET_THRESHOLD) {
      return Operation.GET;
    }
    return Operation.DELETE;
  }

  private static String text(final byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  private static String describe(final NavigableMap<String, byte[]> records) {
    return SummaryUtils.summarize(
        records.entrySet().stream()
            .map(
                (final Map.Entry<String, byte[]> entry) ->
                    entry.getKey() + "=" + text(entry.getValue()))
            .toList());
  }
}

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
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  // Longer than the client's maximum backoff before it subscribes again, 60 seconds
  private static final Duration UPDATES_GRACE = Duration.ofSeconds(75);
  private static final double PUT_THRESHOLD = 0.50;
  private static final double GET_THRESHOLD = PUT_THRESHOLD + 0.30;
  static final double TOTAL_WEIGHT = GET_THRESHOLD + 0.20;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final SequenceModel model;
  private final RunnerMetrics metrics;
  private final Tracer tracer;
  private final Duration retryTimeout;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final ValueGenerator valueGenerator;
  private final List<String> prefixes;
  private final int prefixCapacity;

  private long operationCount;
  private long checkpointCount;
  private long adoptedCount;

  public Sequence(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final SequenceModel model,
      final RunnerMetrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.model = model;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
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
            .spanBuilder("sequence.run")
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

  private void append(final String prefix, final int index, final SplittableRandom random) {
    final long[] deltas = new long[parts(index)];
    deltas[0] = 1 + random.nextInt(MAX_DELTA);
    for (int part = 1; part < deltas.length; part++) {
      deltas[part] = random.nextInt(MAX_DELTA + 1);
    }
    final byte[] value = valueGenerator.next(index, operationCount);
    final Set<PutOption> putOptions =
        Set.of(
            PutOption.PartitionKey(prefix),
            PutOption.SequenceKeysDeltas(LongStream.of(deltas).boxed().toList()));
    observe(
        Operation.SEQUENCE_PUT,
        prefix,
        () -> {
          final String expected = SequenceModel.key(prefix, model.next(prefix, deltas));
          final AtomicInteger attempts = new AtomicInteger();
          final PutResult result =
              retried(
                  () -> {
                    attempts.incrementAndGet();
                    return client.put(prefix, value, putOptions);
                  });
          // Each failed attempt has an unknown outcome: it may have appended a record already
          final int failedAttempts = attempts.get() - 1;
          final List<String> adopted =
              model
                  .assign(prefix, deltas, failedAttempts, result.key(), value)
                  .orElseThrow(
                      () ->
                          CorrectnessViolationException.operationMismatch(
                              Operation.SEQUENCE_PUT,
                              prefix,
                              expected + " after up to " + failedAttempts + " applied failed puts",
                              result.key()));
          if (!adopted.isEmpty()) {
            adoptedCount += adopted.size();
            LOGGER
                .atWarn()
                .addKeyValue("prefix", prefix)
                .addKeyValue("keys", adopted)
                .log("failed sequence puts were applied");
          }
        });
  }

  private void trim(final String prefix) {
    final String key = model.trimmable(prefix).orElseThrow();
    observe(
        Operation.DELETE,
        key,
        () -> {
          final AtomicBoolean attempted = new AtomicBoolean();
          retried(
              () -> {
                final boolean retry = attempted.getAndSet(true);
                final boolean deleted =
                    client.delete(key, Set.of(DeleteOption.PartitionKey(prefix)));
                // A failed attempt may have deleted the record already
                if (!deleted && !retry) {
                  throw CorrectnessViolationException.operationMismatch(
                      Operation.DELETE, key, "true", "false");
                }
                return null;
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
            .spanBuilder("sequence.checkpoint")
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
   * Waits until the sequence updates deliver the last assigned key. The subscriber may be behind
   * while the shard is unavailable, so only staying behind for the grace period while the shard
   * keeps serving reads is a violation.
   */
  private void awaitUpdates(final String prefix) throws InterruptedException {
    final Optional<String> tail = model.tail(prefix);
    if (tail.isEmpty()) {
      return;
    }
    final long started = System.nanoTime();
    long availableSince = started;
    while (true) {
      validateDeliveries(prefix);
      if (model.latestDelivered(prefix).equals(tail)) {
        return;
      }
      final long now = System.nanoTime();
      if (now - availableSince >= UPDATES_GRACE.toNanos()) {
        throw CorrectnessViolationException.operationMismatch(
            Operation.SEQUENCE_UPDATE,
            prefix,
            tail.get(),
            model.latestDelivered(prefix).orElse("none")
                + " while the shard served reads for "
                + UPDATES_GRACE);
      }
      if (now - started >= retryTimeout.toNanos()) {
        throw new IllegalStateException(
            "sequence updates of " + prefix + " did not catch up while the shard was unavailable");
      }
      Thread.sleep(UPDATES_POLL_INTERVAL);
      try {
        Failsafe.with(attemptTimeout)
            .get(() -> client.get(tail.get(), Set.of(GetOption.PartitionKey(prefix))));
      } catch (final RuntimeException error) {
        availableSince = System.nanoTime();
      }
    }
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String operationLabel = operation.label();
    final Span span =
        tracer
            .spanBuilder("sequence." + operationLabel)
            .setNoParent()
            .setAttribute("db.operation.name", operationLabel)
            .setAttribute("db.key", key)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      action.run();
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

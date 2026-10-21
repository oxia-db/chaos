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
package io.oxia.chaos.testcase.notification;

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
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.KeyValue;
import io.oxia.chaos.inference.NotificationChecker;
import io.oxia.chaos.observability.CaseTelemetry;
import io.oxia.chaos.observability.Metrics;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that the notifications of a subscriber follow the writes exactly.
 *
 * <p>The case subscribes to the namespace's notifications, then puts, deletes and range-deletes its
 * keys. At each checkpoint it stops writing and waits until every acknowledged write was notified:
 * per key in the order of the writes, once each, with the type and version of the write, and
 * nothing else. It then checks that the state the notifications describe is the one a range scan
 * reads. {@link NotificationChecker} describes what each notification is matched against, and what
 * it can't check for range deletes.
 *
 * <p>A notification that doesn't arrive within the retry window is reported as an execution failure
 * rather than a violation: no history explains it, but it may still arrive later.
 */
public final class Notifications {

  private static final Logger LOGGER = LoggerFactory.getLogger(Notifications.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.notification";
  private static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration WARMUP_REPUT_INTERVAL = Duration.ofSeconds(5);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
  private static final double DELETE_RANGE_WEIGHT = 0.05;
  private static final double PUT_WEIGHT = 0.7;
  private static final int MAX_RANGE_KEYS = 8;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final InferenceStore inference;
  private final Metrics metrics;
  private final Tracer tracer;
  private final CaseTelemetry telemetry;
  private final Duration deliveryTimeout;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final ValueGenerator valueGenerator;
  private final String prefix;
  private final List<String> keys;
  private final NotificationChecker checker;

  private volatile long lastFaultNanos;
  private volatile boolean faulted;
  private long operationCount;
  private long rangeCount;
  private long checkpointCount;

  public Notifications(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final InferenceStore inference,
      final Metrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.inference = inference;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.telemetry = new CaseTelemetry(tracer, metrics, Options.NOTIFICATION);
    // Notifications of acknowledged writes must arrive within the window a write may retry in
    this.deliveryTimeout = retryTimeout;
    this.retryPolicy = RetryUtils.retryPolicy(retryTimeout);
    this.attemptTimeout = RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, retryTimeout);
    this.valueGenerator = new ValueGenerator(options.seed());
    this.prefix = "/oxia-chaos/runs/" + runId + "/notification/key-";
    final List<String> created = new ArrayList<>(options.keyCount());
    for (int index = 0; index < options.keyCount(); index++) {
      created.add(prefix + String.format(Locale.ROOT, "%08d", index));
    }
    this.keys = List.copyOf(created);
    this.checker = new NotificationChecker(prefix, keys);
  }

  /** Runs warmup, the timed workload, the final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    final Span runSpan =
        tracer
            .spanBuilder("notification.run")
            .setAttribute("test.case", Options.NOTIFICATION)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    Throwable failure = null;
    try (final Scope ignored = runSpan.makeCurrent();
        final ExecutorService executor =
            Context.taskWrapping(
                Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("oxia-chaos-notification-", 0).factory()))) {
      client.notifications(checker);
      warmup();
      executeWorkload(executor);
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
        final String end = prefix + "~";
        Failsafe.with(retryPolicy, attemptTimeout).run(() -> client.deleteRange(prefix, end));
        inference.deleteRange(prefix, end);
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
        .addKeyValue("range_deletes", rangeCount)
        .addKeyValue("checkpoints", checkpointCount)
        .log("notification completed");
  }

  /**
   * Puts every key until a notification of each arrived. The subscription starts on each shard at
   * the offset the shard committed when it connected, which the client doesn't report: a key
   * notified proves that the stream of its shard covers the writes that follow.
   */
  private void warmup() throws InterruptedException {
    final long started = System.nanoTime();
    final Map<String, Long> versions = new HashMap<>();
    final Map<String, Long> putAt = new HashMap<>();
    for (int index = 0; index < keys.size(); index++) {
      final String key = keys.get(index);
      final byte[] value = valueGenerator.warmup(index);
      versions.put(
          key,
          Failsafe.with(retryPolicy, attemptTimeout)
              .get(() -> client.put(key, value))
              .version()
              .versionId());
      putAt.put(key, System.nanoTime());
      inference.put(key, value);
    }
    // The initial puts of many keys can take long under faults: only waiting for them is bounded
    final long deadline = addSaturated(System.nanoTime(), deliveryTimeout.toNanos());
    while (true) {
      final List<String> missing =
          keys.stream()
              .filter((final String key) -> !checker.observed(key, versions.get(key)))
              .toList();
      if (missing.isEmpty()) {
        break;
      }
      for (final String key : missing) {
        if (System.nanoTime() >= deadline) {
          throw new IllegalStateException(
              "notification subscription delivered nothing for "
                  + SummaryUtils.summarize(missing)
                  + " within "
                  + deliveryTimeout
                  + " of the warmup puts");
        }
        if (System.nanoTime() - putAt.get(key) >= WARMUP_REPUT_INTERVAL.toNanos()) {
          final byte[] value = valueGenerator.warmup(keys.indexOf(key));
          versions.put(
              key,
              Failsafe.with(retryPolicy, attemptTimeout)
                  .get(() -> client.put(key, value))
                  .version()
                  .versionId());
          putAt.put(key, System.nanoTime());
        }
      }
      Thread.sleep(POLL_INTERVAL);
    }
    checker.start(versions);
    LOGGER
        .atInfo()
        .addKeyValue("run_id", runId)
        .addKeyValue("keys", keys.size())
        .addKeyValue("duration_ms", elapsedMillis(started))
        .log("notification warmup completed");
  }

  private void executeWorkload(final ExecutorService executor) throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());

    while (System.nanoTime() < deadline) {
      pacer.beforeOperation();
      if (random.nextDouble() < DELETE_RANGE_WEIGHT) {
        // Alone in its batch: the server keys a range delete notification by its start, which is
        // a key, and keeps one notification per key in each batch it applies
        final int first = random.nextInt(keys.size());
        final int last = Math.min(keys.size() - 1, first + random.nextInt(MAX_RANGE_KEYS));
        final String start = keys.get(first);
        // Unique bounds tell the notifications of each range delete apart
        final String end = keys.get(last) + "~" + String.format(Locale.ROOT, "%016x", rangeCount);
        checker.registerRange(start, end);
        observe(
            Operation.DELETE_RANGE,
            start,
            () -> {
              client.deleteRange(start, end);
              return null;
            });
        checker.rangeDeleted(start, end);
        inference.deleteRange(start, end);
        rangeCount++;
        operationCount++;
      } else {
        // Distinct keys: the server keeps one notification per key in each batch it applies
        final List<Callable<Runnable>> writes = new ArrayList<>(options.batchSize());
        final Set<Integer> written = new HashSet<>();
        while (writes.size() < options.batchSize()
            && written.size() < keys.size()
            && System.nanoTime() < deadline) {
          if (!writes.isEmpty()) {
            pacer.beforeOperation();
          }
          int index = random.nextInt(keys.size());
          while (!written.add(index)) {
            index = random.nextInt(keys.size());
          }
          final String key = keys.get(index);
          if (random.nextDouble() < PUT_WEIGHT) {
            final byte[] value = valueGenerator.next(index, operationCount + writes.size());
            writes.add(
                () -> {
                  final Attempted<PutResult> put =
                      observe(Operation.PUT, key, () -> client.put(key, value));
                  return () -> {
                    checker.put(
                        key,
                        put.result().version().versionId(),
                        put.result().version().modificationsCount(),
                        put.attempts());
                    inference.put(key, value);
                  };
                });
          } else {
            writes.add(
                () -> {
                  final Attempted<Boolean> delete =
                      observe(Operation.DELETE, key, () -> client.delete(key));
                  return () -> {
                    checker.delete(key, delete.result(), delete.attempts());
                    inference.delete(key);
                  };
                });
          }
        }
        final List<Runnable> records = new ArrayList<>(writes.size());
        for (final Future<Runnable> future : executor.invokeAll(writes)) {
          try {
            records.add(future.get());
          } catch (final ExecutionException error) {
            if (error.getCause() instanceof final RuntimeException runtime) {
              throw runtime;
            }
            if (error.getCause() instanceof final Error fatal) {
              throw fatal;
            }
            throw new IllegalStateException("concurrent operation failed", error.getCause());
          }
        }
        // In the order the writes were generated: each key appears at most once
        records.forEach(Runnable::run);
        operationCount += writes.size();
      }

      final long now = System.nanoTime();
      if (now >= nextCheckpoint && now < deadline) {
        checkpoint(Checkpoint.PERIODIC);
        nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
      }
    }
  }

  /**
   * With no write in flight, waits for the notifications of every acknowledged write, then checks
   * that the state they describe is the one a range scan reads.
   */
  private void checkpoint(final Checkpoint checkpoint) throws InterruptedException {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder("notification.checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      final long deadline = addSaturated(started, deliveryTimeout.toNanos());
      List<String> missing = checker.check();
      while (!missing.isEmpty()) {
        if (System.nanoTime() >= deadline) {
          throw new IllegalStateException(
              "notifications still missing after their writes were acknowledged: missing="
                  + SummaryUtils.summarize(missing)
                  + ", waitedMs="
                  + elapsedMillis(started)
                  + ", sinceLastFailedAttempt="
                  + (faulted ? elapsedMillis(lastFaultNanos) + "ms" : "none"));
        }
        Thread.sleep(POLL_INTERVAL);
        missing = checker.check();
      }

      final String end = prefix + "~";
      final Map<String, GetResult> actual =
          Failsafe.with(retryPolicy, attemptTimeout)
              .get(
                  () -> {
                    final Map<String, GetResult> scanned = new HashMap<>();
                    try (final CloseableIterable<GetResult> results =
                        client.rangeScan(prefix, end)) {
                      for (final GetResult result : results) {
                        scanned.put(result.key(), result);
                      }
                    }
                    return scanned;
                  });
      final List<KeyValue> expected = inference.range(prefix, end);
      if (expected.size() != actual.size()) {
        throw CorrectnessViolationException.checkpointMismatch(
            SummaryUtils.summarize(expected.stream().map(KeyValue::key).sorted().toList()),
            SummaryUtils.summarize(actual.keySet().stream().sorted().toList()));
      }
      for (final KeyValue record : expected) {
        final GetResult result = actual.get(record.key());
        final Optional<Long> notified = checker.version(record.key());
        if (result == null
            || !Arrays.equals(record.value(), result.value())
            || !notified.equals(Optional.of(result.version().versionId()))) {
          throw CorrectnessViolationException.checkpointMismatch(
              record + " notified at version " + notified.map(String::valueOf).orElse("none"),
              result == null
                  ? "absent"
                  : new KeyValue(result.key(), result.value())
                      + " at version "
                      + result.version().versionId());
        }
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
          .log("notification checkpoint passed");
    } catch (final RuntimeException | InterruptedException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(checkpointLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  /** Runs one write with retries, remembering when an attempt last failed. */
  private <T> Attempted<T> observe(
      final Operation operation, final String key, final Supplier<T> action) {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicReference<T> result = new AtomicReference<>();
    telemetry.observe(
        operation,
        key,
        () ->
            result.set(
                Failsafe.with(retryPolicy, attemptTimeout)
                    .get(
                        () -> {
                          if (attempts.getAndIncrement() > 0) {
                            faulted = true;
                            lastFaultNanos = System.nanoTime();
                          }
                          return action.get();
                        })));
    return new Attempted<>(result.get(), attempts.get());
  }

  /** The result of a write and how many attempts it took: the failed ones have unknown outcomes. */
  private record Attempted<T>(T result, int attempts) {}
}

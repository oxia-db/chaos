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
import io.oxia.chaos.inference.VersionModel;
import io.oxia.chaos.inference.VersionModel.Kind;
import io.oxia.chaos.inference.VersionModel.Observed;
import io.oxia.chaos.inference.VersionModel.RecordVersion;
import io.oxia.chaos.observability.CaseTelemetry;
import io.oxia.chaos.observability.Metrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.BatchUtils;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks optimistic concurrency control: conditional writes on a record's version.
 *
 * <p>Each operation reads a key, then creates it if absent, or updates or deletes it on the version
 * it read. Concurrent operations favor a few hot keys, so they race on the same versions: at most
 * one write may replace a version, every version must carry the metadata of its write, and reads
 * never go back to an older version. Some writes deliberately name a version older than the one
 * read, which the server must always reject.
 */
public final class Versioning {

  private static final Logger LOGGER = LoggerFactory.getLogger(Versioning.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.versioning";
  private static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final int HOT_KEY_COUNT = 16;
  private static final double HOT_KEY_SHARE = 0.5;
  private static final double UPDATE_THRESHOLD = 0.60;
  private static final double DELETE_THRESHOLD = UPDATE_THRESHOLD + 0.20;
  private static final double STALE_PUT_THRESHOLD = DELETE_THRESHOLD + 0.12;
  static final double TOTAL_WEIGHT = STALE_PUT_THRESHOLD + 0.08;

  private final Options options;
  private final String runId;
  private final SyncOxiaClient client;
  private final VersionModel model;
  private final Metrics metrics;
  private final Tracer tracer;
  private final CaseTelemetry telemetry;
  private final RetryPolicy<Object> retryPolicy;
  private final Timeout<Object> attemptTimeout;
  private final String prefix;

  private long operationCount;
  private long checkpointCount;

  public Versioning(
      final Options options,
      final String runId,
      final SyncOxiaClient client,
      final OpenTelemetry openTelemetry,
      final VersionModel model,
      final Metrics metrics,
      final Duration retryTimeout) {
    this.options = options;
    this.runId = runId;
    this.client = client;
    this.model = model;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.telemetry = new CaseTelemetry(tracer, metrics, Options.VERSIONING);
    this.retryPolicy = RetryUtils.retryPolicy(retryTimeout);
    this.attemptTimeout = RetryUtils.attemptTimeout(OPERATION_ATTEMPT_TIMEOUT, retryTimeout);
    this.prefix = "/oxia-chaos/runs/" + runId + "/versioning/key-";
  }

  /** Runs the timed workload, a final checkpoint, and mandatory cleanup. */
  public void run() throws InterruptedException {
    final Span runSpan =
        tracer
            .spanBuilder("versioning.run")
            .setAttribute("test.case", Options.VERSIONING)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    Throwable failure = null;
    try (final Scope ignored = runSpan.makeCurrent()) {
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
        .log("versioning completed");
  }

  private void executeWorkload() throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());
    final Thread.Builder.OfVirtual threads = Thread.ofVirtual().name("versioning-operation-", 0);

    try (final ExecutorService executor =
        Context.taskWrapping(Executors.newThreadPerTaskExecutor(threads.factory()))) {
      while (System.nanoTime() < deadline) {
        final List<Callable<Void>> operations = new ArrayList<>(options.batchSize());
        while (operations.size() < options.batchSize() && System.nanoTime() < deadline) {
          pacer.beforeOperation();
          if (System.nanoTime() >= deadline) {
            break;
          }
          final String key =
              key(selectKeyIndex(random.nextDouble(), random.nextInt(), options.keyCount()));
          final double choice = random.nextDouble(TOTAL_WEIGHT);
          final String valuePrefix =
              String.format(
                  Locale.ROOT,
                  "value-%016x-%016x",
                  options.seed(),
                  operationCount + operations.size());
          operations.add(
              () -> {
                execute(key, choice, valuePrefix);
                return null;
              });
        }

        BatchUtils.executeConcurrentBatch(executor, operations);
        operationCount += operations.size();

        final long now = System.nanoTime();
        if (now >= nextCheckpoint && now < deadline) {
          checkpoint(Checkpoint.PERIODIC);
          nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
        }
      }
    }
  }

  /** Reads the key, then writes it conditionally on what the read returned. */
  private void execute(final String key, final double choice, final String valuePrefix) {
    final long readStarted = model.begin(key);
    final AtomicReference<Optional<Observed>> found = new AtomicReference<>();
    observe(
        Operation.GET,
        key,
        () ->
            found.set(
                Failsafe.with(retryPolicy, attemptTimeout)
                    .get(
                        () ->
                            Optional.ofNullable(client.get(key))
                                .map(result -> observed(key, result)))));
    final Optional<Observed> current = found.get();
    model.read(key, readStarted, current);

    if (current.isEmpty()) {
      write(Operation.CREATE, key, Kind.CREATE, Optional.empty(), valuePrefix);
      return;
    }
    final RecordVersion read = current.get().version();
    final Operation operation = selectOperation(choice, read.versionId());
    switch (operation) {
      case CONDITIONAL_PUT -> write(operation, key, Kind.UPDATE, Optional.of(read), valuePrefix);
      case CONDITIONAL_DELETE -> write(operation, key, Kind.DELETE, Optional.of(read), null);
      case STALE_PUT, STALE_DELETE -> writeStale(operation, key, read.versionId() - 1, valuePrefix);
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  /**
   * Writes conditionally, retrying failures. Every attempt writes its own value: an attempt that
   * failed may still have applied, and a read later tells which attempt a version belongs to.
   */
  private void write(
      final Operation operation,
      final String key,
      final Kind kind,
      final Optional<RecordVersion> predecessor,
      final String valuePrefix) {
    final AtomicInteger attempts = new AtomicInteger();
    observe(
        operation,
        key,
        () ->
            RetryUtils.run(
                retryPolicy,
                attemptTimeout,
                () -> {
                  final int attempt = attempts.getAndIncrement();
                  final String value =
                      valuePrefix == null ? null : valuePrefix + "-attempt-" + attempt;
                  final VersionModel.Attempt registered =
                      model.attempt(key, kind, predecessor, value);
                  try {
                    switch (kind) {
                      case CREATE ->
                          model.applied(
                              key,
                              registered,
                              Optional.of(
                                  version(
                                      key,
                                      client.put(
                                          key,
                                          value.getBytes(StandardCharsets.UTF_8),
                                          Set.of(PutOption.IfRecordDoesNotExist)))));
                      case UPDATE ->
                          model.applied(
                              key,
                              registered,
                              Optional.of(
                                  version(
                                      key,
                                      client.put(
                                          key,
                                          value.getBytes(StandardCharsets.UTF_8),
                                          Set.of(
                                              PutOption.IfVersionIdEquals(
                                                  predecessor.get().versionId()))))));
                      case DELETE -> {
                        if (!client.delete(
                            key,
                            Set.of(
                                DeleteOption.IfVersionIdEquals(predecessor.get().versionId())))) {
                          throw CorrectnessViolationException.operationMismatch(
                              operation, key, "deleted or an unexpected version", "not found");
                        }
                        model.applied(key, registered, Optional.empty());
                      }
                    }
                  } catch (final UnexpectedVersionIdException | KeyAlreadyExistsException error) {
                    model.rejected(key, registered, attempt == 0);
                  }
                }));
  }

  /**
   * Writes on a version older than the one read. Versions only grow, so the record can never be at
   * that version again, and the server must reject every attempt.
   */
  private void writeStale(
      final Operation operation,
      final String key,
      final long staleVersionId,
      final String valuePrefix) {
    final AtomicInteger attempts = new AtomicInteger();
    observe(
        operation,
        key,
        () ->
            RetryUtils.run(
                retryPolicy,
                attemptTimeout,
                () -> {
                  final String applied;
                  try {
                    if (operation == Operation.STALE_PUT) {
                      final String value = valuePrefix + "-stale-" + attempts.getAndIncrement();
                      applied =
                          version(
                                  key,
                                  client.put(
                                      key,
                                      value.getBytes(StandardCharsets.UTF_8),
                                      Set.of(PutOption.IfVersionIdEquals(staleVersionId))))
                              .toString();
                    } else {
                      applied =
                          client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(staleVersionId)))
                              ? "deleted"
                              : "not found";
                    }
                  } catch (final UnexpectedVersionIdException error) {
                    return;
                  } catch (final KeyAlreadyExistsException error) {
                    throw new IllegalStateException(
                        "put conditional on a version failed as a create: " + key, error);
                  }
                  throw CorrectnessViolationException.operationMismatch(
                      operation,
                      key,
                      "rejected for version " + staleVersionId,
                      "applied: " + applied);
                }));
  }

  private void checkpoint(final Checkpoint checkpoint) {
    final long started = System.nanoTime();
    telemetry.checkpoint(
        checkpoint,
        () -> {
          final Map<String, Observed> scanned = new HashMap<>();
          Failsafe.with(retryPolicy, attemptTimeout)
              .run(
                  () -> {
                    scanned.clear();
                    try (final CloseableIterable<GetResult> results =
                        client.rangeScan(prefix, prefix + "~")) {
                      for (final GetResult result : results) {
                        scanned.put(result.key(), observed(result.key(), result));
                      }
                    }
                  });
          model.checkpoint(scanned);
          checkpointCount++;
          LOGGER
              .atInfo()
              .addKeyValue("run_id", runId)
              .addKeyValue("kind", checkpoint.label())
              .addKeyValue("keys", scanned.size())
              .addKeyValue("operations", operationCount)
              .addKeyValue("duration_ms", elapsedMillis(started))
              .log("versioning checkpoint passed");
          return scanned.size();
        });
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    telemetry.observe(operation, key, action);
  }

  private void cleanup() {
    Failsafe.with(retryPolicy, attemptTimeout).run(() -> client.deleteRange(prefix, prefix + "~"));
    model.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("versioning cleanup completed");
  }

  private String key(final int index) {
    return prefix + String.format(Locale.ROOT, "%08d", index);
  }

  /** Picks one of the first hot keys for a share of the operations, so that writes race. */
  static int selectKeyIndex(final double hot, final int selected, final int keyCount) {
    final int hotKeys = Math.min(HOT_KEY_COUNT, keyCount);
    return Math.floorMod(selected, hot < HOT_KEY_SHARE ? hotKeys : keyCount);
  }

  /** Picks the write for a present record; a stale write needs a version below the one read. */
  static Operation selectOperation(final double selected, final long versionId) {
    if (selected < UPDATE_THRESHOLD) {
      return Operation.CONDITIONAL_PUT;
    }
    if (selected < DELETE_THRESHOLD) {
      return Operation.CONDITIONAL_DELETE;
    }
    if (versionId <= 0) {
      return Operation.CONDITIONAL_PUT;
    }
    return selected < STALE_PUT_THRESHOLD ? Operation.STALE_PUT : Operation.STALE_DELETE;
  }

  private static RecordVersion version(final String key, final PutResult result) {
    return recordVersion(key, result.version());
  }

  private static Observed observed(final String key, final GetResult result) {
    return new Observed(
        new String(result.value(), StandardCharsets.UTF_8), recordVersion(key, result.version()));
  }

  private static RecordVersion recordVersion(final String key, final Version version) {
    if (version.sessionId().isPresent() || version.clientIdentifier().isPresent()) {
      throw CorrectnessViolationException.versionHistory(
          key, "a persistent record has session metadata: " + version);
    }
    return new RecordVersion(
        version.versionId(),
        version.createdTimestamp(),
        version.modifiedTimestamp(),
        version.modificationsCount());
  }
}

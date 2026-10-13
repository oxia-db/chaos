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
import dev.failsafe.TimeoutExceededException;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.EphemeralModel;
import io.oxia.chaos.inference.EphemeralRecord;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.OxiaExceptionUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.PutOption;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that ephemeral records live exactly as long as the session that owns them.
 *
 * <p>Two owner child processes each hold a slice of ephemeral keys while an observer client in this
 * process verifies them. Owners are ended in turn, either gracefully or by SIGKILL, so Oxia must
 * delete their records on close or expire them on the session timeout, but never earlier.
 */
public final class Ephemeral {

  private static final Logger LOGGER = LoggerFactory.getLogger(Ephemeral.class);
  private static final String INSTRUMENTATION_SCOPE = "io.oxia.chaos.ephemeral";
  private static final List<String> SLOT_NAMES = List.of("a", "b");
  private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
  private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(5);
  private static final Duration OBSERVER_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DRAIN_POLL_INTERVAL = Duration.ofSeconds(1);
  private static final double PUT_THRESHOLD = 0.45;
  private static final double OWNER_GET_THRESHOLD = PUT_THRESHOLD + 0.20;
  private static final double GET_THRESHOLD = OWNER_GET_THRESHOLD + 0.25;
  static final double TOTAL_WEIGHT = GET_THRESHOLD + 0.10;

  /** Session and lifecycle timing; fixed per case rather than exposed as CLI options. */
  public record Settings(
      Duration sessionTimeout,
      Duration requestTimeout,
      Duration rotationInterval,
      Duration expirySlack) {

    public static final Settings DEFAULT =
        new Settings(
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            Duration.ofMinutes(2),
            Duration.ofSeconds(30));

    /** A crashed owner's records must survive at least this long after the kill. */
    public Duration earliestExpiry() {
      return sessionTimeout.dividedBy(2);
    }

    /**
     * An ended owner's records must be gone by this deadline. A failover can restart the session
     * timer on the new leader, so allow two timeouts plus slack.
     */
    public Duration expiryDeadline() {
      return sessionTimeout.multipliedBy(2).plus(expirySlack);
    }
  }

  private final Options options;
  private final String runId;
  private final String namespace;
  private final SyncOxiaClient observer;
  private final EphemeralModel model;
  private final RunnerMetrics metrics;
  private final Tracer tracer;
  private final Settings settings;
  private final RetryPolicy<Object> retryPolicy;
  private final RetryPolicy<Object> spawnPolicy;
  private final Timeout<Object> observerAttemptTimeout;
  private final ValueGenerator valueGenerator;
  private final int keysPerSlot;
  private final List<Slot> slots;

  private boolean finishing;
  private long operationCount;
  private long checkpointCount;
  private long rotationCount;

  public Ephemeral(
      final Options options,
      final String runId,
      final String namespace,
      final SyncOxiaClient observer,
      final OpenTelemetry openTelemetry,
      final EphemeralModel model,
      final RunnerMetrics metrics,
      final Duration retryTimeout,
      final Settings settings) {
    this.options = options;
    this.runId = runId;
    this.namespace = namespace;
    this.observer = observer;
    this.model = model;
    this.metrics = metrics;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.settings = settings;
    this.retryPolicy = createRetryPolicy(retryTimeout);
    this.spawnPolicy =
        RetryPolicy.builder()
            .handle(IllegalStateException.class)
            .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
            .withMaxAttempts(-1)
            .withMaxDuration(retryTimeout)
            .build();
    this.observerAttemptTimeout =
        BasicKv.createAttemptTimeout(
            OBSERVER_ATTEMPT_TIMEOUT.compareTo(retryTimeout) <= 0
                ? OBSERVER_ATTEMPT_TIMEOUT
                : retryTimeout);
    this.valueGenerator = new ValueGenerator(options.seed());
    this.keysPerSlot = Math.max(1, options.keyCount() / SLOT_NAMES.size());
    this.slots =
        SLOT_NAMES.stream()
            .map(
                (final String name) ->
                    new Slot(name, "/oxia-chaos/runs/" + runId + "/ephemeral/" + name + "/key-"))
            .toList();
  }

  /** Runs warmup, the timed workload, final owner expiry checks, and mandatory cleanup. */
  public void run() throws InterruptedException {
    final Span runSpan =
        tracer
            .spanBuilder("ephemeral.run")
            .setAttribute("test.case", Options.EPHEMERAL)
            .setAttribute("test.run_id", runId)
            .setAttribute("test.seed", options.seed())
            .setAttribute("test.key_count", options.keyCount())
            .startSpan();

    Throwable failure = null;
    try (final Scope ignored = runSpan.makeCurrent()) {
      for (final Slot slot : slots) {
        spawn(slot);
      }
      executeWorkload();
      awaitDrains();
      checkpoint(Checkpoint.FINAL);
      finishing = true;
      endOwner(slots.get(0), Operation.OWNER_CRASH);
      endOwner(slots.get(1), Operation.OWNER_CLOSE);
      awaitDrains();
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
        .addKeyValue("rotations", rotationCount)
        .log("ephemeral completed");
  }

  private void executeWorkload() throws InterruptedException {
    final long started = System.nanoTime();
    final long deadline = addSaturated(started, options.duration().toNanos());
    long nextCheckpoint = addSaturated(started, options.checkpointInterval().toNanos());
    long nextRotation = addSaturated(started, settings.rotationInterval().toNanos());
    final RatePacer pacer = new RatePacer(options.rate());
    final SplittableRandom random = new SplittableRandom(options.seed());

    while (System.nanoTime() < deadline) {
      pollDrains();
      if (System.nanoTime() >= nextRotation && slots.stream().allMatch(Slot::alive)) {
        final Slot slot = slots.get((int) (rotationCount % slots.size()));
        endOwner(slot, random.nextBoolean() ? Operation.OWNER_CRASH : Operation.OWNER_CLOSE);
        rotationCount++;
        nextRotation = addSaturated(System.nanoTime(), settings.rotationInterval().toNanos());
      }

      pacer.beforeOperation();
      if (System.nanoTime() >= deadline) {
        break;
      }
      final List<Slot> alive = slots.stream().filter(Slot::alive).toList();
      final Slot slot = alive.get(random.nextInt(alive.size()));
      final int index = random.nextInt(keysPerSlot);
      executeOperation(selectOperation(random.nextDouble(TOTAL_WEIGHT)), slot, index);
      operationCount++;

      final long now = System.nanoTime();
      if (now >= nextCheckpoint && now < deadline) {
        checkpoint(Checkpoint.PERIODIC);
        nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
      }
    }
  }

  private void executeOperation(final Operation operation, final Slot slot, final int index) {
    final String key = slot.key(index);
    switch (operation) {
      case EPHEMERAL_PUT -> {
        final byte[] value = valueGenerator.next(index, operationCount);
        observe(operation, key, () -> putOwned(slot, key, value));
      }
      case OWNER_GET ->
          observe(operation, key, () -> verify(slot, key, ownerCall(() -> slot.owner.get(key))));
      case GET -> observe(operation, key, () -> verify(slot, key, observerGet(key)));
      case DELETE ->
          observe(
              operation,
              key,
              () -> {
                ownerCall(() -> slot.owner.delete(key));
                model.delete(key);
              });
      default -> throw new IllegalStateException("unreachable operation: " + operation);
    }
  }

  private void putOwned(final Slot slot, final String key, final byte[] value) {
    final OwnedVersion version = ownerCall(() -> slot.owner.put(key, value));
    model.put(new EphemeralRecord(key, value, slot.owner.clientIdentifier(), owned(key, version)));
  }

  private long owned(final String key, final OwnedVersion version) {
    if (version.sessionId().isEmpty()) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.EPHEMERAL_PUT, key, "a session id", "none");
    }
    return version.sessionId().get();
  }

  /** Compares an observed record with the model, probing the owner when the record is missing. */
  private void verify(final Slot slot, final String key, final Optional<OwnedValue> actual) {
    final Optional<EphemeralRecord> expected = model.get(key);
    if (expected.isEmpty()) {
      if (actual.isPresent()) {
        throw CorrectnessViolationException.operationMismatch(
            Operation.GET, key, "absent", describe(actual.get()));
      }
      return;
    }
    if (actual.isEmpty()) {
      probeMissing(slot, expected.get());
      return;
    }
    if (!matches(expected.get(), actual.get())) {
      throw CorrectnessViolationException.operationMismatch(
          Operation.GET, key, expected.get().toString(), describe(actual.get()));
    }
  }

  /**
   * A record missing while its owner is alive is a violation only if the owner still holds the
   * session that wrote it. Rewriting the record reveals the owner's current session on that shard;
   * a new session means the old one legitimately expired, for example during a long fault.
   */
  private void probeMissing(final Slot slot, final EphemeralRecord expected) {
    final OwnedVersion version = ownerCall(() -> slot.owner.put(expected.key(), expected.value()));
    final long current = owned(expected.key(), version);
    if (current == expected.sessionId()) {
      throw CorrectnessViolationException.ephemeralLost(expected.key(), expected.sessionId());
    }
    metrics.recordSessionReplacement();
    LOGGER
        .atWarn()
        .addKeyValue("owner", expected.owner())
        .addKeyValue("key", expected.key())
        .addKeyValue("previous_session", expected.sessionId())
        .addKeyValue("current_session", current)
        .log("ephemeral owner session was replaced");
    model.put(expected.withSessionId(current));
  }

  private void checkpoint(final Checkpoint checkpoint) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder("ephemeral.checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      int keys = 0;
      for (final Slot slot : slots) {
        if (slot.alive()) {
          keys += verifySlot(slot);
        }
      }
      checkpointCount++;
      span.setAttribute("checkpoint.keys", keys).setStatus(StatusCode.OK);
      LOGGER
          .atInfo()
          .addKeyValue("run_id", runId)
          .addKeyValue("kind", checkpointLabel)
          .addKeyValue("keys", keys)
          .addKeyValue("operations", operationCount)
          .addKeyValue("duration_ms", elapsedMillis(started))
          .log("ephemeral checkpoint passed");
    } catch (final RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(checkpointLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  /** Verifies every record of a live owner's slice and returns how many are expected. */
  private int verifySlot(final Slot slot) {
    final Map<String, OwnedValue> present = scan(slot);
    final List<EphemeralRecord> expected = model.range(slot.prefix, slot.end());
    for (final EphemeralRecord record : expected) {
      verify(slot, record.key(), Optional.ofNullable(present.remove(record.key())));
    }
    if (!present.isEmpty()) {
      throw CorrectnessViolationException.checkpointMismatch(
          SummaryUtils.summarize(expected), "unexpected " + describe(present));
    }
    return expected.size();
  }

  /** Ends an owner and starts tracking how its records disappear. */
  private void endOwner(final Slot slot, final Operation mode) throws InterruptedException {
    verifySlot(slot);
    final Map<String, EphemeralRecord> expected = new LinkedHashMap<>();
    for (final EphemeralRecord record : model.removeRange(slot.prefix, slot.end())) {
      expected.put(record.key(), record);
    }
    final String owner = slot.owner.clientIdentifier();
    final long ended = System.nanoTime();
    if (mode == Operation.OWNER_CRASH) {
      slot.owner.crash();
    } else if (!slot.owner.closeGracefully(settings.expiryDeadline())) {
      LOGGER.atWarn().addKeyValue("owner", owner).log("ephemeral owner was killed during close");
    }
    slot.owner = null;
    slot.drain = new Drain(mode, owner, expected, ended);
    LOGGER
        .atInfo()
        .addKeyValue("owner", owner)
        .addKeyValue("mode", mode.label())
        .addKeyValue("keys", expected.size())
        .log("ephemeral owner ended");
    // Scan right away so a crashed owner's records are seen before any expiry is allowed.
    pollDrain(slot);
  }

  private void pollDrains() {
    for (final Slot slot : slots) {
      if (slot.drain != null && System.nanoTime() >= slot.nextPoll) {
        pollDrain(slot);
      }
    }
  }

  private void awaitDrains() throws InterruptedException {
    while (slots.stream().anyMatch((final Slot slot) -> slot.drain != null)) {
      pollDrains();
      Thread.sleep(DRAIN_POLL_INTERVAL);
    }
  }

  /** Checks one draining slice: records may only vanish, and only inside the allowed window. */
  private void pollDrain(final Slot slot) {
    final Drain drain = slot.drain;
    final long scanStarted = System.nanoTime();
    final Map<String, OwnedValue> present = scan(slot);
    final long scanEnded = System.nanoTime();

    for (final Map.Entry<String, OwnedValue> entry : present.entrySet()) {
      final EphemeralRecord expected = drain.records.get(entry.getKey());
      if (expected == null || !matches(expected, entry.getValue())) {
        throw CorrectnessViolationException.operationMismatch(
            drain.mode,
            entry.getKey(),
            expected == null ? "absent" : expected.toString(),
            describe(entry.getValue()));
      }
    }
    final long elapsedMillis = elapsedMillis(drain.endedNanos);
    switch (judgeDrain(
        settings,
        drain.mode,
        drain.records.size(),
        present.size(),
        Duration.ofNanos(scanStarted - drain.endedNanos),
        Duration.ofNanos(scanEnded - drain.endedNanos))) {
      case EARLY_EXPIRY ->
          throw CorrectnessViolationException.earlyExpiry(
              drain.owner, drain.records.size(), present.size(), elapsedMillis);
      case LEAKED -> {
        metrics.recordOperation(drain.mode.label(), "error", elapsedSeconds(drain.endedNanos));
        throw CorrectnessViolationException.ephemeralLeaked(
            drain.owner, describe(present), elapsedMillis);
      }
      case DRAINED -> {
        metrics.recordOperation(drain.mode.label(), "success", elapsedSeconds(drain.endedNanos));
        LOGGER
            .atInfo()
            .addKeyValue("owner", drain.owner)
            .addKeyValue("mode", drain.mode.label())
            .addKeyValue("keys", drain.records.size())
            .addKeyValue("elapsed_ms", elapsedMillis)
            .log("ephemeral owner records removed");
        slot.drain = null;
        if (!finishing) {
          spawn(slot);
        }
        return;
      }
      case WAITING -> {}
    }
    slot.nextPoll = addSaturated(System.nanoTime(), DRAIN_POLL_INTERVAL.toNanos());
  }

  /** Starts a new owner generation for the slice and fills the slice with ephemeral records. */
  private void spawn(final Slot slot) {
    slot.generation++;
    final String identifier =
        String.format(
            Locale.ROOT, "oxia-chaos/%s/ephemeral/%s/%d", runId, slot.name, slot.generation);
    final long started = System.nanoTime();
    slot.owner =
        Failsafe.with(spawnPolicy)
            .get(
                () ->
                    Owner.start(
                        options.serviceAddress(),
                        namespace,
                        identifier,
                        settings.sessionTimeout(),
                        settings.requestTimeout()));
    for (int index = 0; index < keysPerSlot; index++) {
      putOwned(slot, slot.key(index), valueGenerator.warmup(index));
    }
    LOGGER
        .atInfo()
        .addKeyValue("owner", identifier)
        .addKeyValue("pid", slot.owner.pid())
        .addKeyValue("keys", keysPerSlot)
        .addKeyValue("duration_ms", elapsedMillis(started))
        .log("ephemeral owner started");
  }

  private Map<String, OwnedValue> scan(final Slot slot) {
    final Map<String, OwnedValue> present = new LinkedHashMap<>();
    observerCall(
        () -> {
          present.clear();
          try (final CloseableIterable<GetResult> results =
              observer.rangeScan(slot.prefix, slot.end())) {
            for (final GetResult result : results) {
              present.put(result.key(), owned(result));
            }
          }
        });
    return present;
  }

  private Optional<OwnedValue> observerGet(final String key) {
    return Failsafe.with(retryPolicy, observerAttemptTimeout)
        .get(() -> Optional.ofNullable(observer.get(key)).map(Ephemeral::owned));
  }

  private void observerCall(final Runnable action) {
    Failsafe.with(retryPolicy, observerAttemptTimeout).run(action::run);
  }

  // Owner requests are not interrupted: the child bounds each attempt with its request timeout,
  // and abandoning a pending reply would desynchronize the request/response stream.
  private <T> T ownerCall(final Supplier<T> action) {
    return Failsafe.with(retryPolicy).get(action::get);
  }

  private void observe(final Operation operation, final String key, final Runnable action) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String operationLabel = operation.label();
    final Span span =
        tracer
            .spanBuilder("ephemeral." + operationLabel)
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

  private void cleanup() {
    for (final Slot slot : slots) {
      if (slot.owner != null) {
        slot.owner.close();
        slot.owner = null;
      }
    }
    for (final Slot slot : slots) {
      observerCall(() -> observer.deleteRange(slot.prefix, slot.end()));
    }
    model.clear();
    LOGGER.atInfo().addKeyValue("run_id", runId).log("ephemeral cleanup completed");
  }

  static RetryPolicy<Object> createRetryPolicy(final Duration retryTimeout) {
    return RetryPolicy.builder()
        .handleIf(
            (final Throwable error) ->
                error instanceof TimeoutExceededException
                    || (error instanceof final OwnerOperationException owner && owner.retryable())
                    || OxiaExceptionUtils.isRetryable(error))
        .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
        .withMaxAttempts(-1)
        .withMaxDuration(retryTimeout)
        .onRetry(
            (final var event) ->
                LOGGER
                    .atWarn()
                    .addKeyValue("attempt", event.getAttemptCount())
                    .addKeyValue("error", String.valueOf(event.getLastException()))
                    .log("Retrying current ephemeral operation"))
        .build();
  }

  /**
   * Decides what one scan of an ended owner's slice means. Bounds are checked conservatively: a
   * scan proves records were already gone when it ended and were still present when it started.
   */
  static DrainVerdict judgeDrain(
      final Settings settings,
      final Operation mode,
      final int expected,
      final int present,
      final Duration sinceEndAtScanStart,
      final Duration sinceEndAtScanEnd) {
    if (mode == Operation.OWNER_CRASH
        && present < expected
        && sinceEndAtScanEnd.compareTo(settings.earliestExpiry()) < 0) {
      return DrainVerdict.EARLY_EXPIRY;
    }
    if (present == 0) {
      return DrainVerdict.DRAINED;
    }
    if (sinceEndAtScanStart.compareTo(settings.expiryDeadline()) > 0) {
      return DrainVerdict.LEAKED;
    }
    return DrainVerdict.WAITING;
  }

  static Operation selectOperation(final double selected) {
    if (selected < PUT_THRESHOLD) {
      return Operation.EPHEMERAL_PUT;
    }
    if (selected < OWNER_GET_THRESHOLD) {
      return Operation.OWNER_GET;
    }
    if (selected < GET_THRESHOLD) {
      return Operation.GET;
    }
    return Operation.DELETE;
  }

  static boolean matches(final EphemeralRecord expected, final OwnedValue actual) {
    return Arrays.equals(expected.value(), actual.value())
        && actual.version().sessionId().equals(Optional.of(expected.sessionId()))
        && actual.version().clientIdentifier().equals(Optional.of(expected.owner()));
  }

  private static OwnedValue owned(final GetResult result) {
    return new OwnedValue(
        result.value(),
        new OwnedVersion(result.version().sessionId(), result.version().clientIdentifier()));
  }

  private static String describe(final OwnedValue value) {
    return "value="
        + Base64.getEncoder().encodeToString(value.value())
        + ", sessionId="
        + value.version().sessionId().map(String::valueOf).orElse("none")
        + ", owner="
        + value.version().clientIdentifier().orElse("none");
  }

  private static String describe(final Map<String, OwnedValue> records) {
    return SummaryUtils.summarize(
        records.entrySet().stream()
            .map(
                (final Map.Entry<String, OwnedValue> entry) ->
                    entry.getKey() + "{" + describe(entry.getValue()) + "}")
            .toList());
  }

  /**
   * An owner child process and the parent-side handle that drives it.
   *
   * <p>{@link #start} launches {@link #main} on the current JVM and classpath. The parent drives it
   * over a line protocol: each request on stdin receives exactly one response on stdout, and
   * closing stdin closes the client gracefully. Killing the process instead stops session
   * heartbeats so Oxia must expire the session on its own. The child logs to stderr.
   *
   * <ul>
   *   <li>{@code PUT <key> <base64-value>} replies {@code OK <session-id> <client-identifier>}
   *   <li>{@code DELETE <key>} replies {@code OK <true|false>}
   *   <li>{@code GET <key>} replies {@code OK NONE} or {@code OK <base64-value> <session-id>
   *       <client-identifier>}, using {@code -} for missing metadata
   *   <li>failures reply {@code ERR <retryable> <base64-message>}
   * </ul>
   */
  public static final class Owner implements AutoCloseable {

    private static final String READY = "READY";
    private static final Set<PutOption> EPHEMERAL = Set.of(PutOption.AsEphemeralRecord);
    private static final List<String> JVM_OPTIONS = List.of("-Xmx128m", "-XX:+UseSerialGC");

    private final String clientIdentifier;
    private final Process process;
    private final PrintStream requests;
    private final BufferedReader responses;

    private Owner(final String clientIdentifier, final Process process) {
      this.clientIdentifier = clientIdentifier;
      this.process = process;
      this.requests = new PrintStream(process.getOutputStream(), true, StandardCharsets.UTF_8);
      this.responses =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    }

    /** Starts an owner on the current JVM and classpath and waits until its client is connected. */
    static Owner start(
        final String serviceAddress,
        final String namespace,
        final String clientIdentifier,
        final Duration sessionTimeout,
        final Duration requestTimeout) {
      final List<String> command =
          new ArrayList<>(
              List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString()));
      command.addAll(JVM_OPTIONS);
      command.addAll(
          List.of(
              "-cp",
              System.getProperty("java.class.path"),
              Owner.class.getName(),
              serviceAddress,
              namespace,
              clientIdentifier,
              Long.toString(sessionTimeout.toMillis()),
              Long.toString(requestTimeout.toMillis())));
      final Process process;
      try {
        process =
            new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
      } catch (final IOException error) {
        throw new UncheckedIOException(
            "unable to start ephemeral owner " + clientIdentifier, error);
      }
      final Owner owner = new Owner(clientIdentifier, process);
      final String ready = owner.readLine();
      if (!READY.equals(ready)) {
        owner.close();
        throw new IllegalStateException(
            "ephemeral owner " + clientIdentifier + " failed to start: " + ready);
      }
      return owner;
    }

    public String clientIdentifier() {
      return clientIdentifier;
    }

    public long pid() {
      return process.pid();
    }

    /** Writes an ephemeral record and returns the session that owns it. */
    public synchronized OwnedVersion put(final String key, final byte[] value) {
      final String[] reply =
          request("PUT " + key + " " + Base64.getEncoder().encodeToString(value));
      return new OwnedVersion(sessionId(reply[1]), identifier(reply[2]));
    }

    public synchronized boolean delete(final String key) {
      return Boolean.parseBoolean(request("DELETE " + key)[1]);
    }

    public synchronized Optional<OwnedValue> get(final String key) {
      final String[] reply = request("GET " + key);
      if ("NONE".equals(reply[1])) {
        return Optional.empty();
      }
      return Optional.of(
          new OwnedValue(
              Base64.getDecoder().decode(reply[1]),
              new OwnedVersion(sessionId(reply[2]), identifier(reply[3]))));
    }

    /**
     * Closes stdin so the owner closes its client, which deletes its sessions immediately.
     *
     * @return false when the owner had to be killed because it did not exit within the timeout
     */
    public synchronized boolean closeGracefully(final Duration timeout)
        throws InterruptedException {
      requests.close();
      if (process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        return true;
      }
      process.destroyForcibly().waitFor();
      return false;
    }

    /** Kills the owner without closing its client, leaving Oxia to expire its sessions. */
    public synchronized void crash() throws InterruptedException {
      process.destroyForcibly().waitFor();
    }

    public boolean isAlive() {
      return process.isAlive();
    }

    @Override
    public void close() {
      process.destroyForcibly();
    }

    private String[] request(final String request) {
      if (!process.isAlive()) {
        throw new IllegalStateException("ephemeral owner " + clientIdentifier + " is not running");
      }
      requests.println(request);
      final String reply = readLine();
      if (reply == null) {
        throw new IllegalStateException(
            "ephemeral owner " + clientIdentifier + " exited during: " + request);
      }
      final String[] parts = reply.split(" ");
      if ("ERR".equals(parts[0])) {
        throw new OwnerOperationException(
            clientIdentifier,
            Boolean.parseBoolean(parts[1]),
            new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8));
      }
      return parts;
    }

    private String readLine() {
      try {
        return responses.readLine();
      } catch (final IOException error) {
        throw new UncheckedIOException(
            "unable to read from ephemeral owner " + clientIdentifier, error);
      }
    }

    private static Optional<Long> sessionId(final String value) {
      return "-".equals(value) ? Optional.empty() : Optional.of(Long.parseLong(value));
    }

    private static Optional<String> identifier(final String value) {
      return "-".equals(value) ? Optional.empty() : Optional.of(value);
    }

    /**
     * Child entry point. Arguments: service address, namespace, client identifier, session and
     * request timeouts in milliseconds.
     */
    public static void main(final String[] args) throws Exception {
      if (args.length != 5) {
        throw new IllegalArgumentException(
            "usage: <service-address> <namespace> <client-identifier> <session-timeout-ms>"
                + " <request-timeout-ms>");
      }
      final PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
      final BufferedReader in =
          new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      try (final SyncOxiaClient client =
          OxiaClientBuilder.create(args[0])
              .namespace(args[1])
              .clientIdentifier(args[2])
              .sessionTimeout(Duration.ofMillis(Long.parseLong(args[3])))
              .requestTimeout(Duration.ofMillis(Long.parseLong(args[4])))
              .syncClient()) {
        out.println(READY);
        String line;
        while ((line = in.readLine()) != null) {
          out.println(handle(client, line));
        }
      }
    }

    static String handle(final SyncOxiaClient client, final String line) {
      final String[] parts = line.split(" ");
      try {
        return switch (parts[0]) {
          case "PUT" -> {
            final PutResult result =
                client.put(parts[1], Base64.getDecoder().decode(parts[2]), EPHEMERAL);
            yield "OK " + metadata(result.version());
          }
          case "DELETE" -> "OK " + client.delete(parts[1]);
          case "GET" -> {
            final GetResult result = client.get(parts[1]);
            yield result == null
                ? "OK NONE"
                : "OK "
                    + Base64.getEncoder().encodeToString(result.value())
                    + " "
                    + metadata(result.version());
          }
          default -> throw new IllegalArgumentException("unknown request: " + parts[0]);
        };
      } catch (final Exception error) {
        return "ERR "
            + OxiaExceptionUtils.isRetryable(error)
            + " "
            + Base64.getEncoder()
                .encodeToString(String.valueOf(error).getBytes(StandardCharsets.UTF_8));
      }
    }

    private static String metadata(final Version version) {
      return version.sessionId().map(String::valueOf).orElse("-")
          + " "
          + version.clientIdentifier().orElse("-");
    }
  }

  /** Session metadata reported by Oxia for an ephemeral record. */
  record OwnedVersion(Optional<Long> sessionId, Optional<String> clientIdentifier) {}

  /** A value read by an owner together with its session metadata. */
  record OwnedValue(byte[] value, OwnedVersion version) {}

  /** An operation the owner attempted failed inside the child process. */
  static final class OwnerOperationException extends RuntimeException {

    private final boolean retryable;

    OwnerOperationException(
        final String clientIdentifier, final boolean retryable, final String message) {
      super("ephemeral owner " + clientIdentifier + " failed: " + message);
      this.retryable = retryable;
    }

    public boolean retryable() {
      return retryable;
    }
  }

  enum DrainVerdict {
    WAITING,
    DRAINED,
    EARLY_EXPIRY,
    LEAKED
  }

  private static final class Slot {

    private final String name;
    private final String prefix;
    private int generation;
    private Owner owner;
    private Drain drain;
    private long nextPoll;

    private Slot(final String name, final String prefix) {
      this.name = name;
      this.prefix = prefix;
    }

    private String key(final int index) {
      return prefix + String.format(Locale.ROOT, "%08d", index);
    }

    private String end() {
      return prefix + "~";
    }

    private boolean alive() {
      return owner != null;
    }
  }

  private record Drain(
      Operation mode, String owner, Map<String, EphemeralRecord> records, long endedNanos) {

    private Drain {
      Objects.requireNonNull(mode, "mode");
      records = Map.copyOf(records);
    }
  }
}

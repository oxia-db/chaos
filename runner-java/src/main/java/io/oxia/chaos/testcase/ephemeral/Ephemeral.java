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
package io.oxia.chaos.testcase.ephemeral;

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
import io.opentelemetry.context.Scope;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.EphemeralModel;
import io.oxia.chaos.inference.EphemeralRecord;
import io.oxia.chaos.observability.CaseTelemetry;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnedValue;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnedVersion;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnerOperationException;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnerUnavailableException;
import io.oxia.chaos.util.ExceptionUtils;
import io.oxia.chaos.util.RatePacer;
import io.oxia.chaos.util.RetryUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.chaos.util.ValueGenerator;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
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
import java.util.SplittableRandom;
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

    /** How long an owner may take to connect before it is killed and started again. */
    public Duration startTimeout() {
      return requestTimeout.multipliedBy(6);
    }

    /** How long an owner may take to answer one request before it is treated as lost. */
    public Duration replyTimeout() {
      return requestTimeout.multipliedBy(3);
    }

    /** How long a graceful close may take before the owner is killed instead. */
    public Duration closeTimeout() {
      return requestTimeout;
    }

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
  private final CaseTelemetry telemetry;
  private final Settings settings;
  private final RetryPolicy<Object> retryPolicy;
  private final RetryPolicy<Object> spawnPolicy;
  private final Timeout<Object> observerAttemptTimeout;
  private final ValueGenerator valueGenerator;
  private final List<Slot> slots;

  private boolean finishing;
  private boolean faulted;
  private long lastFaultNanos;
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
    this.telemetry = new CaseTelemetry(tracer, metrics, Options.EPHEMERAL);
    this.settings = settings;
    this.retryPolicy =
        RetryUtils.retryPolicy(
            retryTimeout,
            (final Throwable error) ->
                error instanceof final OwnerOperationException owner && owner.retryable());
    this.spawnPolicy =
        RetryPolicy.builder()
            .handle(IllegalStateException.class)
            .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
            .withMaxAttempts(-1)
            .withMaxDuration(retryTimeout)
            .build();
    this.observerAttemptTimeout = RetryUtils.attemptTimeout(OBSERVER_ATTEMPT_TIMEOUT, retryTimeout);
    this.valueGenerator = new ValueGenerator(options.seed());
    final List<Slot> created = new ArrayList<>(SLOT_NAMES.size());
    for (int index = 0; index < SLOT_NAMES.size(); index++) {
      final String name = SLOT_NAMES.get(index);
      created.add(
          new Slot(
              name,
              "/oxia-chaos/runs/" + runId + "/ephemeral/" + name + "/key-",
              slotKeyCount(options.keyCount(), SLOT_NAMES.size(), index)));
    }
    this.slots = List.copyOf(created);
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
      while (true) {
        try {
          awaitDrains();
          checkpoint(Checkpoint.FINAL);
          break;
        } catch (final OwnerUnavailableException error) {
          ownerLost(error);
        }
      }
      finishing = true;
      endOwnerAtFinish(slots.get(0), Operation.OWNER_CRASH);
      endOwnerAtFinish(slots.get(1), Operation.OWNER_CLOSE);
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
      try {
        pollDrains();
        if (System.nanoTime() >= nextRotation && slots.stream().allMatch(Slot::alive)) {
          final Slot slot = slots.get((int) (rotationCount % slots.size()));
          endOwner(slot, random.nextBoolean() ? Operation.OWNER_CRASH : Operation.OWNER_CLOSE);
          rotationCount++;
          nextRotation = addSaturated(System.nanoTime(), settings.rotationInterval().toNanos());
        }

        final List<Slot> alive = slots.stream().filter(Slot::alive).toList();
        if (alive.isEmpty()) {
          Thread.sleep(DRAIN_POLL_INTERVAL);
          continue;
        }
        pacer.beforeOperation();
        if (System.nanoTime() >= deadline) {
          break;
        }
        final Slot slot = alive.get(random.nextInt(alive.size()));
        final int index = random.nextInt(slot.keyCount);
        executeOperation(selectOperation(random.nextDouble(TOTAL_WEIGHT)), slot, index);
        operationCount++;

        final long now = System.nanoTime();
        if (now >= nextCheckpoint && now < deadline) {
          checkpoint(Checkpoint.PERIODIC);
          nextCheckpoint = addSaturated(System.nanoTime(), options.checkpointInterval().toNanos());
        }
      } catch (final OwnerUnavailableException error) {
        ownerLost(error);
      }
    }
  }

  private void executeOperation(final Operation operation, final Slot slot, final int index) {
    final String key = slot.key(index);
    switch (operation) {
      case EPHEMERAL_PUT -> {
        final byte[] value = valueGenerator.next(index, operationCount);
        telemetry.observe(operation, key, () -> putOwned(slot, key, value));
      }
      case OWNER_GET ->
          telemetry.observe(
              operation, key, () -> verify(slot, key, ownerCall(() -> slot.owner.get(key))));
      case GET -> telemetry.observe(operation, key, () -> verify(slot, key, observerGet(key)));
      case DELETE ->
          telemetry.observe(
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
    telemetry.checkpoint(
        checkpoint,
        () -> {
          int keys = 0;
          for (final Slot slot : slots) {
            if (slot.alive()) {
              keys += verifySlot(slot);
            }
          }
          checkpointCount++;
          LOGGER
              .atInfo()
              .addKeyValue("run_id", runId)
              .addKeyValue("kind", checkpoint.label())
              .addKeyValue("keys", keys)
              .addKeyValue("operations", operationCount)
              .addKeyValue("duration_ms", elapsedMillis(started))
              .log("ephemeral checkpoint passed");
          return keys;
        });
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
    final long ended;
    if (mode == Operation.OWNER_CRASH) {
      ended = System.nanoTime();
      slot.owner.crash();
    } else {
      if (!slot.owner.closeGracefully(settings.closeTimeout())) {
        LOGGER.atWarn().addKeyValue("owner", owner).log("ephemeral owner was killed during close");
      }
      // Measure from when the owner was gone, so a slow close cannot eat the expiry window.
      ended = System.nanoTime();
    }
    slot.owner = null;
    final boolean checkEarlyExpiry =
        checksEarlyExpiry(
            mode,
            faulted ? Optional.of(Duration.ofNanos(ended - lastFaultNanos)) : Optional.empty(),
            settings.sessionTimeout());
    slot.drain = new Drain(mode, owner, expected, ended, checkEarlyExpiry);
    LOGGER
        .atInfo()
        .addKeyValue("owner", owner)
        .addKeyValue("mode", mode.label())
        .addKeyValue("keys", expected.size())
        .log("ephemeral owner ended");
    // Scan right away so a crashed owner's records are seen before any expiry is allowed.
    pollDrain(slot);
  }

  private void endOwnerAtFinish(final Slot slot, final Operation mode) throws InterruptedException {
    if (slot.owner == null) {
      return;
    }
    try {
      endOwner(slot, mode);
    } catch (final OwnerUnavailableException error) {
      ownerLost(error);
    }
  }

  /**
   * Handles an owner process that died or stopped responding. Its last request may or may not have
   * been applied, so its slice drains leniently: remaining records only need to belong to it.
   */
  private void ownerLost(final OwnerUnavailableException error) {
    final Slot slot =
        slots.stream()
            .filter(
                (final Slot candidate) ->
                    candidate.owner != null
                        && candidate.owner.clientIdentifier().equals(error.clientIdentifier()))
            .findFirst()
            .orElseThrow(() -> error);
    noteFault();
    final Map<String, EphemeralRecord> expected = new LinkedHashMap<>();
    for (final EphemeralRecord record : model.removeRange(slot.prefix, slot.end())) {
      expected.put(record.key(), record);
    }
    slot.owner.close();
    slot.owner = null;
    slot.drain =
        new Drain(
            Operation.OWNER_LOST, error.clientIdentifier(), expected, System.nanoTime(), false);
    slot.nextPoll = System.nanoTime();
    LOGGER
        .atWarn()
        .addKeyValue("owner", error.clientIdentifier())
        .addKeyValue("keys", expected.size())
        .setCause(error)
        .log("ephemeral owner lost");
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
      if (drain.mode == Operation.OWNER_LOST) {
        if (!entry.getValue().version().clientIdentifier().equals(Optional.of(drain.owner))) {
          throw CorrectnessViolationException.operationMismatch(
              drain.mode, entry.getKey(), "owner=" + drain.owner, describe(entry.getValue()));
        }
      } else if (expected == null || !matches(expected, entry.getValue())) {
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
        drain.checkEarlyExpiry,
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
                        settings.requestTimeout(),
                        settings.startTimeout(),
                        settings.replyTimeout()));
    for (int index = 0; index < slot.keyCount; index++) {
      putOwned(slot, slot.key(index), valueGenerator.warmup(index));
    }
    LOGGER
        .atInfo()
        .addKeyValue("owner", identifier)
        .addKeyValue("pid", slot.owner.pid())
        .addKeyValue("keys", slot.keyCount)
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
        .get(() -> tracked(() -> Optional.ofNullable(observer.get(key)).map(Ephemeral::owned)));
  }

  private void observerCall(final Runnable action) {
    Failsafe.with(retryPolicy, observerAttemptTimeout)
        .run(
            () ->
                tracked(
                    () -> {
                      action.run();
                      return null;
                    }));
  }

  // Owner requests are not interrupted by Failsafe: the owner bounds each reply itself and is
  // treated as lost when it does not answer, since a late reply would desynchronize the stream.
  private <T> T ownerCall(final Supplier<T> action) {
    return Failsafe.with(retryPolicy).get(() -> tracked(action));
  }

  /** Runs one attempt and remembers when any attempt last failed. */
  private <T> T tracked(final Supplier<T> attempt) {
    try {
      return attempt.get();
    } catch (final RuntimeException error) {
      noteFault();
      throw error;
    }
  }

  private void noteFault() {
    faulted = true;
    lastFaultNanos = System.nanoTime();
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

  /**
   * Decides what one scan of an ended owner's slice means. Bounds are checked conservatively: a
   * scan proves records were already gone when it ended and were still present when it started.
   */
  static DrainVerdict judgeDrain(
      final Settings settings,
      final boolean checkEarlyExpiry,
      final int expected,
      final int present,
      final Duration sinceEndAtScanStart,
      final Duration sinceEndAtScanEnd) {
    if (checkEarlyExpiry
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

  /**
   * The server times a session out from its last heartbeat, not from the kill. A fault shortly
   * before the kill may have stopped heartbeats early, so the lower bound only applies to a crash
   * that follows a full session timeout without failed requests.
   */
  static boolean checksEarlyExpiry(
      final Operation mode,
      final Optional<Duration> sinceLastFault,
      final Duration sessionTimeout) {
    return mode == Operation.OWNER_CRASH
        && sinceLastFault
            .map((final Duration since) -> since.compareTo(sessionTimeout) > 0)
            .orElse(true);
  }

  /** Splits the key count across slots, giving the remainder to the first slots. */
  static int slotKeyCount(final int keyCount, final int slots, final int slot) {
    return keyCount / slots + (slot < keyCount % slots ? 1 : 0);
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

  enum DrainVerdict {
    WAITING,
    DRAINED,
    EARLY_EXPIRY,
    LEAKED
  }

  private static final class Slot {

    private final String name;
    private final String prefix;
    private final int keyCount;
    private int generation;
    private Owner owner;
    private Drain drain;
    private long nextPoll;

    private Slot(final String name, final String prefix, final int keyCount) {
      this.name = name;
      this.prefix = prefix;
      this.keyCount = keyCount;
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
      Operation mode,
      String owner,
      Map<String, EphemeralRecord> records,
      long endedNanos,
      boolean checkEarlyExpiry) {

    private Drain {
      Objects.requireNonNull(mode, "mode");
      records = Map.copyOf(records);
    }
  }
}

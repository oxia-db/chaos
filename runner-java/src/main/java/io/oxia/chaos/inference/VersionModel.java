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
package io.oxia.chaos.inference;

import io.oxia.chaos.error.CorrectnessViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Reference history of records that only conditional writes change.
 *
 * <p>Every update and delete names the version it read, so the writes that apply to a key form one
 * chain, and each version is replaced at most once. Every attempt of a create or update writes its
 * own value, which attributes each version a read observes to the attempt that produced it. An
 * attempt with an unknown outcome stays pending until a read shows its value. At a checkpoint, a
 * pending create or update that no read showed never applied: every later write of the key read the
 * key first, and would have shown it.
 */
public final class VersionModel {

  /** The kinds of conditional writes. */
  public enum Kind {
    CREATE,
    UPDATE,
    DELETE
  }

  /** The version metadata the case checks; ephemeral fields are checked by the case. */
  public record RecordVersion(
      long versionId, long createdTimestamp, long modifiedTimestamp, long modificationsCount) {}

  /** A record as a read returned it. */
  public record Observed(String value, RecordVersion version) {

    public Observed {
      Objects.requireNonNull(value, "value");
      Objects.requireNonNull(version, "version");
    }
  }

  /** One attempt of a conditional write. */
  public static final class Attempt {

    private final Kind kind;
    private final String value;
    private final RecordVersion predecessor;
    private final long startTick;
    private Status status = Status.PENDING;
    private RecordVersion version;
    private boolean checkedRejection;

    private Attempt(
        final Kind kind,
        final String value,
        final RecordVersion predecessor,
        final long startTick) {
      this.kind = kind;
      this.value = value;
      this.predecessor = predecessor;
      this.startTick = startTick;
    }

    private String describe() {
      return kind.name().toLowerCase(Locale.ROOT)
          + "{value="
          + value
          + ", predecessor="
          + (predecessor == null ? "none" : predecessor.versionId())
          + ", status="
          + status
          + (version == null ? "" : ", version=" + version)
          + "}";
    }
  }

  private enum Status {
    PENDING,
    APPLIED,
    REJECTED
  }

  private static final class History {

    private final Map<String, Attempt> puts = new HashMap<>();
    private final List<Attempt> deletes = new ArrayList<>();
    private final Map<Long, Attempt> producers = new HashMap<>();
    private final Map<Long, Attempt> consumers = new HashMap<>();
    // {completion tick, highest version completed so far}, in tick order
    private final List<long[]> completions = new ArrayList<>();
  }

  private final Map<String, History> histories = new HashMap<>();
  private long clock;
  private int present;

  /** Returns a tick that orders an operation after every operation already completed. */
  public synchronized long begin() {
    return ++clock;
  }

  /**
   * Registers an attempt before it is sent. A create has no predecessor, and a delete no value.
   * Each create or update attempt must write a value no other attempt of the key wrote.
   */
  public synchronized Attempt attempt(
      final String key,
      final Kind kind,
      final Optional<RecordVersion> predecessor,
      final String value) {
    if ((kind == Kind.CREATE) != predecessor.isEmpty()
        || (kind == Kind.DELETE) != (value == null)) {
      throw new IllegalArgumentException("inconsistent " + kind + " attempt for key " + key);
    }
    final Attempt attempt = new Attempt(kind, value, predecessor.orElse(null), ++clock);
    final History history = histories.computeIfAbsent(key, (final String ignored) -> new History());
    if (kind == Kind.DELETE) {
      history.deletes.add(attempt);
    } else if (history.puts.putIfAbsent(value, attempt) != null) {
      throw new IllegalArgumentException("value written twice to key " + key + ": " + value);
    }
    return attempt;
  }

  /** Records that the server applied an attempt; a create or update returns its new version. */
  public synchronized void applied(
      final String key, final Attempt attempt, final Optional<RecordVersion> version) {
    final History history = histories.get(key);
    if ((attempt.kind == Kind.DELETE) != version.isEmpty()) {
      throw new IllegalArgumentException("inconsistent " + attempt.kind + " result for key " + key);
    }
    adopt(key, history, attempt, version.orElse(null));
    version.ifPresent((final RecordVersion applied) -> complete(history, applied.versionId()));
  }

  /**
   * Records that the server rejected an attempt for a version mismatch. Only the rejection of a
   * first attempt needs an explanation: a retry may conflict with its own earlier attempt.
   */
  public synchronized void rejected(final Attempt attempt, final boolean firstAttempt) {
    if (attempt.status != Status.PENDING) {
      throw new IllegalStateException("attempt already settled: " + attempt.describe());
    }
    attempt.status = Status.REJECTED;
    attempt.checkedRejection = firstAttempt;
  }

  /** Checks a read that started at {@code startTick}. */
  public synchronized void read(
      final String key, final long startTick, final Optional<Observed> observed) {
    observe(
        key,
        histories.computeIfAbsent(key, (final String ignored) -> new History()),
        startTick,
        observed);
  }

  /**
   * Checks a full scan taken while no write was in flight, then forgets everything but the current
   * records: none of the forgotten values can show up again.
   */
  public synchronized void checkpoint(final Map<String, Observed> scanned) {
    final long startTick = ++clock;
    final Set<String> keys = new HashSet<>(histories.keySet());
    keys.addAll(scanned.keySet());
    present = 0;
    for (final String key : keys) {
      final History history =
          histories.computeIfAbsent(key, (final String ignored) -> new History());
      final Optional<Observed> observed = Optional.ofNullable(scanned.get(key));
      observe(key, history, startTick, observed);
      judge(key, history, observed);

      final History next = new History();
      final long highest = history.completions.getLast()[1];
      next.completions.add(new long[] {startTick, highest});
      if (observed.isPresent()) {
        final Attempt head = history.puts.get(observed.get().value());
        next.puts.put(head.value, head);
        next.producers.put(head.version.versionId(), head);
        present++;
      }
      histories.put(key, next);
    }
  }

  /** Returns how many records the last checkpoint found. */
  public synchronized int size() {
    return present;
  }

  public synchronized void clear() {
    histories.clear();
    present = 0;
  }

  private void observe(
      final String key,
      final History history,
      final long startTick,
      final Optional<Observed> observed) {
    if (observed.isEmpty()) {
      // A delete leaves nothing to compare, and a later create gets a higher version
      complete(history, Long.MIN_VALUE);
      return;
    }
    final Observed record = observed.get();
    final Attempt attempt = history.puts.get(record.value());
    if (attempt == null) {
      throw CorrectnessViolationException.versionHistory(
          key, "read a value that no accepted write of the key wrote: " + record);
    }
    adopt(key, history, attempt, record.version());
    final long versionId = record.version().versionId();
    final long committedBefore = highestBefore(history, startTick);
    if (versionId < committedBefore) {
      throw CorrectnessViolationException.versionHistory(
          key,
          "read version "
              + versionId
              + " after version "
              + committedBefore
              + " had already committed: "
              + attempt.describe());
    }
    complete(history, versionId);
  }

  /** Accepts that an attempt applied with a version, checking it against the chain. */
  private static void adopt(
      final String key, final History history, final Attempt attempt, final RecordVersion version) {
    if (attempt.status == Status.APPLIED) {
      if (!Objects.equals(attempt.version, version)) {
        throw CorrectnessViolationException.versionHistory(
            key, "observed " + version + " for " + attempt.describe());
      }
      return;
    }
    if (attempt.status == Status.REJECTED) {
      throw CorrectnessViolationException.versionHistory(
          key, "a write that the server rejected is visible: " + attempt.describe());
    }
    if (attempt.kind != Kind.DELETE) {
      final String mismatch = metadataMismatch(attempt.kind, attempt.predecessor, version);
      if (mismatch != null) {
        throw CorrectnessViolationException.versionHistory(
            key, mismatch + ": " + version + " for " + attempt.describe());
      }
      final long committedBefore = highestBefore(history, attempt.startTick);
      if (version.versionId() <= committedBefore) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "write got version "
                + version.versionId()
                + " though version "
                + committedBefore
                + " committed before it started: "
                + attempt.describe());
      }
      final Attempt producer = history.producers.putIfAbsent(version.versionId(), attempt);
      if (producer != null) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "two writes got the same version: " + producer.describe() + ", " + attempt.describe());
      }
    }
    if (attempt.kind != Kind.CREATE) {
      final Attempt replaced =
          history.consumers.putIfAbsent(attempt.predecessor.versionId(), attempt);
      if (replaced != null) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "two writes replaced version "
                + attempt.predecessor.versionId()
                + ": "
                + replaced.describe()
                + ", "
                + attempt.describe());
      }
    }
    attempt.status = Status.APPLIED;
    attempt.version = version;
  }

  /** Checks a quiet key's history against what its scan found. */
  private static void judge(
      final String key, final History history, final Optional<Observed> observed) {
    // A pending delete may have applied; a pending create or update would have been read
    final Set<Long> replaced = new HashSet<>(history.consumers.keySet());
    for (final Attempt delete : history.deletes) {
      if (delete.status == Status.PENDING) {
        replaced.add(delete.predecessor.versionId());
      }
    }
    final Optional<Long> current =
        observed.map((final Observed record) -> record.version().versionId());
    if (current.isPresent() && history.consumers.containsKey(current.get())) {
      throw CorrectnessViolationException.versionHistory(
          key,
          "version "
              + current.get()
              + " is still current after "
              + history.consumers.get(current.get()).describe());
    }
    for (final Map.Entry<Long, Attempt> producer : history.producers.entrySet()) {
      if (!current.equals(Optional.of(producer.getKey()))
          && !replaced.contains(producer.getKey())) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "version "
                + producer.getKey()
                + " is gone though no write replaced it: "
                + producer.getValue().describe()
                + ", current="
                + observed.map(Observed::toString).orElse("none"));
      }
    }
    final List<Attempt> attempts = new ArrayList<>(history.puts.values());
    attempts.addAll(history.deletes);
    for (final Attempt attempt : attempts) {
      if (attempt.status != Status.REJECTED || !attempt.checkedRejection) {
        continue;
      }
      final boolean explained =
          attempt.kind == Kind.CREATE
              ? !history.producers.isEmpty()
              : replaced.contains(attempt.predecessor.versionId());
      if (!explained) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "the server rejected a write that no other write conflicts with: "
                + attempt.describe()
                + ", current="
                + observed.map(Observed::toString).orElse("none"));
      }
    }
  }

  /**
   * Returns why a version cannot follow its predecessor, or null. A create starts the modification
   * count at zero; an update increments it and keeps the creation time. Timestamps come from the
   * clocks of different leaders, so only these equalities are checked.
   */
  static String metadataMismatch(
      final Kind kind, final RecordVersion predecessor, final RecordVersion version) {
    if (kind == Kind.CREATE) {
      if (version.modificationsCount() != 0) {
        return "a created record must have no modifications";
      }
      if (version.modifiedTimestamp() != version.createdTimestamp()) {
        return "a created record must be modified when it is created";
      }
      return null;
    }
    if (version.versionId() <= predecessor.versionId()) {
      return "an update must get a higher version than " + predecessor.versionId();
    }
    if (version.modificationsCount() != predecessor.modificationsCount() + 1) {
      return "an update must count one more modification than " + predecessor;
    }
    if (version.createdTimestamp() != predecessor.createdTimestamp()) {
      return "an update must keep the creation time of " + predecessor;
    }
    return null;
  }

  private void complete(final History history, final long versionId) {
    final long highest =
        history.completions.isEmpty()
            ? versionId
            : Math.max(history.completions.getLast()[1], versionId);
    history.completions.add(new long[] {++clock, highest});
  }

  /** Returns the highest version that completed before the tick, or Long.MIN_VALUE. */
  private static long highestBefore(final History history, final long tick) {
    int low = 0;
    int high = history.completions.size() - 1;
    long highest = Long.MIN_VALUE;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final long[] completion = history.completions.get(middle);
      if (completion[0] < tick) {
        highest = completion[1];
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    return highest;
  }
}

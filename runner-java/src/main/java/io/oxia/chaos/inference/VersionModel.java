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
 *
 * <p>This relies on a failed attempt being settled before its operation returns: an attempt that
 * timed out cannot apply after its retry, since the Java client sends the writes of a shard on one
 * stream in submission order. Otherwise it could apply after the checkpoint that ruled it out.
 *
 * <p>Versions only grow, so each key keeps the highest version that any completed operation saw or
 * wrote. An operation records it when it starts: the record cannot be at an older version after
 * that.
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
    private final long committedBefore;
    private Status status = Status.PENDING;
    private RecordVersion version;
    private boolean checkedRejection;

    private Attempt(
        final Kind kind,
        final String value,
        final RecordVersion predecessor,
        final long committedBefore) {
      this.kind = kind;
      this.value = value;
      this.predecessor = predecessor;
      this.committedBefore = committedBefore;
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
    // The highest version that a completed operation saw or wrote
    private long committed = Long.MIN_VALUE;
  }

  private final Map<String, History> histories = new HashMap<>();
  private int present;

  /**
   * Returns the highest version of the key that an operation completed so far, for a read that
   * starts now.
   */
  public synchronized long begin(final String key) {
    return history(key).committed;
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
    final History history = history(key);
    final Attempt attempt = new Attempt(kind, value, predecessor.orElse(null), history.committed);
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
    version.ifPresent((final RecordVersion applied) -> commit(history, applied.versionId()));
  }

  /**
   * Records that the server rejected an attempt for a version mismatch. Only the rejection of a
   * first attempt needs an explanation: a retry may conflict with its own earlier attempt.
   */
  public synchronized void rejected(
      final String key, final Attempt attempt, final boolean firstAttempt) {
    if (attempt.status == Status.APPLIED) {
      throw CorrectnessViolationException.versionHistory(
          key, "the server rejected a write that a read already showed: " + attempt.describe());
    }
    if (attempt.status != Status.PENDING) {
      throw new IllegalStateException("attempt already settled: " + attempt.describe());
    }
    attempt.status = Status.REJECTED;
    attempt.checkedRejection = firstAttempt;
  }

  /**
   * Checks a read, given the highest version that {@link #begin} returned when the read started.
   */
  public synchronized void read(
      final String key, final long committedBefore, final Optional<Observed> observed) {
    observe(key, history(key), committedBefore, observed);
  }

  /**
   * Checks a full scan taken while no write was in flight, then forgets everything but the current
   * records: none of the forgotten values can show up again.
   */
  public synchronized void checkpoint(final Map<String, Observed> scanned) {
    final Set<String> keys = new HashSet<>(histories.keySet());
    keys.addAll(scanned.keySet());
    present = 0;
    for (final String key : keys) {
      final History history = history(key);
      final Optional<Observed> observed = Optional.ofNullable(scanned.get(key));
      observe(key, history, history.committed, observed);
      judge(key, history, observed);

      final History next = new History();
      next.committed = history.committed;
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

  private History history(final String key) {
    return histories.computeIfAbsent(key, (final String ignored) -> new History());
  }

  private static void observe(
      final String key,
      final History history,
      final long committedBefore,
      final Optional<Observed> observed) {
    if (observed.isEmpty()) {
      // The record must have been gone at some point during the read. The highest version
      // committed before the read was current then, unless a write replaced it: a write is
      // registered before it is sent, so it is known by now
      final Attempt current = history.producers.get(committedBefore);
      if (current != null && !replaced(history).contains(committedBefore)) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "found no record though version "
                + committedBefore
                + " was current and no write replaced it: "
                + current.describe());
      }
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
    commit(history, versionId);
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
      if (version.versionId() <= attempt.committedBefore) {
        throw CorrectnessViolationException.versionHistory(
            key,
            "write got version "
                + version.versionId()
                + " though version "
                + attempt.committedBefore
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
    final Set<Long> replaced = replaced(history);
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

  /**
   * Returns the versions that a write may have replaced. A pending delete may have applied; a
   * pending create or update would have been read by the write that replaced it.
   */
  private static Set<Long> replaced(final History history) {
    final Set<Long> replaced = new HashSet<>(history.consumers.keySet());
    for (final Attempt delete : history.deletes) {
      if (delete.status == Status.PENDING) {
        replaced.add(delete.predecessor.versionId());
      }
    }
    return replaced;
  }

  private static void commit(final History history, final long versionId) {
    history.committed = Math.max(history.committed, versionId);
  }
}

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
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.api.Notification.KeyRangeDelete;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Matches the notifications of a case's keys against the writes the case made to them.
 *
 * <p>The notifications of a key come from the stream of its shard, in the order the shard applied
 * the writes, so each key's notifications must follow its writes exactly. Oxia documents
 * at-least-once delivery; exactly-once is a property of the client implementation that this checker
 * tests: the client resumes each shard's stream after the offset of the last batch it received and
 * drops a batch it already received. A write whose attempt failed has an unknown outcome: each
 * failed attempt may add one notification before the one of the attempt that succeeded.
 *
 * <p>A range delete notifies once per shard without telling which shard. Its bounds are unique per
 * range delete, and it must be notified at least once. Every notification gets the order it arrived
 * in, and one shard's notifications arrive in the order of its stream. So, for each key the range
 * delete covers, one of its notifications must arrive after the key's notification before the range
 * delete, and before the key's notification after it. What this can't check: how many shards
 * notified a range delete, and a range delete notification that one shard lost or reordered while
 * another shard's notification of the same range delete arrived inside the key's window.
 */
public final class NotificationChecker implements Consumer<Notification> {

  private final String prefix;
  private final Map<String, KeyHistory> keys;
  private final Map<Range, RangeHistory> ranges = new HashMap<>();
  private final List<Notification> unexpected = new ArrayList<>();
  private long arrivals;

  public NotificationChecker(final String prefix, final List<String> keys) {
    this.prefix = prefix;
    final Map<String, KeyHistory> histories = new LinkedHashMap<>();
    for (final String key : keys) {
      histories.put(key, new KeyHistory());
    }
    this.keys = histories;
  }

  /** Records a notification; runs on the client's notification threads. */
  @Override
  public synchronized void accept(final Notification notification) {
    final long arrival = ++arrivals;
    if (notification instanceof final KeyRangeDelete range) {
      if (!range.startKeyInclusive().startsWith(prefix)) {
        return;
      }
      final RangeHistory history =
          ranges.get(new Range(range.startKeyInclusive(), range.endKeyExclusive()));
      if (history == null) {
        unexpected.add(notification);
      } else {
        history.arrivals.add(arrival);
      }
      return;
    }
    if (!notification.key().startsWith(prefix)) {
      return;
    }
    final KeyHistory history = keys.get(notification.key());
    if (history == null) {
      unexpected.add(notification);
    } else {
      history.observed.add(new Observed(notification, arrival));
    }
  }

  /** Whether a notification of the key at the version or a later one arrived. */
  public synchronized boolean observed(final String key, final long versionId) {
    return keys.get(key).observed.stream()
        .anyMatch((final Observed observed) -> putVersion(observed.notification()) >= versionId);
  }

  /**
   * Starts the strict checks once every key was notified: each shard that holds a key now streams
   * from before the versions given, which the keys hold.
   */
  public synchronized void start(final Map<String, Long> versions) {
    for (final Map.Entry<String, KeyHistory> entry : keys.entrySet()) {
      final KeyHistory history = entry.getValue();
      history.exists = true;
      history.versionId = versions.get(entry.getKey());
      history.arrival = history.observed.isEmpty() ? 0 : history.observed.getLast().arrival();
      history.writes.clear();
      history.observed.clear();
    }
    unexpected.clear();
  }

  public synchronized void put(
      final String key, final long versionId, final long modificationsCount, final int attempts) {
    keys.get(key).writes.add(new Put(versionId, modificationsCount, attempts));
  }

  public synchronized void delete(final String key, final boolean deleted, final int attempts) {
    keys.get(key).writes.add(new Delete(deleted, attempts));
  }

  /**
   * Registers a range delete before it is sent, so that none of its notifications is unexpected.
   */
  public synchronized void registerRange(final String startInclusive, final String endExclusive) {
    ranges.put(new Range(startInclusive, endExclusive), new RangeHistory());
  }

  public synchronized void rangeDeleted(final String startInclusive, final String endExclusive) {
    final Range range = new Range(startInclusive, endExclusive);
    final RangeHistory history = ranges.get(range);
    history.acknowledged = true;
    for (final Map.Entry<String, KeyHistory> entry : keys.entrySet()) {
      final String key = entry.getKey();
      if (key.compareTo(startInclusive) >= 0 && key.compareTo(endExclusive) < 0) {
        entry.getValue().writes.add(new Covered(range.toString(), history.arrivals));
      }
    }
  }

  /** The version a key holds after the last check that matched, if it exists. */
  public synchronized Optional<Long> version(final String key) {
    final KeyHistory history = keys.get(key);
    return history.exists ? Optional.of(history.versionId) : Optional.empty();
  }

  /**
   * Matches every acknowledged write with its notifications while no write is in flight. Returns
   * the notifications still missing; when none are, keeps the matched state and drops the history.
   *
   * @throws CorrectnessViolationException for a notification that no valid history explains
   */
  public synchronized List<String> check() {
    if (!unexpected.isEmpty()) {
      final Notification notification = unexpected.getFirst();
      throw CorrectnessViolationException.notificationMismatch(
          notification.key(), "no notification outside the case's writes", describe(notification));
    }
    final List<String> missing = new ArrayList<>();
    final Map<KeyHistory, Match> matches = new HashMap<>();
    for (final Map.Entry<String, KeyHistory> entry : keys.entrySet()) {
      final KeyHistory history = entry.getValue();
      final Match match =
          match(
              entry.getKey(),
              new Match(history.exists, history.versionId, history.arrival, Optional.empty()),
              history.writes,
              history.observed);
      if (match.missing().isPresent()) {
        missing.add(entry.getKey() + ": " + match.missing().get());
      } else {
        matches.put(history, match);
      }
    }
    for (final Map.Entry<Range, RangeHistory> entry : ranges.entrySet()) {
      if (entry.getValue().acknowledged && entry.getValue().arrivals.isEmpty()) {
        missing.add(entry.getKey() + ": a range delete notification");
      }
    }
    if (!missing.isEmpty()) {
      return missing;
    }
    for (final Map.Entry<KeyHistory, Match> entry : matches.entrySet()) {
      final KeyHistory history = entry.getKey();
      history.exists = entry.getValue().exists();
      history.versionId = entry.getValue().versionId();
      history.arrival = entry.getValue().arrival();
      history.writes.clear();
      history.observed.clear();
    }
    return List.of();
  }

  /**
   * Matches the notifications of one key against its writes, from the state the key had before
   * them: whether it existed, its version, and when its last notification arrived.
   */
  public static Match match(
      final String key,
      final Match previous,
      final List<Write> writes,
      final List<Observed> observed) {
    boolean exists = previous.exists();
    long versionId = previous.versionId();
    long arrival = previous.arrival();
    int next = 0;
    for (final Write write : writes) {
      switch (write) {
        case final Put put -> {
          int failedAttemptsApplied = 0;
          while (true) {
            if (next == observed.size()) {
              return Match.missing("the notification of version " + put.versionId());
            }
            final Notification notification = observed.get(next).notification();
            final long notified = putVersion(notification);
            if (notified < 0 || notified > put.versionId()) {
              throw CorrectnessViolationException.notificationMismatch(
                  key, expectedPut(exists, put.versionId()), describe(notification));
            }
            if (notified <= versionId) {
              throw CorrectnessViolationException.notificationMismatch(
                  key, "a version after " + versionId, describe(notification));
            }
            if ((notification instanceof KeyCreated) == exists) {
              throw CorrectnessViolationException.notificationMismatch(
                  key, expectedPut(exists, notified), describe(notification));
            }
            arrival = observed.get(next).arrival();
            next++;
            exists = true;
            versionId = notified;
            if (notified == put.versionId()) {
              if ((notification instanceof KeyCreated) != (put.modificationsCount() == 0)) {
                throw CorrectnessViolationException.notificationMismatch(
                    key,
                    "a notification matching modificationsCount=" + put.modificationsCount(),
                    describe(notification));
              }
              break;
            }
            // An attempt that failed was applied before the one that succeeded
            if (++failedAttemptsApplied >= put.attempts()) {
              throw CorrectnessViolationException.notificationMismatch(
                  key, expectedPut(false, put.versionId()), describe(notification));
            }
          }
        }
        case final Delete delete -> {
          if (delete.deleted() && !exists) {
            throw CorrectnessViolationException.notificationMismatch(
                key, "a notification of the record that the delete found", "the key was deleted");
          }
          if (exists && !delete.deleted() && delete.attempts() == 1) {
            throw CorrectnessViolationException.notificationMismatch(
                key, "a delete that finds the record", "the delete found no record");
          }
          if (exists) {
            // Deleted by the attempt that succeeded, or by one that failed before it
            if (next == observed.size()) {
              return Match.missing("the notification of a delete");
            }
            final Notification notification = observed.get(next).notification();
            if (!(notification instanceof KeyDeleted)) {
              throw CorrectnessViolationException.notificationMismatch(
                  key, "KeyDeleted", describe(notification));
            }
            arrival = observed.get(next).arrival();
            next++;
            exists = false;
          }
        }
        case final Covered covered -> {
          // The key's shard notified the range delete between the key's notifications around it
          final long after = arrival;
          final long before =
              next < observed.size() ? observed.get(next).arrival() : Long.MAX_VALUE;
          final Optional<Long> inside =
              covered.arrivals().stream()
                  .filter((final Long notified) -> notified > after && notified < before)
                  .findFirst();
          if (inside.isEmpty()) {
            if (before == Long.MAX_VALUE) {
              return Match.missing("a notification of the range delete " + covered.range());
            }
            throw CorrectnessViolationException.notificationMismatch(
                key,
                "KeyRangeDelete " + covered.range(),
                describe(observed.get(next).notification()));
          }
          arrival = inside.get();
          exists = false;
        }
      }
    }
    if (next < observed.size()) {
      throw CorrectnessViolationException.notificationMismatch(
          key, "no notification after the last write", describe(observed.get(next).notification()));
    }
    return new Match(exists, versionId, arrival, Optional.empty());
  }

  private static long putVersion(final Notification notification) {
    return switch (notification) {
      case final KeyCreated created -> created.version();
      case final KeyModified modified -> modified.version();
      default -> -1;
    };
  }

  private static String expectedPut(final boolean exists, final long versionId) {
    return (exists ? "KeyModified" : "KeyCreated") + " version=" + versionId;
  }

  private static String describe(final Notification notification) {
    return switch (notification) {
      case final KeyCreated created -> "KeyCreated version=" + created.version();
      case final KeyModified modified -> "KeyModified version=" + modified.version();
      case final KeyDeleted ignored -> "KeyDeleted";
      case final KeyRangeDelete range ->
          "KeyRangeDelete [" + range.startKeyInclusive() + ", " + range.endKeyExclusive() + ")";
    };
  }

  /** A notification and the order it arrived in, among all the notifications. */
  public record Observed(Notification notification, long arrival) {}

  /** A write the case made to one key, in the order it made them. */
  public sealed interface Write permits Put, Delete, Covered {}

  /** A put acknowledged at a version after the given number of attempts. */
  public record Put(long versionId, long modificationsCount, int attempts) implements Write {}

  /** A delete acknowledged with whether it found the record, after the given number of attempts. */
  public record Delete(boolean deleted, int attempts) implements Write {}

  /**
   * An acknowledged range delete covering the key, with the arrivals of its notifications so far.
   */
  public record Covered(String range, List<Long> arrivals) implements Write {}

  /**
   * The state of one key after matching its writes: whether it exists, its version, and when its
   * last notification arrived; or what is still missing.
   */
  public record Match(boolean exists, long versionId, long arrival, Optional<String> missing) {

    static Match missing(final String what) {
      return new Match(false, -1, -1, Optional.of(what));
    }
  }

  private record Range(String startInclusive, String endExclusive) {

    @Override
    public String toString() {
      return "[" + startInclusive + ", " + endExclusive + ")";
    }
  }

  private static final class RangeHistory {

    private final List<Long> arrivals = new ArrayList<>();
    private boolean acknowledged;
  }

  private static final class KeyHistory {

    private final List<Write> writes = new ArrayList<>();
    private final List<Observed> observed = new ArrayList<>();
    private boolean exists;
    private long versionId = -1;
    private long arrival;
  }
}

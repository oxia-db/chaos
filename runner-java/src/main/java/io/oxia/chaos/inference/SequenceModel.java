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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Reference state of server-assigned sequence keys and of the sequence updates delivered for them.
 *
 * <p>The case thread owns every method except {@link #delivered(String, String)}, which the client
 * calls from its own threads, and {@link #size()}, which the metrics reader calls.
 */
public final class SequenceModel {

  private final Map<String, Sequence> sequences = new LinkedHashMap<>();
  private final AtomicInteger size = new AtomicInteger();

  public void register(final String prefix, final int parts) {
    if (parts <= 0) {
      throw new IllegalArgumentException("a sequence needs at least one part");
    }
    sequences.put(prefix, new Sequence(parts));
  }

  /** Returns the key that the server assigns for the given sequence values. */
  public static String key(final String prefix, final long[] values) {
    final StringBuilder key = new StringBuilder(prefix);
    for (final long value : values) {
      key.append('-').append(String.format(Locale.ROOT, "%020d", value));
    }
    return key.toString();
  }

  /** Parses the sequence values of a key assigned under the prefix. */
  public static Optional<long[]> parse(final String prefix, final int parts, final String key) {
    if (!key.startsWith(prefix + "-")) {
      return Optional.empty();
    }
    final String[] encoded = key.substring(prefix.length() + 1).split("-", -1);
    if (encoded.length != parts) {
      return Optional.empty();
    }
    final long[] values = new long[parts];
    for (int index = 0; index < parts; index++) {
      if (encoded[index].length() != 20 || !encoded[index].chars().allMatch(Character::isDigit)) {
        return Optional.empty();
      }
      values[index] = Long.parseLong(encoded[index]);
    }
    return Optional.of(values);
  }

  /**
   * Returns how many attempts before the acknowledged one were applied: the server adds the deltas
   * once per applied attempt, so the acknowledged values are {@code last + (skips + 1) * deltas}.
   */
  public static OptionalInt skips(
      final long[] last, final long[] deltas, final long[] actual, final int maxSkips) {
    for (int skips = 0; skips <= maxSkips; skips++) {
      final int steps = skips + 1;
      if (IntStream.range(0, last.length)
          .allMatch((final int index) -> actual[index] == last[index] + steps * deltas[index])) {
        return OptionalInt.of(skips);
      }
    }
    return OptionalInt.empty();
  }

  /**
   * Returns the values the next put with the deltas is assigned when no earlier attempt applied.
   */
  public long[] next(final String prefix, final long[] deltas) {
    final long[] last = sequence(prefix).last;
    final long[] next = new long[last.length];
    for (int index = 0; index < last.length; index++) {
      next[index] = last[index] + deltas[index];
    }
    return next;
  }

  /**
   * Records an acknowledged sequence put. Up to {@code failedAttempts} earlier attempts of the same
   * put may have been applied, each creating a record with the same value; those records are
   * adopted. Returns the adopted keys, or empty when no valid history explains the assigned key.
   */
  public Optional<List<String>> assign(
      final String prefix,
      final long[] deltas,
      final int failedAttempts,
      final String actualKey,
      final byte[] value) {
    final Sequence sequence = sequence(prefix);
    final Optional<long[]> actual = parse(prefix, sequence.last.length, actualKey);
    if (actual.isEmpty()) {
      return Optional.empty();
    }
    final OptionalInt skips = skips(sequence.last, deltas, actual.get(), failedAttempts);
    if (skips.isEmpty()) {
      return Optional.empty();
    }
    final List<String> adopted = new ArrayList<>(skips.getAsInt());
    for (int step = 0; step <= skips.getAsInt(); step++) {
      for (int index = 0; index < sequence.last.length; index++) {
        sequence.last[index] += deltas[index];
      }
      final String key = key(prefix, sequence.last);
      if (step < skips.getAsInt()) {
        adopted.add(key);
      }
      sequence.live.put(key, value);
      sequence.assigned.add(key);
      size.incrementAndGet();
    }
    sequence.tail = Optional.of(actualKey);
    return Optional.of(List.copyOf(adopted));
  }

  /** Returns the lowest live key, unless it is also the last one: the tail is never trimmed. */
  public Optional<String> trimmable(final String prefix) {
    final Sequence sequence = sequence(prefix);
    return sequence.live.size() < 2 ? Optional.empty() : Optional.of(sequence.live.firstKey());
  }

  public Optional<String> keyAt(final String prefix, final int index) {
    return sequence(prefix).live.keySet().stream().skip(index).findFirst();
  }

  public Optional<byte[]> get(final String prefix, final String key) {
    return Optional.ofNullable(sequence(prefix).live.get(key));
  }

  public void delete(final String prefix, final String key) {
    if (sequence(prefix).live.remove(key) != null) {
      size.decrementAndGet();
    }
  }

  /** Returns a copy of the live records of the prefix ordered by key. */
  public NavigableMap<String, byte[]> live(final String prefix) {
    return new TreeMap<>(sequence(prefix).live);
  }

  public int liveCount(final String prefix) {
    return sequence(prefix).live.size();
  }

  /** Returns the last assigned key of the prefix, which the sequence updates must converge to. */
  public Optional<String> tail(final String prefix) {
    return sequence(prefix).tail;
  }

  /** Queues a key delivered by the sequence updates; safe to call from any thread. */
  public void delivered(final String prefix, final String key) {
    sequence(prefix).deliveries.add(key);
  }

  /** Returns the latest delivered key that {@link #validateDeliveries(String)} accepted. */
  public Optional<String> latestDelivered(final String prefix) {
    return sequence(prefix).delivered;
  }

  /**
   * Checks the queued deliveries in order: each one repeats the latest key or moves to a later key
   * that was assigned. Call it only when no put is in flight, so every applied put is recorded.
   * Returns the first violation found, if any.
   */
  public Optional<String> validateDeliveries(final String prefix) {
    final Sequence sequence = sequence(prefix);
    while (true) {
      final String key = sequence.deliveries.poll();
      if (key == null) {
        return Optional.empty();
      }
      if (sequence.delivered.isPresent()) {
        final int order = key.compareTo(sequence.delivered.get());
        if (order == 0) {
          continue;
        }
        if (order < 0) {
          return Optional.of("regressed from " + sequence.delivered.get() + " to " + key);
        }
      }
      if (!sequence.assigned.contains(key)) {
        return Optional.of("delivered a key that was never assigned: " + key);
      }
      sequence.delivered = Optional.of(key);
      // Later deliveries can't go below this one, so the earlier keys are no longer needed
      sequence.assigned.headSet(key, false).clear();
    }
  }

  public int size() {
    return size.get();
  }

  public void clear() {
    for (final Sequence sequence : sequences.values()) {
      sequence.live.clear();
      sequence.assigned.clear();
      sequence.deliveries.clear();
    }
    size.set(0);
  }

  private Sequence sequence(final String prefix) {
    final Sequence sequence = sequences.get(prefix);
    if (sequence == null) {
      throw new IllegalArgumentException("unknown sequence prefix: " + prefix);
    }
    return sequence;
  }

  private static final class Sequence {

    private final long[] last;
    private final TreeMap<String, byte[]> live = new TreeMap<>();
    private final TreeSet<String> assigned = new TreeSet<>();
    private final ConcurrentLinkedQueue<String> deliveries = new ConcurrentLinkedQueue<>();
    private Optional<String> tail = Optional.empty();
    private Optional<String> delivered = Optional.empty();

    private Sequence(final int parts) {
      this.last = new long[parts];
    }
  }
}

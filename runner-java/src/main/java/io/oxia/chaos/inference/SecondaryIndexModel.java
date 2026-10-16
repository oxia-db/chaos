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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Reference state of records and the entries they give their secondary indexes.
 *
 * <p>An index holds one entry per secondary key of a record: a record with several secondary keys
 * in a range appears once for each, and the same secondary key given twice is one entry.
 */
public final class SecondaryIndexModel {

  private final ConcurrentSkipListMap<String, IndexedRecord> records =
      new ConcurrentSkipListMap<>();
  // index name -> secondary key -> primary keys
  private final Map<String, NavigableMap<String, NavigableSet<String>>> indexes = new TreeMap<>();

  public Optional<IndexedRecord> get(final String key) {
    return Optional.ofNullable(records.get(key));
  }

  /** Replaces the record and its index entries, as a put does. */
  public void put(final String key, final byte[] value, final Map<String, Set<String>> secondary) {
    delete(key);
    final Map<String, NavigableSet<String>> copied = new TreeMap<>();
    for (final Map.Entry<String, Set<String>> entry : secondary.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      copied.put(entry.getKey(), new TreeSet<>(entry.getValue()));
      final NavigableMap<String, NavigableSet<String>> index =
          indexes.computeIfAbsent(entry.getKey(), (final String name) -> new TreeMap<>());
      for (final String secondaryKey : entry.getValue()) {
        index.computeIfAbsent(secondaryKey, (final String ignored) -> new TreeSet<>()).add(key);
      }
    }
    records.put(key, new IndexedRecord(key, value, copied));
  }

  public void delete(final String key) {
    final IndexedRecord removed = records.remove(key);
    if (removed == null) {
      return;
    }
    for (final Map.Entry<String, NavigableSet<String>> entry : removed.secondary().entrySet()) {
      final NavigableMap<String, NavigableSet<String>> index = indexes.get(entry.getKey());
      for (final String secondaryKey : entry.getValue()) {
        final NavigableSet<String> primaries = index.get(secondaryKey);
        primaries.remove(key);
        if (primaries.isEmpty()) {
          index.remove(secondaryKey);
        }
      }
    }
  }

  public void deleteRange(final String fromInclusive, final String toExclusive) {
    for (final String key : List.copyOf(records.subMap(fromInclusive, toExclusive).keySet())) {
      delete(key);
    }
  }

  /** Returns the records ordered by primary key in {@code [fromInclusive, toExclusive)}. */
  public List<IndexedRecord> range(final String fromInclusive, final String toExclusive) {
    return List.copyOf(records.subMap(fromInclusive, toExclusive).values());
  }

  /** Returns the primary keys that have the secondary key in the index. */
  public Set<String> primaries(final String indexName, final String secondaryKey) {
    final NavigableMap<String, NavigableSet<String>> index = indexes.get(indexName);
    if (index == null || !index.containsKey(secondaryKey)) {
      return Set.of();
    }
    return Set.copyOf(index.get(secondaryKey));
  }

  /**
   * Returns the records of the index entries whose secondary key is in {@code [fromInclusive,
   * toExclusive)}, one per entry, ordered by secondary key and then primary key.
   */
  public List<IndexedRecord> indexRange(
      final String indexName, final String fromInclusive, final String toExclusive) {
    final NavigableMap<String, NavigableSet<String>> index = indexes.get(indexName);
    if (index == null) {
      return List.of();
    }
    final List<IndexedRecord> entries = new ArrayList<>();
    for (final NavigableSet<String> primaries : index.subMap(fromInclusive, toExclusive).values()) {
      for (final String primary : primaries) {
        entries.add(records.get(primary));
      }
    }
    return List.copyOf(entries);
  }

  public int size() {
    return records.size();
  }

  public void clear() {
    records.clear();
    indexes.clear();
  }

  /** A record with its secondary keys by index name. */
  public record IndexedRecord(
      String key, byte[] value, Map<String, NavigableSet<String>> secondary) {}
}

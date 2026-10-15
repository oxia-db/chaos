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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/** Ordered reference state of the ephemeral records that live owners are expected to hold. */
public final class EphemeralModel {

  private final ConcurrentSkipListMap<String, EphemeralRecord> records =
      new ConcurrentSkipListMap<>();

  public Optional<EphemeralRecord> get(final String key) {
    return Optional.ofNullable(records.get(key));
  }

  public void put(final EphemeralRecord record) {
    records.put(record.key(), record);
  }

  public void delete(final String key) {
    records.remove(key);
  }

  /** Returns the expected records in {@code [fromInclusive, toExclusive)} ordered by key. */
  public List<EphemeralRecord> range(final String fromInclusive, final String toExclusive) {
    return List.copyOf(records.subMap(fromInclusive, toExclusive).values());
  }

  /** Forgets every record in the range and returns what the range held before removal. */
  public List<EphemeralRecord> removeRange(final String fromInclusive, final String toExclusive) {
    final var view = records.subMap(fromInclusive, toExclusive);
    final List<EphemeralRecord> removed = List.copyOf(view.values());
    view.clear();
    return removed;
  }

  public int size() {
    return records.size();
  }

  public void clear() {
    records.clear();
  }
}

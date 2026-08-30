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

import io.oxia.chaos.inference.InferenceStore.KeyValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** Ordered, in-memory {@link InferenceStore} implementation shared by correctness cases. */
public final class MemoryInferenceStore implements InferenceStore {

  private final ConcurrentSkipListMap<String, byte[]> values = new ConcurrentSkipListMap<>();

  @Override
  public Optional<KeyValue> get(String key) {
    return valueOf(key, values.get(key));
  }

  @Override
  public Optional<KeyValue> floor(String key) {
    return entryOf(values.floorEntry(key));
  }

  @Override
  public Optional<KeyValue> ceiling(String key) {
    return entryOf(values.ceilingEntry(key));
  }

  @Override
  public Optional<KeyValue> lower(String key) {
    return entryOf(values.lowerEntry(key));
  }

  @Override
  public Optional<KeyValue> higher(String key) {
    return entryOf(values.higherEntry(key));
  }

  @Override
  public void put(String key, byte[] value) {
    values.put(key, copy(value));
  }

  @Override
  public boolean delete(String key) {
    return values.remove(key) != null;
  }

  @Override
  public void deleteRange(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    values.subMap(fromInclusive, true, toExclusive, false).clear();
  }

  @Override
  public List<KeyValue> range(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    List<KeyValue> result = new ArrayList<>();
    values
        .subMap(fromInclusive, true, toExclusive, false)
        .forEach((key, value) -> result.add(new KeyValue(key, value)));
    return List.copyOf(result);
  }

  @Override
  public List<String> list(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    return List.copyOf(values.subMap(fromInclusive, true, toExclusive, false).keySet());
  }

  @Override
  public NavigableMap<String, byte[]> snapshot() {
    NavigableMap<String, byte[]> snapshot = new TreeMap<>();
    values.forEach((key, value) -> snapshot.put(key, copy(value)));
    return Collections.unmodifiableNavigableMap(snapshot);
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public void clear() {
    values.clear();
  }

  private static Optional<KeyValue> entryOf(java.util.Map.Entry<String, byte[]> entry) {
    return entry == null
        ? Optional.empty()
        : Optional.of(new KeyValue(entry.getKey(), entry.getValue()));
  }

  private static Optional<KeyValue> valueOf(String key, byte[] value) {
    return value == null ? Optional.empty() : Optional.of(new KeyValue(key, value));
  }

  private static void validateRange(String fromInclusive, String toExclusive) {
    if (fromInclusive.compareTo(toExclusive) > 0) {
      throw new IllegalArgumentException("range start must not be greater than range end");
    }
  }

  private static byte[] copy(byte[] value) {
    if (value == null) {
      throw new NullPointerException("value");
    }
    return Arrays.copyOf(value, value.length);
  }
}

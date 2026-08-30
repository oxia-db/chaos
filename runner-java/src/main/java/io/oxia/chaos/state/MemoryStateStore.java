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
package io.oxia.chaos.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** Per-run, ordered, in-memory reference state shared by correctness cases. */
public final class MemoryStateStore {

  private final ConcurrentSkipListMap<String, byte[]> values = new ConcurrentSkipListMap<>();

  public Optional<KeyValue> get(String key) {
    return valueOf(key, values.get(key));
  }

  public Optional<KeyValue> floor(String key) {
    return entryOf(values.floorEntry(key));
  }

  public Optional<KeyValue> ceiling(String key) {
    return entryOf(values.ceilingEntry(key));
  }

  public Optional<KeyValue> lower(String key) {
    return entryOf(values.lowerEntry(key));
  }

  public Optional<KeyValue> higher(String key) {
    return entryOf(values.higherEntry(key));
  }

  public void put(String key, byte[] value) {
    values.put(key, copy(value));
  }

  public boolean delete(String key) {
    return values.remove(key) != null;
  }

  public void deleteRange(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    values.subMap(fromInclusive, true, toExclusive, false).clear();
  }

  public List<KeyValue> range(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    List<KeyValue> result = new ArrayList<>();
    values
        .subMap(fromInclusive, true, toExclusive, false)
        .forEach((key, value) -> result.add(new KeyValue(key, value)));
    return List.copyOf(result);
  }

  public List<String> list(String fromInclusive, String toExclusive) {
    validateRange(fromInclusive, toExclusive);
    return List.copyOf(values.subMap(fromInclusive, true, toExclusive, false).keySet());
  }

  public NavigableMap<String, byte[]> snapshot() {
    NavigableMap<String, byte[]> snapshot = new TreeMap<>();
    values.forEach((key, value) -> snapshot.put(key, copy(value)));
    return Collections.unmodifiableNavigableMap(snapshot);
  }

  public int size() {
    return values.size();
  }

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

  /** Immutable key/value view with defensive value copies. */
  public record KeyValue(String key, byte[] value) {

    public KeyValue {
      if (key == null) {
        throw new NullPointerException("key");
      }
      value = copy(value);
    }

    @Override
    public byte[] value() {
      return copy(value);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof KeyValue that
          && key.equals(that.key)
          && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return 31 * key.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
      return "KeyValue[key=" + key + ", value=" + Base64.getEncoder().encodeToString(value) + "]";
    }
  }
}

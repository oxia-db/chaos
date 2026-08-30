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

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;

/** Ordered reference data used to infer the expected result of client operations. */
public interface InferenceStore {

  Optional<KeyValue> get(String key);

  Optional<KeyValue> floor(String key);

  Optional<KeyValue> ceiling(String key);

  Optional<KeyValue> lower(String key);

  Optional<KeyValue> higher(String key);

  void put(String key, byte[] value);

  boolean delete(String key);

  void deleteRange(String fromInclusive, String toExclusive);

  List<KeyValue> range(String fromInclusive, String toExclusive);

  List<String> list(String fromInclusive, String toExclusive);

  NavigableMap<String, byte[]> snapshot();

  int size();

  void clear();

  /** Immutable key/value view with defensive value copies. */
  record KeyValue(String key, byte[] value) {

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

    private static byte[] copy(byte[] value) {
      if (value == null) {
        throw new NullPointerException("value");
      }
      return Arrays.copyOf(value, value.length);
    }
  }
}

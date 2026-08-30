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
import java.util.NavigableMap;
import java.util.Optional;

/** Ordered reference data used to infer the expected result of client operations. */
public interface InferenceStore {

  Optional<KeyValue> get(final String key);

  Optional<KeyValue> floor(final String key);

  Optional<KeyValue> ceiling(final String key);

  Optional<KeyValue> lower(final String key);

  Optional<KeyValue> higher(final String key);

  void put(final String key, final byte[] value);

  boolean delete(final String key);

  void deleteRange(final String fromInclusive, final String toExclusive);

  List<KeyValue> range(final String fromInclusive, final String toExclusive);

  List<String> list(final String fromInclusive, final String toExclusive);

  NavigableMap<String, byte[]> snapshot();

  int size();

  void clear();
}

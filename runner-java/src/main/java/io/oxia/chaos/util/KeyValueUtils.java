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
package io.oxia.chaos.util;

import io.oxia.chaos.inference.KeyValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Helpers for comparing key/value results independently of shard delivery order. */
public final class KeyValueUtils {

  private static final Comparator<KeyValue> BY_KEY = Comparator.comparing(KeyValue::key);

  private KeyValueUtils() {}

  public static List<KeyValue> sortedByKey(final List<KeyValue> values) {
    final List<KeyValue> sorted = new ArrayList<>(values);
    sorted.sort(BY_KEY);
    return List.copyOf(sorted);
  }
}

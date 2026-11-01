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
package io.oxia.chaos.testcase.basickv.read;

import io.oxia.chaos.error.ViolationException;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.basickv.OperationStrategy;
import io.oxia.chaos.testcase.basickv.Planned;
import io.oxia.chaos.testcase.basickv.ReferenceUpdate;
import io.oxia.chaos.testcase.basickv.Workload;
import io.oxia.chaos.util.KeyValue;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.options.GetOption;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;

/** Gets a key, or the closest key in one direction, and compares it with the reference. */
public record OrderedGet(Operation operation, double weight, Workload workload)
    implements OperationStrategy {

  @Override
  public BatchType batchType() {
    return BatchType.READ;
  }

  @Override
  public Planned plan(final SplittableRandom random, final int index, final long sequence) {
    final String key = workload.keyGenerator().key(index);
    return new Planned(key, () -> verify(key), ReferenceUpdate.NONE);
  }

  private void verify(final String key) {
    final var reference = workload.reference();
    final GetOption option;
    final Map.Entry<String, byte[]> entry;
    switch (operation) {
      case GET -> {
        option = GetOption.ComparisonEqual;
        final byte[] value = reference.get(key);
        entry = value == null ? null : Map.entry(key, value);
      }
      case FLOOR -> {
        option = GetOption.ComparisonFloor;
        entry = reference.floorEntry(key);
      }
      case CEILING -> {
        option = GetOption.ComparisonCeiling;
        entry = reference.ceilingEntry(key);
      }
      case LOWER -> {
        option = GetOption.ComparisonLower;
        entry = reference.lowerEntry(key);
      }
      case HIGHER -> {
        option = GetOption.ComparisonHigher;
        entry = reference.higherEntry(key);
      }
      default -> throw new IllegalArgumentException("not a get operation: " + operation);
    }
    final Optional<KeyValue> expected =
        entry == null
            ? Optional.empty()
            : Optional.of(new KeyValue(entry.getKey(), entry.getValue()));
    final GetResult result = workload.client().get(key, Set.of(option));
    final Optional<KeyValue> actual =
        result == null ? Optional.empty() : Optional.of(new KeyValue(result.key(), result.value()));
    if (!expected.equals(actual)) {
      throw ViolationException.operationMismatch(
          operation, key, expected.toString(), actual.toString());
    }
  }
}

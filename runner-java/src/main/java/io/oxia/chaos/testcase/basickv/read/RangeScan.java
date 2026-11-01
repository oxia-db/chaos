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

import static io.oxia.chaos.util.KeyValueUtils.sortedByKey;

import io.oxia.chaos.error.ViolationException;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.basickv.OperationStrategy;
import io.oxia.chaos.testcase.basickv.Planned;
import io.oxia.chaos.testcase.basickv.ReferenceUpdate;
import io.oxia.chaos.testcase.basickv.Workload;
import io.oxia.chaos.util.KeyValue;
import io.oxia.chaos.util.RangeUtils;
import io.oxia.chaos.util.SummaryUtils;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/** Scans a short range of keys and compares it with the reference. */
public record RangeScan(double weight, Workload workload) implements OperationStrategy {

  @Override
  public Operation operation() {
    return Operation.RANGE_SCAN;
  }

  @Override
  public BatchType batchType() {
    return BatchType.READ;
  }

  @Override
  public Planned plan(final SplittableRandom random, final int index, final long sequence) {
    final String key = workload.keyGenerator().key(index);
    final String endKey =
        workload.keyGenerator().key(RangeUtils.nextEnd(random, index, workload.keyCount()));
    return new Planned(key, () -> verify(key, endKey), ReferenceUpdate.NONE);
  }

  private void verify(final String key, final String endKey) {
    final List<KeyValue> expected =
        workload.reference().subMap(key, endKey).entrySet().stream()
            .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
            .toList();
    final List<KeyValue> unorderedActual = new ArrayList<>();
    try (final CloseableIterable<GetResult> results = workload.client().rangeScan(key, endKey)) {
      for (final GetResult result : results) {
        unorderedActual.add(new KeyValue(result.key(), result.value()));
      }
    }
    final List<KeyValue> actual = sortedByKey(unorderedActual);
    if (!expected.equals(actual)) {
      throw ViolationException.operationMismatch(
          Operation.RANGE_SCAN,
          key,
          SummaryUtils.summarize(expected),
          SummaryUtils.summarize(actual));
    }
  }
}

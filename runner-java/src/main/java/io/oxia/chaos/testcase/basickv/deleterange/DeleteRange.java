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
package io.oxia.chaos.testcase.basickv.deleterange;

import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.basickv.OperationStrategy;
import io.oxia.chaos.testcase.basickv.Planned;
import io.oxia.chaos.testcase.basickv.Workload;
import io.oxia.chaos.util.RangeUtils;
import java.util.SplittableRandom;

/** Deletes a short range of keys. */
public record DeleteRange(double weight, Workload workload) implements OperationStrategy {

  @Override
  public Operation operation() {
    return Operation.DELETE_RANGE;
  }

  @Override
  public BatchType batchType() {
    return BatchType.DELETE_RANGE;
  }

  @Override
  public Planned plan(final SplittableRandom random, final int index, final long sequence) {
    final String key = workload.keyGenerator().key(index);
    final String endKey =
        workload.keyGenerator().key(RangeUtils.nextEnd(random, index, workload.keyCount()));
    return new Planned(
        key,
        () -> workload.client().deleteRange(key, endKey),
        () -> workload.reference().subMap(key, endKey).clear());
  }
}

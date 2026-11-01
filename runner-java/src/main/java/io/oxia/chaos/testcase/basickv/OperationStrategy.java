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
package io.oxia.chaos.testcase.basickv;

import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import java.util.SplittableRandom;

/** One kind of operation of the basic key/value workload, picked by its weight. */
public interface OperationStrategy {

  Operation operation();

  /** The batch the operation runs in: operations of a batch run concurrently. */
  BatchType batchType();

  /** The relative chance of picking the operation. */
  double weight();

  /** Plans the operation on the key at {@code index}, the n-th of the run as {@code sequence}. */
  Planned plan(SplittableRandom random, int index, long sequence);
}

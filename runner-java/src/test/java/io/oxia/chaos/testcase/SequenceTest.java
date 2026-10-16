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
package io.oxia.chaos.testcase;

import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.chaos.ops.Operation;
import org.junit.jupiter.api.Test;

class SequenceTest {

  @Test
  void mixesAppendsReadsAndTrims() {
    assertThat(Sequence.selectOperation(0.0)).isEqualTo(Operation.SEQUENCE_PUT);
    assertThat(Sequence.selectOperation(0.49)).isEqualTo(Operation.SEQUENCE_PUT);
    assertThat(Sequence.selectOperation(0.5)).isEqualTo(Operation.GET);
    assertThat(Sequence.selectOperation(0.79)).isEqualTo(Operation.GET);
    assertThat(Sequence.selectOperation(0.8)).isEqualTo(Operation.DELETE);
    assertThat(Sequence.selectOperation(Math.nextDown(Sequence.TOTAL_WEIGHT)))
        .isEqualTo(Operation.DELETE);
  }

  @Test
  void alternatesOneAndTwoPartSequences() {
    assertThat(Sequence.parts(0)).isOne();
    assertThat(Sequence.parts(1)).isEqualTo(2);
    assertThat(Sequence.parts(2)).isOne();
  }
}

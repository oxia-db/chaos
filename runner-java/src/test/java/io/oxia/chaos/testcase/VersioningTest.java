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

class VersioningTest {

  @Test
  void selectsTheWriteForAPresentRecord() {
    assertThat(Versioning.selectOperation(0.0, 7)).isEqualTo(Operation.CONDITIONAL_PUT);
    assertThat(Versioning.selectOperation(0.7, 7)).isEqualTo(Operation.CONDITIONAL_DELETE);
    assertThat(Versioning.selectOperation(0.85, 7)).isEqualTo(Operation.STALE_PUT);
    assertThat(Versioning.selectOperation(Versioning.TOTAL_WEIGHT - 0.01, 7))
        .isEqualTo(Operation.STALE_DELETE);
  }

  @Test
  void writesTheVersionReadWhenNoOlderVersionExists() {
    assertThat(Versioning.selectOperation(0.85, 0)).isEqualTo(Operation.CONDITIONAL_PUT);
    assertThat(Versioning.selectOperation(Versioning.TOTAL_WEIGHT - 0.01, 0))
        .isEqualTo(Operation.CONDITIONAL_PUT);
  }

  @Test
  void sendsAShareOfTheOperationsToTheHotKeys() {
    assertThat(Versioning.selectKeyIndex(0.1, 1_000_003, 10_000)).isLessThan(16);
    assertThat(Versioning.selectKeyIndex(0.9, 1_000_003, 10_000)).isEqualTo(3);
    assertThat(Versioning.selectKeyIndex(0.1, -5, 4)).isBetween(0, 3);
    assertThat(Versioning.selectKeyIndex(0.9, -5, 4)).isBetween(0, 3);
  }
}

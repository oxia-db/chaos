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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class RangeUtilsTest {

  @Test
  void capsRangesAtOnePercentWithAOneKeyMinimum() {
    assertThat(RangeUtils.maximumLength(1)).isEqualTo(1);
    assertThat(RangeUtils.maximumLength(99)).isEqualTo(1);
    assertThat(RangeUtils.maximumLength(10_000)).isEqualTo(100);
    assertThat(RangeUtils.maximumLength(100_000)).isEqualTo(1_000);
  }

  @Test
  void capsTheEndAtTheKeyCount() {
    assertThat(RangeUtils.nextEnd(new SplittableRandom(7), 99, 100)).isEqualTo(100);
  }
}

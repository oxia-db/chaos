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
package io.oxia.chaos.basic;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class BasicKvCaseTest {

  @Test
  void followsTheNormalizedBasicKvDistribution() {
    SplittableRandom random = new SplittableRandom(7);
    Map<String, Integer> counts = new HashMap<>();
    int samples = 250_000;

    for (int index = 0; index < samples; index++) {
      String operation = BasicKvCase.selectOperation(random.nextDouble(BasicKvCase.TOTAL_WEIGHT));
      counts.merge(operation, 1, Integer::sum);
    }

    assertShare(counts, samples, "put", 0.2930);
    assertShare(counts, samples, "get", 0.2198);
    assertShare(counts, samples, "floor", 0.0733);
    assertShare(counts, samples, "ceiling", 0.0733);
    assertShare(counts, samples, "lower", 0.0733);
    assertShare(counts, samples, "higher", 0.0733);
    assertShare(counts, samples, "delete", 0.1172);
    assertShare(counts, samples, "delete-range", 0.0037);
    assertShare(counts, samples, "range-scan", 0.0440);
    assertShare(counts, samples, "list", 0.0293);
  }

  @Test
  void capsRangesAtOnePercentWithAOneKeyMinimum() {
    assertThat(BasicKvCase.maximumRangeLength(1)).isEqualTo(1);
    assertThat(BasicKvCase.maximumRangeLength(99)).isEqualTo(1);
    assertThat(BasicKvCase.maximumRangeLength(10_000)).isEqualTo(100);
    assertThat(BasicKvCase.maximumRangeLength(100_000)).isEqualTo(1_000);
  }

  private static void assertShare(
      Map<String, Integer> counts, int samples, String operation, double expected) {
    double actual = counts.getOrDefault(operation, 0) / (double) samples;
    assertThat(actual).isCloseTo(expected, Offset.offset(0.0025));
  }
}

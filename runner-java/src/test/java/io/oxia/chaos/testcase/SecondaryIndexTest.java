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

import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import io.oxia.client.api.options.PutOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class SecondaryIndexTest {

  @Test
  void followsTheSecondaryIndexDistribution() {
    final SplittableRandom random = new SplittableRandom(7);
    final Map<Operation, Integer> counts = new HashMap<>();
    final int samples = 250_000;

    for (int index = 0; index < samples; index++) {
      counts.merge(
          SecondaryIndex.selectOperation(random.nextDouble(SecondaryIndex.TOTAL_WEIGHT)),
          1,
          Integer::sum);
    }

    assertShare(counts, samples, Operation.PUT, 0.30);
    assertShare(counts, samples, Operation.DELETE, 0.08);
    assertShare(counts, samples, Operation.DELETE_RANGE, 0.01);
    assertShare(counts, samples, Operation.GET, 0.15);
    assertShare(counts, samples, Operation.INDEX_GET, 0.20);
    assertShare(counts, samples, Operation.INDEX_LIST, 0.13);
    assertShare(counts, samples, Operation.INDEX_RANGE_SCAN, 0.13);
    assertThat(SecondaryIndex.selectOperation(0)).isEqualTo(Operation.PUT);
    assertThat(SecondaryIndex.selectOperation(Math.nextDown(SecondaryIndex.TOTAL_WEIGHT)))
        .isEqualTo(Operation.INDEX_RANGE_SCAN);
  }

  @Test
  void batchesWritesReadsAndDeleteRangesSeparately() {
    assertThat(SecondaryIndex.batchType(Operation.PUT)).isEqualTo(BatchType.WRITE);
    assertThat(SecondaryIndex.batchType(Operation.DELETE)).isEqualTo(BatchType.WRITE);
    assertThat(SecondaryIndex.batchType(Operation.DELETE_RANGE)).isEqualTo(BatchType.DELETE_RANGE);
    for (final Operation operation :
        List.of(
            Operation.GET, Operation.INDEX_GET, Operation.INDEX_LIST, Operation.INDEX_RANGE_SCAN)) {
      assertThat(SecondaryIndex.batchType(operation)).isEqualTo(BatchType.READ);
    }
  }

  @Test
  void drawsTheSecondaryKeysOfANewRecord() {
    final SplittableRandom random = new SplittableRandom(11);
    final AtomicInteger draws = new AtomicInteger();
    for (int sample = 0; sample < 1_000; sample++) {
      final Map<String, Set<String>> secondary =
          SecondaryIndex.nextSecondaryKeys(
              random, Map.of(), () -> "new-" + draws.getAndIncrement());
      assertThat(secondary.keySet()).containsExactlyElementsOf(SecondaryIndex.INDEXES);
      for (final Set<String> keys : secondary.values()) {
        assertThat(keys).hasSizeLessThanOrEqualTo(SecondaryIndex.MAX_SECONDARY_KEYS_PER_INDEX);
        assertThat(keys).allMatch(key -> key.startsWith("new-"));
      }
    }
  }

  @Test
  void updatesKeepAllKeepSomeAddToOrRedrawTheSecondaryKeys() {
    final SplittableRandom random = new SplittableRandom(13);
    final Map<String, Set<String>> current =
        Map.of("bucket", Set.of("b1", "b2", "b3"), "label", Set.of("l1", "l2"));
    final Set<SecondaryIndex.SecondaryKeysUpdate> seen = new HashSet<>();
    final AtomicInteger draws = new AtomicInteger();
    for (int sample = 0; sample < 2_000; sample++) {
      final Map<String, Set<String>> updated =
          SecondaryIndex.nextSecondaryKeys(random, current, () -> "new-" + draws.getAndIncrement());
      final Set<String> bucket = updated.get("bucket");
      final Set<String> kept = new HashSet<>(bucket);
      kept.retainAll(current.get("bucket"));
      final Set<String> added = new HashSet<>(bucket);
      added.removeAll(current.get("bucket"));
      if (updated.equals(current)) {
        seen.add(SecondaryIndex.SecondaryKeysUpdate.KEEP_ALL);
      } else if (added.isEmpty() && !kept.isEmpty()) {
        seen.add(SecondaryIndex.SecondaryKeysUpdate.KEEP_SOME);
      } else if (kept.equals(current.get("bucket")) && added.size() == 1) {
        seen.add(SecondaryIndex.SecondaryKeysUpdate.ADD);
        assertThat(updated.get("label")).containsAll(current.get("label")).hasSize(3);
      } else if (kept.isEmpty() && !added.isEmpty()) {
        seen.add(SecondaryIndex.SecondaryKeysUpdate.REDRAW);
      }
    }
    assertThat(seen).containsExactlyInAnyOrder(SecondaryIndex.SecondaryKeysUpdate.values());
  }

  @Test
  void sharesSecondaryKeysBetweenRecords() {
    assertThat(SecondaryIndex.secondaryKeyCount(10_000)).isEqualTo(2_500);
    assertThat(SecondaryIndex.secondaryKeyCount(3)).isOne();
  }

  @Test
  void givesOnePutOptionPerSecondaryKey() {
    final Map<String, Set<String>> secondary = new HashMap<>();
    secondary.put("bucket", new LinkedHashSet<>(List.of("s1", "s2")));
    secondary.put("label", Set.of("s1"));
    secondary.put("empty", Set.of());

    assertThat(SecondaryIndex.putOptions(secondary))
        .containsExactlyInAnyOrder(
            PutOption.SecondaryIndex("bucket", "s1"),
            PutOption.SecondaryIndex("bucket", "s2"),
            PutOption.SecondaryIndex("label", "s1"));
  }

  private static void assertShare(
      final Map<Operation, Integer> counts,
      final int samples,
      final Operation operation,
      final double expected) {
    assertThat(counts.getOrDefault(operation, 0) / (double) samples)
        .isCloseTo(expected, Offset.offset(0.005));
  }
}

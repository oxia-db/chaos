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
package io.oxia.chaos.inference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemoryInferenceStoreTest {

  private InferenceStore inference;

  @BeforeEach
  void setUp() {
    inference = new MemoryInferenceStore();
    inference.put("b", bytes("two"));
    inference.put("d", bytes("four"));
    inference.put("f", bytes("six"));
  }

  @Test
  void providesOrderedPointAndComparisonReads() {
    assertThat(inference.get("d")).contains(new KeyValue("d", bytes("four")));
    assertThat(inference.get("c")).isEmpty();
    assertThat(inference.floor("c")).contains(new KeyValue("b", bytes("two")));
    assertThat(inference.ceiling("c")).contains(new KeyValue("d", bytes("four")));
    assertThat(inference.lower("d")).contains(new KeyValue("b", bytes("two")));
    assertThat(inference.higher("d")).contains(new KeyValue("f", bytes("six")));
  }

  @Test
  void scansAndListsExclusiveEndRangesInOrder() {
    assertThat(inference.range("b", "f"))
        .containsExactly(new KeyValue("b", bytes("two")), new KeyValue("d", bytes("four")));
    assertThat(inference.list("b", "f")).containsExactly("b", "d");
  }

  @Test
  void deletesPointsAndRanges() {
    assertThat(inference.delete("b")).isTrue();
    assertThat(inference.delete("b")).isFalse();

    inference.deleteRange("c", "g");

    assertThat(inference.list("a", "z")).isEmpty();
  }

  @Test
  void protectsStoredAndReturnedValuesFromMutation() {
    byte[] input = bytes("value");
    inference.put("key", input);
    input[0] = 'X';

    byte[] returned = inference.get("key").orElseThrow().value();
    returned[1] = 'X';
    byte[] snapshotValue = inference.snapshot().get("key");
    snapshotValue[2] = 'X';

    assertThat(inference.get("key").orElseThrow().value()).isEqualTo(bytes("value"));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(UTF_8);
  }
}

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
package io.oxia.chaos.state;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.chaos.state.MemoryStateStore.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemoryStateStoreTest {

  private MemoryStateStore state;

  @BeforeEach
  void setUp() {
    state = new MemoryStateStore();
    state.put("b", bytes("two"));
    state.put("d", bytes("four"));
    state.put("f", bytes("six"));
  }

  @Test
  void providesOrderedPointAndComparisonReads() {
    assertThat(state.get("d")).contains(new KeyValue("d", bytes("four")));
    assertThat(state.get("c")).isEmpty();
    assertThat(state.floor("c")).contains(new KeyValue("b", bytes("two")));
    assertThat(state.ceiling("c")).contains(new KeyValue("d", bytes("four")));
    assertThat(state.lower("d")).contains(new KeyValue("b", bytes("two")));
    assertThat(state.higher("d")).contains(new KeyValue("f", bytes("six")));
  }

  @Test
  void scansAndListsExclusiveEndRangesInOrder() {
    assertThat(state.range("b", "f"))
        .containsExactly(new KeyValue("b", bytes("two")), new KeyValue("d", bytes("four")));
    assertThat(state.list("b", "f")).containsExactly("b", "d");
  }

  @Test
  void deletesPointsAndRanges() {
    assertThat(state.delete("b")).isTrue();
    assertThat(state.delete("b")).isFalse();

    state.deleteRange("c", "g");

    assertThat(state.list("a", "z")).isEmpty();
  }

  @Test
  void protectsStoredAndReturnedValuesFromMutation() {
    byte[] input = bytes("value");
    state.put("key", input);
    input[0] = 'X';

    byte[] returned = state.get("key").orElseThrow().value();
    returned[1] = 'X';
    byte[] snapshotValue = state.snapshot().get("key");
    snapshotValue[2] = 'X';

    assertThat(state.get("key").orElseThrow().value()).isEqualTo(bytes("value"));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(UTF_8);
  }
}

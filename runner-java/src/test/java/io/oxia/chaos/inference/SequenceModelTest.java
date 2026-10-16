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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SequenceModelTest {

  private static final String PREFIX = "/runs/r/sequence/p-00";
  private static final String PAIR = "/runs/r/sequence/p-01";

  @Test
  void formatsAndParsesSequenceKeys() {
    final String key = SequenceModel.key(PAIR, new long[] {7, 0});

    assertThat(key).isEqualTo(PAIR + "-00000000000000000007-00000000000000000000");
    assertThat(SequenceModel.parse(PAIR, 2, key))
        .hasValueSatisfying(v -> assertThat(v).containsExactly(7, 0));
    assertThat(SequenceModel.parse(PAIR, 1, key)).isEmpty();
    assertThat(SequenceModel.parse(PREFIX, 1, key)).isEmpty();
    assertThat(SequenceModel.parse(PREFIX, 1, PREFIX + "-7")).isEmpty();
  }

  @Test
  void findsThePositionOfAKeyAfterTheLastOne() {
    final long[] last = {4, 2};
    final long[] deltas = {3, 1};

    assertThat(SequenceModel.position(last, deltas, new long[] {7, 3})).hasValue(1);
    assertThat(SequenceModel.position(last, deltas, new long[] {10, 4})).hasValue(2);
    assertThat(SequenceModel.position(last, deltas, new long[] {10, 3})).isEmpty();
    assertThat(SequenceModel.position(last, deltas, new long[] {8, 3})).isEmpty();
    assertThat(SequenceModel.position(last, deltas, new long[] {4, 2})).isEmpty();
  }

  @Test
  void assignsKeysByAddingTheDeltasToTheLastKey() {
    final SequenceModel model = model();

    assertThat(model.assign(PAIR, new long[] {2, 0}, 0, acked(PAIR, "a", 2, 0)))
        .contains(List.of());
    assertThat(model.assign(PAIR, new long[] {1, 3}, 0, acked(PAIR, "b", 3, 3)))
        .contains(List.of());
    assertThat(model.next(PAIR, new long[] {1, 0})).containsExactly(4, 3);
    assertThat(model.tail(PAIR)).contains(SequenceModel.key(PAIR, new long[] {3, 3}));
    assertThat(model.liveCount(PAIR)).isEqualTo(2);
    assertThat(model.size()).isEqualTo(2);

    // A key that skips the deltas without a failed attempt has no valid history
    assertThat(model.assign(PAIR, new long[] {1, 0}, 0, acked(PAIR, "c", 5, 3))).isEmpty();
  }

  @Test
  void leavesThePositionsOfAppliedFailedAttemptsToAdopt() {
    final SequenceModel model = model();
    model.assign(PREFIX, new long[] {1}, 0, acked(PREFIX, "a", 1));

    assertThat(model.assign(PREFIX, new long[] {2}, 2, acked(PREFIX, "b", 7)))
        .contains(List.of(key(PREFIX, 3), key(PREFIX, 5)));
    model.adopt(PREFIX, key(PREFIX, 3), value("b"));
    model.adopt(PREFIX, key(PREFIX, 5), value("b"));
    assertThat(model.live(PREFIX).keySet())
        .containsExactly(key(PREFIX, 1), key(PREFIX, 3), key(PREFIX, 5), key(PREFIX, 7));
    assertThat(model.get(PREFIX, key(PREFIX, 5))).hasValue(value("b"));
    assertThat(model.size()).isEqualTo(4);
  }

  @Test
  void acceptsConcurrentPutsAtConsecutivePositionsInAnyOrder() {
    final SequenceModel model = model();
    final Map<String, byte[]> acknowledged = new LinkedHashMap<>();
    acknowledged.put(key(PREFIX, 6), value("c"));
    acknowledged.put(key(PREFIX, 2), value("a"));
    acknowledged.put(key(PREFIX, 4), value("b"));

    assertThat(model.assign(PREFIX, new long[] {2}, 0, acknowledged)).contains(List.of());
    assertThat(model.tail(PREFIX)).contains(key(PREFIX, 6));
    assertThat(model.get(PREFIX, key(PREFIX, 2))).hasValue(value("a"));

    // One applied failed attempt can fill one free position, not two
    final Map<String, byte[]> gapped = new LinkedHashMap<>();
    gapped.put(key(PREFIX, 8), value("d"));
    gapped.put(key(PREFIX, 14), value("e"));
    assertThat(model(PREFIX, 6).assign(PREFIX, new long[] {2}, 1, gapped)).isEmpty();
    assertThat(model(PREFIX, 6).assign(PREFIX, new long[] {2}, 2, gapped))
        .contains(List.of(key(PREFIX, 10), key(PREFIX, 12)));
  }

  @Test
  void trimsFromTheHeadButNeverTheTail() {
    final SequenceModel model = model();
    model.assign(PREFIX, new long[] {1}, 0, acked(PREFIX, "a", 1));
    assertThat(model.trimmable(PREFIX)).isEmpty();

    model.assign(PREFIX, new long[] {1}, 0, acked(PREFIX, "b", 2));
    assertThat(model.trimmable(PREFIX)).contains(SequenceModel.key(PREFIX, new long[] {1}));

    model.delete(PREFIX, SequenceModel.key(PREFIX, new long[] {1}));
    assertThat(model.trimmable(PREFIX)).isEmpty();
    assertThat(model.size()).isOne();
    assertThat(model.keyAt(PREFIX, 0)).contains(SequenceModel.key(PREFIX, new long[] {2}));
  }

  @Test
  void acceptsUpdatesThatRepeatOrSkipForwardToAssignedKeys() {
    final SequenceModel model = model();
    for (long value = 1; value <= 4; value++) {
      model.assign(PREFIX, new long[] {1}, 0, acked(PREFIX, "v", value));
    }
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {1}));
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {1}));
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {3}));

    assertThat(model.validateDeliveries(PREFIX)).isEmpty();
    assertThat(model.latestDelivered(PREFIX)).contains(SequenceModel.key(PREFIX, new long[] {3}));

    // A trimmed key that was assigned is still a valid, if late, update
    model.delete(PREFIX, SequenceModel.key(PREFIX, new long[] {4}));
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {4}));
    assertThat(model.validateDeliveries(PREFIX)).isEmpty();
  }

  @Test
  void rejectsUpdatesThatGoBackOrToUnassignedKeys() {
    final SequenceModel model = model();
    for (long value = 1; value <= 3; value++) {
      model.assign(PREFIX, new long[] {1}, 0, acked(PREFIX, "v", value));
    }
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {2}));
    model.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {1}));
    assertThat(model.validateDeliveries(PREFIX))
        .hasValueSatisfying(v -> assertThat(v).contains("regressed"));

    final SequenceModel other = model();
    other.assign(PREFIX, new long[] {2}, 0, acked(PREFIX, "v", 2));
    other.delivered(PREFIX, SequenceModel.key(PREFIX, new long[] {1}));
    assertThat(other.validateDeliveries(PREFIX))
        .hasValueSatisfying(v -> assertThat(v).contains("never assigned"));
  }

  private static SequenceModel model(final String prefix, final long last) {
    final SequenceModel model = model();
    model.assign(prefix, new long[] {last}, 0, acked(prefix, "base", last));
    return model;
  }

  private static Map<String, byte[]> acked(
      final String prefix, final String value, final long... values) {
    return Map.of(SequenceModel.key(prefix, values), value(value));
  }

  private static String key(final String prefix, final long value) {
    return SequenceModel.key(prefix, new long[] {value});
  }

  private static SequenceModel model() {
    final SequenceModel model = new SequenceModel();
    model.register(PREFIX, 1);
    model.register(PAIR, 2);
    return model;
  }

  private static byte[] value(final String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }
}

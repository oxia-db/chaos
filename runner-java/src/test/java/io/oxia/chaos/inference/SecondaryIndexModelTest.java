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

import io.oxia.chaos.inference.SecondaryIndexModel.IndexedRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SecondaryIndexModelTest {

  @Test
  void listsARecordOnceForEachOfItsSecondaryKeysInTheRange() {
    final SecondaryIndexModel model = new SecondaryIndexModel();
    model.put("k1", bytes("v1"), Map.of("i", Set.of("s1", "s3")));
    model.put("k2", bytes("v2"), Map.of("i", Set.of("s1"), "j", Set.of("s2")));

    assertThat(keys(model.indexRange("i", "s0", "s9"))).containsExactly("k1", "k2", "k1");
    assertThat(keys(model.indexRange("i", "s1", "s3"))).containsExactly("k1", "k2");
    assertThat(keys(model.indexRange("j", "s0", "s9"))).containsExactly("k2");
    assertThat(model.indexRange("missing", "s0", "s9")).isEmpty();
    assertThat(model.primaries("i", "s1")).containsExactlyInAnyOrder("k1", "k2");
    assertThat(model.primaries("i", "s2")).isEmpty();
  }

  @Test
  void replacesTheSecondaryKeysOfAnUpdatedRecord() {
    final SecondaryIndexModel model = new SecondaryIndexModel();
    model.put("k1", bytes("v1"), Map.of("i", Set.of("s1", "s2"), "j", Set.of("s1")));
    model.put("k1", bytes("v2"), Map.of("i", Set.of("s2", "s3"), "j", Set.of()));

    assertThat(model.primaries("i", "s1")).isEmpty();
    assertThat(model.primaries("i", "s2")).containsExactly("k1");
    assertThat(model.primaries("i", "s3")).containsExactly("k1");
    assertThat(model.indexRange("j", "s0", "s9")).isEmpty();
    assertThat(model.indexRange("i", "s0", "s9"))
        .extracting(IndexedRecord::value)
        .containsExactly(bytes("v2"), bytes("v2"));
    assertThat(model.size()).isOne();
  }

  @Test
  void dropsTheEntriesOfDeletedRecords() {
    final SecondaryIndexModel model = new SecondaryIndexModel();
    model.put("k1", bytes("v"), Map.of("i", Set.of("s1")));
    model.put("k2", bytes("v"), Map.of("i", Set.of("s1")));
    model.put("k3", bytes("v"), Map.of("i", Set.of("s1")));

    model.delete("k1");
    assertThat(model.primaries("i", "s1")).containsExactlyInAnyOrder("k2", "k3");

    model.deleteRange("k2", "k9");
    assertThat(model.primaries("i", "s1")).isEmpty();
    assertThat(model.indexRange("i", "s0", "s9")).isEmpty();
    assertThat(model.size()).isZero();

    model.delete("absent");
    assertThat(model.get("absent")).isEmpty();
  }

  @Test
  void rangesRecordsByPrimaryKey() {
    final SecondaryIndexModel model = new SecondaryIndexModel();
    model.put("k2", bytes("v2"), Map.of());
    model.put("k1", bytes("v1"), Map.of("i", Set.of("s1")));

    assertThat(keys(model.range("k0", "k9"))).containsExactly("k1", "k2");

    model.clear();
    assertThat(model.size()).isZero();
    assertThat(model.primaries("i", "s1")).isEmpty();
  }

  private static List<String> keys(final List<IndexedRecord> records) {
    return records.stream().map(IndexedRecord::key).toList();
  }

  private static byte[] bytes(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}

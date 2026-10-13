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
import org.junit.jupiter.api.Test;

class EphemeralModelTest {

  @Test
  void tracksTheLatestRecordPerKey() {
    final EphemeralModel model = new EphemeralModel();
    model.put(record("a/1", "v1", "owner-a", 1));
    model.put(record("a/1", "v2", "owner-a", 1));

    assertThat(model.get("a/1")).contains(record("a/1", "v2", "owner-a", 1));
    assertThat(model.size()).isOne();

    model.delete("a/1");
    assertThat(model.get("a/1")).isEmpty();
    assertThat(model.size()).isZero();
  }

  @Test
  void removesOnlyTheEndedOwnersRange() {
    final EphemeralModel model = new EphemeralModel();
    model.put(record("a/1", "v", "owner-a", 1));
    model.put(record("a/2", "v", "owner-a", 2));
    model.put(record("b/1", "v", "owner-b", 3));

    assertThat(model.removeRange("a/", "a/~"))
        .containsExactly(record("a/1", "v", "owner-a", 1), record("a/2", "v", "owner-a", 2));
    assertThat(model.range("a/", "a/~")).isEmpty();
    assertThat(model.range("b/", "b/~")).containsExactly(record("b/1", "v", "owner-b", 3));
  }

  @Test
  void replacesOnlyTheSessionOfARecord() {
    final EphemeralRecord original = record("a/1", "v", "owner-a", 1);

    assertThat(original.withSessionId(9)).isEqualTo(record("a/1", "v", "owner-a", 9));
    assertThat(original).isNotEqualTo(original.withSessionId(9));
  }

  @Test
  void copiesValuesDefensively() {
    final byte[] value = "v".getBytes(StandardCharsets.UTF_8);
    final EphemeralRecord record = new EphemeralRecord("a/1", value, "owner-a", 1);
    value[0] = 'x';
    record.value()[0] = 'y';

    assertThat(record.value()).isEqualTo("v".getBytes(StandardCharsets.UTF_8));
  }

  private static EphemeralRecord record(
      final String key, final String value, final String owner, final long sessionId) {
    return new EphemeralRecord(key, value.getBytes(StandardCharsets.UTF_8), owner, sessionId);
  }
}

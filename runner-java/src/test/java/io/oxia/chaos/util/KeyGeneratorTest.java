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

import org.junit.jupiter.api.Test;

class KeyGeneratorTest {

  @Test
  void generatesOrderedRunScopedKeysAndGuards() {
    KeyGenerator generator = new KeyGenerator("run-1");

    assertThat(generator.key(12)).isEqualTo("/oxia-chaos/runs/run-1/keys/key-00000012");
    assertThat(generator.lowerGuardKey()).isLessThan(generator.key(0));
    assertThat(generator.upperGuardKey()).isGreaterThan(generator.key(99_999));
    assertThat(generator.afterUpperGuardKey()).isGreaterThan(generator.upperGuardKey());
  }
}

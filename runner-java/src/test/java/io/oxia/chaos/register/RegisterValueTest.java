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
package io.oxia.chaos.register;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RegisterValueTest {

  @Test
  void protectsHistoryValuesFromMutation() {
    byte[] input = "value".getBytes(StandardCharsets.UTF_8);
    RegisterValue value = new RegisterValue(input, 3);

    input[0] = 'X';
    byte[] returned = value.value();
    returned[1] = 'X';

    assertThat(value.value()).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
    assertThat(value).isEqualTo(new RegisterValue("value".getBytes(StandardCharsets.UTF_8), 3));
  }
}

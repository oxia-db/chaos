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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.ops.Operation;
import org.junit.jupiter.api.Test;

class DeleteUtilsTest {

  @Test
  void requiresTheFirstAttemptToMatchExactly() {
    assertThatCode(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", true, true, false))
        .doesNotThrowAnyException();
    assertThatCode(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", false, false, false))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", true, false, false))
        .isInstanceOf(CorrectnessViolationException.class);
    assertThatThrownBy(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", false, true, false))
        .isInstanceOf(CorrectnessViolationException.class);
  }

  @Test
  void acceptsAnAlreadyDeletedKeyOnlyWhenRetryingADelete() {
    assertThatCode(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", true, false, true))
        .doesNotThrowAnyException();
    assertThatCode(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", true, true, true))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> DeleteUtils.verifyDelete(Operation.DELETE, "k", false, true, true))
        .isInstanceOf(CorrectnessViolationException.class);
  }
}

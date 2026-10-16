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

import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.ops.Operation;

/** Checks delete results against the reference model. */
public final class DeleteUtils {

  private DeleteUtils() {}

  /**
   * Checks the result of a delete attempt. A failed attempt has an unknown outcome: the server may
   * have applied it, so a retry of a delete expected to find the key may find it already deleted.
   * The first attempt must match exactly.
   */
  public static void verifyDelete(
      final Operation operation,
      final String key,
      final boolean expected,
      final boolean actual,
      final boolean retry) {
    if (expected != actual && !(retry && expected)) {
      throw CorrectnessViolationException.operationMismatch(
          operation, key, Boolean.toString(expected), Boolean.toString(actual));
    }
  }
}

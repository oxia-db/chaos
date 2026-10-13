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
package io.oxia.chaos.error;

import io.oxia.chaos.ops.Operation;

/** Signals a correctness failure distinct from an infrastructure or availability failure. */
public final class CorrectnessViolationException extends RuntimeException {

  public CorrectnessViolationException(final String message) {
    super(message);
  }

  public static CorrectnessViolationException operationMismatch(
      final Operation operation, final String key, final String expected, final String actual) {
    return new CorrectnessViolationException(
        "operation mismatch: operation="
            + operation.label()
            + ", key="
            + key
            + ", expected="
            + expected
            + ", actual="
            + actual);
  }

  public static CorrectnessViolationException checkpointMismatch(
      final String expected, final String actual) {
    return new CorrectnessViolationException(
        "checkpoint mismatch: expected=" + expected + ", actual=" + actual);
  }

  public static CorrectnessViolationException ephemeralLost(
      final String key, final long sessionId) {
    return new CorrectnessViolationException(
        "ephemeral record lost while its session was alive: key="
            + key
            + ", sessionId="
            + sessionId);
  }

  public static CorrectnessViolationException earlyExpiry(
      final String owner, final int expected, final int present, final long elapsedMillis) {
    return new CorrectnessViolationException(
        "ephemeral records expired before the session timeout: owner="
            + owner
            + ", expected="
            + expected
            + ", present="
            + present
            + ", elapsedMs="
            + elapsedMillis);
  }

  public static CorrectnessViolationException ephemeralLeaked(
      final String owner, final String remaining, final long elapsedMillis) {
    return new CorrectnessViolationException(
        "ephemeral records outlived their ended owner: owner="
            + owner
            + ", remaining="
            + remaining
            + ", elapsedMs="
            + elapsedMillis);
  }
}

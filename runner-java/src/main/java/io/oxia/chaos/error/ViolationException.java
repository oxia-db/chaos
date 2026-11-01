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
public final class ViolationException extends RuntimeException {

  public ViolationException(final String message) {
    super(message);
  }

  public static ViolationException operationMismatch(
      final Operation operation, final String key, final String expected, final String actual) {
    return new ViolationException(
        "operation mismatch: operation="
            + operation.label()
            + ", key="
            + key
            + ", expected="
            + expected
            + ", actual="
            + actual);
  }

  public static ViolationException checkpointMismatch(final String expected, final String actual) {
    return new ViolationException(
        "checkpoint mismatch: expected=" + expected + ", actual=" + actual);
  }

  public static ViolationException ephemeralLost(final String key, final long sessionId) {
    return new ViolationException(
        "ephemeral record lost while its session was alive: key="
            + key
            + ", sessionId="
            + sessionId);
  }

  public static ViolationException earlyExpiry(
      final String owner, final int expected, final int present, final long elapsedMillis) {
    return new ViolationException(
        "ephemeral records expired before the session timeout: owner="
            + owner
            + ", expected="
            + expected
            + ", present="
            + present
            + ", elapsedMs="
            + elapsedMillis);
  }

  public static ViolationException ephemeralLeaked(
      final String owner, final String remaining, final long elapsedMillis) {
    return new ViolationException(
        "ephemeral records outlived their ended owner: owner="
            + owner
            + ", remaining="
            + remaining
            + ", elapsedMs="
            + elapsedMillis);
  }

  public static ViolationException notificationMismatch(
      final String key, final String expected, final String actual) {
    return new ViolationException(
        "notification mismatch: key=" + key + ", expected=" + expected + ", actual=" + actual);
  }

  public static ViolationException versionHistory(final String key, final String detail) {
    return new ViolationException("version history violation: key=" + key + ", " + detail);
  }
}

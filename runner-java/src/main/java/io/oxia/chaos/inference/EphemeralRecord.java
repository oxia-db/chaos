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

import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/** Immutable view of an ephemeral record and the client session that owns it. */
public record EphemeralRecord(String key, byte[] value, String owner, long sessionId) {

  public EphemeralRecord {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(owner, "owner");
    value = copy(value);
  }

  @Override
  public byte[] value() {
    return copy(value);
  }

  public EphemeralRecord withSessionId(final long replacement) {
    return new EphemeralRecord(key, value, owner, replacement);
  }

  @Override
  public boolean equals(final Object other) {
    return other instanceof final EphemeralRecord that
        && key.equals(that.key)
        && Arrays.equals(value, that.value)
        && owner.equals(that.owner)
        && sessionId == that.sessionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, Arrays.hashCode(value), owner, sessionId);
  }

  @Override
  public String toString() {
    return "EphemeralRecord[key="
        + key
        + ", value="
        + Base64.getEncoder().encodeToString(value)
        + ", owner="
        + owner
        + ", sessionId="
        + sessionId
        + "]";
  }

  private static byte[] copy(final byte[] value) {
    Objects.requireNonNull(value, "value");
    return Arrays.copyOf(value, value.length);
  }
}

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

import java.util.Arrays;
import java.util.Objects;

/** A value and its Oxia version at the point it was read. */
public record RegisterValue(byte[] value, long versionId) {

  public RegisterValue {
    Objects.requireNonNull(value, "value");
    value = Arrays.copyOf(value, value.length);
  }

  @Override
  public byte[] value() {
    return Arrays.copyOf(value, value.length);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RegisterValue that
        && versionId == that.versionId
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(value) + Long.hashCode(versionId);
  }
}

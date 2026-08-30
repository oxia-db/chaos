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

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** Generates deterministic values for correctness workloads. */
public final class ValueGenerator {

  private final long seed;

  public ValueGenerator(long seed) {
    this.seed = seed;
  }

  public byte[] warmup(int index) {
    return String.format(Locale.ROOT, "warmup-%016x-%08d", seed, index)
        .getBytes(StandardCharsets.UTF_8);
  }

  public byte[] next(int index, long operationCount) {
    return String.format(Locale.ROOT, "value-%016x-%08d-%016x", seed, index, operationCount)
        .getBytes(StandardCharsets.UTF_8);
  }
}

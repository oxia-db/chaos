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

import java.util.concurrent.TimeUnit;

/** Monotonic timing helpers shared by runner cases. */
public final class Timing {

  private Timing() {}

  public static long addSaturated(final long left, final long right) {
    final long result = left + right;
    if (((left ^ result) & (right ^ result)) < 0) {
      return Long.MAX_VALUE;
    }
    return result;
  }

  public static long elapsedMillis(final long startedNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
  }

  public static double elapsedSeconds(final long startedNanos) {
    return (System.nanoTime() - startedNanos) / 1_000_000_000d;
  }
}

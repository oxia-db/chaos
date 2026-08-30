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

import static io.oxia.chaos.util.Timing.addSaturated;

import java.util.concurrent.TimeUnit;

/** Monotonic, non-bursting operation pacer shared by workload cases. */
public final class RatePacer {

  private final long intervalNanos;
  private long nextOperation;

  public RatePacer(final int operationsPerSecond) {
    intervalNanos =
        operationsPerSecond == 0
            ? 0
            : Math.max(1, TimeUnit.SECONDS.toNanos(1) / operationsPerSecond);
    nextOperation = System.nanoTime();
  }

  public void beforeOperation() throws InterruptedException {
    if (intervalNanos == 0) {
      return;
    }
    final long remaining = nextOperation - System.nanoTime();
    if (remaining > 0) {
      TimeUnit.NANOSECONDS.sleep(remaining);
    }
    final long now = System.nanoTime();
    nextOperation = Math.max(addSaturated(nextOperation, intervalNanos), now);
  }
}

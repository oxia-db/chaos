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
package io.oxia.chaos.cmd;

import java.time.Duration;
import java.util.Objects;

/** Immutable configuration shared by the selected chaos case. */
public record Options(
    String caseName,
    String serviceAddress,
    Duration duration,
    int keyCount,
    int rate,
    int batchSize,
    Duration checkpointInterval,
    long seed) {

  public static final String BASIC_KV = "basic-kv";
  public static final Duration DEFAULT_DURATION = Duration.ofMinutes(10);
  public static final int DEFAULT_KEY_COUNT = 10_000;
  public static final int DEFAULT_RATE = 258;
  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final Duration DEFAULT_CHECKPOINT_INTERVAL = Duration.ofMinutes(1);

  public Options {
    caseName = requireText(caseName, "--case");
    serviceAddress = requireText(serviceAddress, "--service-address");
    duration = requirePositive(duration, "--duration");
    checkpointInterval = requirePositive(checkpointInterval, "--checkpoint-interval");
    if (!BASIC_KV.equals(caseName)) {
      throw new IllegalArgumentException("unsupported --case: " + caseName);
    }
    if (keyCount <= 0) {
      throw new IllegalArgumentException("--key-count must be greater than zero");
    }
    if (rate < 0) {
      throw new IllegalArgumentException("--rate must be zero or greater");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("--batch-size must be greater than zero");
    }
  }

  public String namespace() {
    return "oc-java-" + caseName;
  }

  private static String requireText(String value, String option) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(option + " requires a value");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String option) {
    Objects.requireNonNull(value, option);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(option + " must be greater than zero");
    }
    try {
      value.toNanos();
    } catch (ArithmeticException error) {
      throw new IllegalArgumentException(option + " is too large", error);
    }
    return value;
  }
}

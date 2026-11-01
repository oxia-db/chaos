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
package io.oxia.chaos.testcase.basickv;

import java.time.Duration;

/** Tuning of the basic key/value case, including its fixed operation distribution. */
final class Constants {

  static final Duration OPERATION_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  static final double PUT_THRESHOLD = 0.20;
  static final double GET_THRESHOLD = PUT_THRESHOLD + 0.15;
  static final double FLOOR_THRESHOLD = GET_THRESHOLD + 0.05;
  static final double CEILING_THRESHOLD = FLOOR_THRESHOLD + 0.05;
  static final double LOWER_THRESHOLD = CEILING_THRESHOLD + 0.05;
  static final double HIGHER_THRESHOLD = LOWER_THRESHOLD + 0.05;
  static final double DELETE_THRESHOLD = HIGHER_THRESHOLD + 0.08;
  static final double DELETE_RANGE_THRESHOLD = DELETE_THRESHOLD + 0.0025;
  static final double TOTAL_WEIGHT = DELETE_RANGE_THRESHOLD + 0.05;

  private Constants() {}
}

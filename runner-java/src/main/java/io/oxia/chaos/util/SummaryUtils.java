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

import java.util.List;

/** Utilities for producing bounded diagnostic summaries. */
public final class SummaryUtils {

  private static final int MAX_DISPLAYED_VALUES = 5;

  private SummaryUtils() {}

  public static String summarize(List<?> values) {
    int displayed = Math.min(values.size(), MAX_DISPLAYED_VALUES);
    return "size=" + values.size() + ", first=" + values.subList(0, displayed);
  }
}

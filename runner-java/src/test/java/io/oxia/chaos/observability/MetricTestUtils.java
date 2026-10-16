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
package io.oxia.chaos.observability;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.Collection;

/** Reads the runner metrics that tests assert on. */
public final class MetricTestUtils {

  private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("operation");
  private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

  private MetricTestUtils() {}

  /** Returns how many operations with the label succeeded. */
  public static long successes(final Collection<MetricData> metrics, final String operation) {
    return metrics.stream()
        .filter((final MetricData metric) -> metric.getName().equals("oxia.chaos.operations"))
        .flatMap((final MetricData metric) -> metric.getLongSumData().getPoints().stream())
        .filter(
            (final LongPointData point) ->
                operation.equals(point.getAttributes().get(OPERATION))
                    && "success".equals(point.getAttributes().get(OUTCOME)))
        .mapToLong(LongPointData::getValue)
        .sum();
  }
}

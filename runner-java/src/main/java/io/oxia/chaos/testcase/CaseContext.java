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
package io.oxia.chaos.testcase;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.obser.Metrics;
import io.oxia.client.api.SyncOxiaClient;
import java.time.Duration;

/** What every case receives from the runner. */
public record CaseContext(
    Options options,
    String runId,
    SyncOxiaClient client,
    OpenTelemetry openTelemetry,
    Metrics metrics,
    Duration retryTimeout) {

  private static final String INSTRUMENTATION_SCOPE_PREFIX = "io.oxia.chaos.";

  /** The tracer of a case, scoped by its name. */
  public Tracer tracer(final String caseName) {
    return openTelemetry.getTracer(INSTRUMENTATION_SCOPE_PREFIX + caseName);
  }
}

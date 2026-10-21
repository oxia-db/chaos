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

import static io.oxia.chaos.util.Timing.elapsedSeconds;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.ops.Checkpoint;
import io.oxia.chaos.ops.Operation;
import java.util.function.IntSupplier;

/** Traces and measures the operations and checkpoints of one case. */
public final class CaseTelemetry {

  private final Tracer tracer;
  private final Metrics metrics;
  private final String caseName;

  public CaseTelemetry(final Tracer tracer, final Metrics metrics, final String caseName) {
    this.tracer = tracer;
    this.metrics = metrics;
    this.caseName = caseName;
  }

  /** Runs an operation in its own root span and records its outcome and duration. */
  public void observe(final Operation operation, final String key, final Runnable action) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String operationLabel = operation.label();
    final Span span =
        tracer
            .spanBuilder(caseName + "." + operationLabel)
            .setNoParent()
            .setAttribute("db.operation.name", operationLabel)
            .setAttribute("db.key", key)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      action.run();
      span.setStatus(StatusCode.OK);
    } catch (final RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordOperation(operationLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }

  /**
   * Runs a checkpoint in its own root span and records its outcome and duration. The check returns
   * the number of keys it verified.
   */
  public void checkpoint(final Checkpoint checkpoint, final IntSupplier check) {
    final long started = System.nanoTime();
    String outcome = "success";
    final String checkpointLabel = checkpoint.label();
    final Span span =
        tracer
            .spanBuilder(caseName + ".checkpoint")
            .setNoParent()
            .setAttribute("checkpoint.kind", checkpointLabel)
            .startSpan();
    try (final Scope ignored = span.makeCurrent()) {
      final int keys = check.getAsInt();
      span.setAttribute("checkpoint.keys", keys).setStatus(StatusCode.OK);
    } catch (final RuntimeException error) {
      outcome = "error";
      span.recordException(error).setStatus(StatusCode.ERROR, String.valueOf(error.getMessage()));
      throw error;
    } finally {
      metrics.recordCheckpoint(checkpointLabel, outcome, elapsedSeconds(started));
      span.end();
    }
  }
}

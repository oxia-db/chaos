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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.oxia.chaos.error.ViolationException;

/** Runs a case inside its run span. */
public final class TestCaseRunner {

  private TestCaseRunner() {}

  /**
   * Starts the case, then closes it. A close failure is suppressed into the failure of the run, so
   * the run's failure is the one reported.
   */
  public static void run(final TestCase testCase, final CaseContext context)
      throws InterruptedException {
    final String name = testCase.name();
    final Span runSpan =
        context
            .tracer(name)
            .spanBuilder(name + ".run")
            .setAttribute("test.case", name)
            .setAttribute("test.run_id", context.runId())
            .setAttribute("test.seed", context.options().seed())
            .setAttribute("test.key_count", context.options().keyCount())
            .startSpan();

    // The case closes before the scope, so its cleanup stays inside the run span
    try (final Scope ignored = runSpan.makeCurrent();
        testCase) {
      testCase.start();
      runSpan.setStatus(StatusCode.OK);
    } catch (final ViolationException error) {
      context.metrics().recordSafetyViolation();
      runSpan.recordException(error).setStatus(StatusCode.ERROR, error.getMessage());
      throw error;
    } catch (final InterruptedException error) {
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "interrupted");
      throw error;
    } catch (final RuntimeException error) {
      runSpan.recordException(error).setStatus(StatusCode.ERROR, "execution failure");
      throw error;
    } finally {
      runSpan.end();
    }
  }
}

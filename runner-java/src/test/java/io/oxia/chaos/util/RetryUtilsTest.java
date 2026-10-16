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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import io.oxia.chaos.error.CorrectnessViolationException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryUtilsTest {

  private static final RetryPolicy<Object> RETRY_POLICY =
      RetryUtils.retryPolicy(Duration.ofSeconds(1));
  private static final Timeout<Object> ATTEMPT_TIMEOUT =
      RetryUtils.attemptTimeout(Duration.ofMillis(50), Duration.ofSeconds(1));

  @Test
  void interruptsAStuckAttemptAndRetriesTheWholeOperation() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicBoolean interrupted = new AtomicBoolean();

    RetryUtils.run(
        RETRY_POLICY,
        ATTEMPT_TIMEOUT,
        () -> {
          if (attempts.incrementAndGet() == 1) {
            try {
              Thread.sleep(Duration.ofMinutes(1));
            } catch (final InterruptedException error) {
              interrupted.set(true);
              Thread.currentThread().interrupt();
            }
          }
        });

    assertThat(interrupted).isTrue();
    assertThat(attempts).hasValue(2);
  }

  @Test
  void reportsAViolationOfAnAttemptThatEndsAfterTheAttemptTimeout() {
    final CorrectnessViolationException violation = new CorrectnessViolationException("mismatch");
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                RetryUtils.run(
                    RETRY_POLICY,
                    ATTEMPT_TIMEOUT,
                    () -> {
                      if (attempts.incrementAndGet() == 1) {
                        // The attempt ignores the interrupt, so it ends after the timeout
                        final long until = System.nanoTime() + Duration.ofMillis(200).toNanos();
                        while (System.nanoTime() < until) {
                          Thread.onSpinWait();
                        }
                        throw violation;
                      }
                    }))
        .isSameAs(violation);
    assertThat(attempts).hasValue(1);
  }

  @Test
  void neverRetriesAViolation() {
    final CorrectnessViolationException violation = new CorrectnessViolationException("mismatch");
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                RetryUtils.run(
                    RETRY_POLICY,
                    ATTEMPT_TIMEOUT,
                    () -> {
                      attempts.incrementAndGet();
                      throw violation;
                    }))
        .isSameAs(violation);
    assertThat(attempts).hasValue(1);
  }
}

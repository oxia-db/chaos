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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import dev.failsafe.TimeoutExceededException;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.client.grpc.OxiaStatusException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded retry and per-attempt timeout policies shared by runner cases. */
public final class RetryUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtils.class);
  private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
  private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(5);

  private RetryUtils() {}

  /** Retries attempt timeouts and retryable Oxia failures until the retry window is exhausted. */
  public static RetryPolicy<Object> retryPolicy(final Duration retryTimeout) {
    return retryPolicy(retryTimeout, (final Throwable error) -> false);
  }

  /**
   * Retries attempt timeouts, retryable Oxia failures, and failures the case marks as retryable
   * until the retry window is exhausted.
   */
  public static RetryPolicy<Object> retryPolicy(
      final Duration retryTimeout, final Predicate<Throwable> retryable) {
    return RetryPolicy.builder()
        .handleIf(
            (final Throwable error) ->
                error instanceof TimeoutExceededException
                    || retryable.test(error)
                    || OxiaExceptionUtils.isRetryable(error))
        .withBackoff(INITIAL_RETRY_DELAY, MAX_RETRY_DELAY)
        .withMaxAttempts(-1)
        .withMaxDuration(retryTimeout)
        .onRetry(
            (final var event) -> {
              final OxiaStatusException error = OxiaExceptionUtils.status(event.getLastException());
              LOGGER
                  .atWarn()
                  .addKeyValue("attempt", event.getAttemptCount())
                  .addKeyValue("status", error.getStatusCode())
                  .log("Retrying current Oxia operation");
            })
        .onRetriesExceeded(
            (final var event) -> {
              final OxiaStatusException error = OxiaExceptionUtils.status(event.getException());
              LOGGER
                  .atError()
                  .addKeyValue("attempts", event.getAttemptCount())
                  .addKeyValue("status", error.getStatusCode())
                  .log("Current Oxia operation exhausted its retry window");
            })
        .build();
  }

  /** Interrupts an attempt that runs longer than the shorter of the two durations. */
  public static Timeout<Object> attemptTimeout(
      final Duration timeout, final Duration retryTimeout) {
    return Timeout.builder(timeout.compareTo(retryTimeout) <= 0 ? timeout : retryTimeout)
        .withInterrupt()
        .build();
  }

  /**
   * Runs an action under a retry policy and an attempt timeout. A correctness violation ends the
   * run: the timeout replaces the outcome of an attempt that ends after it, which the policy would
   * then retry, so the violation is kept aside and thrown instead.
   */
  public static void run(
      final RetryPolicy<Object> retryPolicy,
      final Timeout<Object> attemptTimeout,
      final Runnable action) {
    final AtomicReference<CorrectnessViolationException> violation = new AtomicReference<>();
    try {
      Failsafe.with(retryPolicy, attemptTimeout)
          .run(
              () -> {
                // An attempt runs on the calling thread, so the previous one has already ended
                final CorrectnessViolationException previous = violation.get();
                if (previous != null) {
                  throw previous;
                }
                try {
                  action.run();
                } catch (final CorrectnessViolationException error) {
                  violation.set(error);
                  throw error;
                }
              });
    } catch (final RuntimeException error) {
      final CorrectnessViolationException found = violation.get();
      if (found == null) {
        throw error;
      }
      ExceptionUtils.addSuppressedIfDistinct(found, error);
      throw found;
    }
  }
}

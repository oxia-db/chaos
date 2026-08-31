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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import io.oxia.chaos.ops.BatchType;
import io.oxia.chaos.ops.Operation;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class BasicKvTest {

  @Test
  void followsTheNormalizedBasicKvDistribution() {
    final SplittableRandom random = new SplittableRandom(7);
    final Map<Operation, Integer> counts = new HashMap<>();
    final int samples = 250_000;

    for (int index = 0; index < samples; index++) {
      final Operation operation = BasicKv.selectOperation(random.nextDouble(BasicKv.TOTAL_WEIGHT));
      counts.merge(operation, 1, Integer::sum);
    }

    assertShare(counts, samples, Operation.PUT, 0.2930);
    assertShare(counts, samples, Operation.GET, 0.2198);
    assertShare(counts, samples, Operation.FLOOR, 0.0733);
    assertShare(counts, samples, Operation.CEILING, 0.0733);
    assertShare(counts, samples, Operation.LOWER, 0.0733);
    assertShare(counts, samples, Operation.HIGHER, 0.0733);
    assertShare(counts, samples, Operation.DELETE, 0.1172);
    assertShare(counts, samples, Operation.DELETE_RANGE, 0.0037);
    assertShare(counts, samples, Operation.RANGE_SCAN, 0.0440);
    assertShare(counts, samples, Operation.LIST, 0.0293);
  }

  @Test
  void followsTheNormalizedBatchTypeDistribution() {
    final SplittableRandom random = new SplittableRandom(7);
    final Map<BatchType, Integer> counts = new HashMap<>();
    final int samples = 250_000;

    for (int index = 0; index < samples; index++) {
      final BatchType batchType = BasicKv.selectBatch(random.nextDouble(BasicKv.TOTAL_WEIGHT));
      counts.merge(batchType, 1, Integer::sum);
    }

    assertShare(counts, samples, BatchType.WRITE, 0.4103);
    assertShare(counts, samples, BatchType.READ, 0.5861);
    assertShare(counts, samples, BatchType.DELETE_RANGE, 0.0037);
  }

  @Test
  void executesEveryOperationInABatchConcurrentlyAndWaitsForCompletion() throws Exception {
    final CountDownLatch entered = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    final List<Callable<Void>> operations =
        List.of(operation(entered, release), operation(entered, release));

    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      final Future<?> batch =
          executor.submit(
              () -> {
                BasicKv.executeConcurrentBatch(executor, operations);
                return null;
              });
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(batch).isNotDone();
      } finally {
        release.countDown();
      }
      batch.get();
    }
  }

  @Test
  void preservesASharedBatchFailureWithoutSelfSuppression() {
    final RuntimeException failure = new RuntimeException("shared client failure");
    final List<Callable<Void>> operations =
        List.of(
            () -> {
              throw failure;
            },
            () -> {
              throw failure;
            });

    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      assertThatThrownBy(() -> BasicKv.executeConcurrentBatch(executor, operations))
          .isSameAs(failure);
    }
  }

  @Test
  void interruptsAStuckAttemptAndRetriesTheWholeOperation() {
    final RetryPolicy<Object> retryPolicy = BasicKv.createRetryPolicy(Duration.ofSeconds(1));
    final Timeout<Object> attemptTimeout = BasicKv.createAttemptTimeout(Duration.ofMillis(50));
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicBoolean interrupted = new AtomicBoolean();

    BasicKv.runWithRetry(
        retryPolicy,
        attemptTimeout,
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

  private static <T> void assertShare(
      final Map<T, Integer> counts, final int samples, final T value, final double expected) {
    final double actual = counts.getOrDefault(value, 0) / (double) samples;
    assertThat(actual).isCloseTo(expected, Offset.offset(0.0025));
  }

  private static Callable<Void> operation(
      final CountDownLatch entered, final CountDownLatch release) {
    return () -> {
      entered.countDown();
      assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
      return null;
    };
  }
}

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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BatchUtilsTest {

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
                BatchUtils.executeConcurrentBatch(executor, operations);
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
      assertThatThrownBy(() -> BatchUtils.executeConcurrentBatch(executor, operations))
          .isSameAs(failure);
    }
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

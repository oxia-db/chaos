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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Runs the operations of a batch concurrently, for cases that check them as one batch. */
public final class BatchUtils {

  private BatchUtils() {}

  /** Runs every operation of the batch and waits for all of them, then throws the first failure. */
  public static void executeConcurrentBatch(
      final ExecutorService executor, final List<Callable<Void>> operations)
      throws InterruptedException {
    final List<Future<Void>> batch = executor.invokeAll(operations);
    Throwable failure = null;
    for (final Future<Void> future : batch) {
      try {
        future.get();
      } catch (final ExecutionException error) {
        final Throwable cause = error.getCause() == null ? error : error.getCause();
        if (failure == null) {
          failure = cause;
        } else {
          ExceptionUtils.addSuppressedIfDistinct(failure, cause);
        }
      }
    }

    if (failure instanceof final RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof final Error fatal) {
      throw fatal;
    }
    if (failure != null) {
      throw new IllegalStateException("concurrent operation failed", failure);
    }
  }
}

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

import io.grpc.Status;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Classifies Oxia failures even when an iterable or future wraps the client exception. */
public final class OxiaExceptionUtils {

  private static final Set<Status.Code> RETRYABLE_GRPC_STATUS_CODES =
      Set.of(
          Status.Code.ABORTED,
          Status.Code.CANCELLED,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.RESOURCE_EXHAUSTED,
          Status.Code.UNAVAILABLE);
  private static final List<String> RETRYABLE_MESSAGE_FRAGMENTS =
      List.of("context canceled", "operation was cancelled", "resource is already closed");

  private OxiaExceptionUtils() {}

  public static OxiaStatusException status(final Throwable error) {
    final OxiaStatusException fallback = OxiaStatusException.from(error);
    for (Throwable current = error; current != null; current = current.getCause()) {
      final OxiaStatusException status = OxiaStatusException.from(current);
      if (status.getStatusCode() != OxiaStatusCode.UNKNOWN) {
        return status;
      }
    }
    return fallback;
  }

  public static boolean isRetryable(final Throwable error) {
    if (status(error).isRetryable()) {
      return true;
    }

    if (RETRYABLE_GRPC_STATUS_CODES.contains(Status.fromThrowable(error).getCode())) {
      return true;
    }

    for (Throwable current = error; current != null; current = current.getCause()) {
      final String message = current.getMessage();
      if (current instanceof OxiaStatusException
          && message != null
          && hasRetryableMessage(message)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasRetryableMessage(final String message) {
    final String normalized = message.toLowerCase(Locale.ROOT);
    return RETRYABLE_MESSAGE_FRAGMENTS.stream().anyMatch(normalized::contains);
  }
}

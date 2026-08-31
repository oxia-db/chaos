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

import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;

/** Classifies Oxia failures even when an iterable or future wraps the client exception. */
public final class OxiaExceptionUtils {

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
    return status(error).isRetryable();
  }
}

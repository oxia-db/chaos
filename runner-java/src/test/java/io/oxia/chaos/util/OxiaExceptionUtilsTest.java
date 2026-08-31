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

import io.grpc.Status;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import org.junit.jupiter.api.Test;

class OxiaExceptionUtilsTest {

  @Test
  void classifiesWrappedRetryableStatus() {
    final RuntimeException error =
        new RuntimeException(
            "range scan failed",
            new Exception(OxiaStatusException.resourceUnavailable("leader unavailable")));

    assertThat(OxiaExceptionUtils.isRetryable(error)).isTrue();
    assertThat(OxiaExceptionUtils.status(error).getStatusCode())
        .isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
  }

  @Test
  void leavesUnknownApplicationFailureNonRetryable() {
    final RuntimeException error = new RuntimeException("resource is already closed");

    assertThat(OxiaExceptionUtils.isRetryable(error)).isFalse();
    assertThat(OxiaExceptionUtils.status(error).getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
  }

  @Test
  void classifiesCancelledGrpcFailureAsRetryable() {
    final OxiaStatusException error =
        OxiaStatusException.from(
            Status.CANCELLED.withDescription("context canceled").asRuntimeException());

    assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
    assertThat(OxiaExceptionUtils.isRetryable(error)).isTrue();
  }

  @Test
  void classifiesWrappedClosedResourceFailureAsRetryable() {
    final OxiaStatusException error =
        OxiaStatusException.from(
            Status.UNKNOWN
                .withDescription(
                    "oxia: failed to append to wal: rpc error: "
                        + "code = Code(104) desc = oxia: resource is already closed")
                .asRuntimeException());

    assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
    assertThat(OxiaExceptionUtils.isRetryable(error)).isTrue();
  }
}

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

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.chaos.cmd.Options;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.MemoryInferenceStore;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.ops.BatchType;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Duration;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class BasicKvE2ETest {

  private static final long SEED = 211L;

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void runsBasicKvAgainstThreeShardOxiaAndCleansUpItsKeys() throws Exception {
    assertThat(BasicKv.selectBatch(new SplittableRandom(SEED).nextDouble(BasicKv.TOTAL_WEIGHT)))
        .isEqualTo(BatchType.DELETE_RANGE);

    final String runId = "testcontainers-e2e";
    final String serviceAddress = OXIA.getServiceAddress();
    final Options options =
        new Options(
            Options.BASIC_KV,
            serviceAddress,
            Duration.ofSeconds(3),
            100,
            500,
            100,
            Duration.ofMillis(500),
            SEED);
    final InferenceStore inference = new MemoryInferenceStore();
    final OpenTelemetry openTelemetry = OpenTelemetry.noop();

    try (final RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, Options.BASIC_KV, inference::size);
        final SyncOxiaClient client =
            OxiaClientBuilder.create(serviceAddress).namespace("default").syncClient()) {
      new BasicKv(options, runId, client, openTelemetry, inference, metrics, Duration.ofSeconds(30))
          .run();

      final String runPrefix = "/oxia-chaos/runs/" + runId + "/keys/key-";
      assertThat(inference.size()).isZero();
      assertThat(client.list(runPrefix, runPrefix + "zz")).isEmpty();
    }
  }
}

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
package io.oxia.chaos.cmd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class OxiaChaosRunnerTest {

  @Test
  void parsesTheBasicKvDefaults() {
    Options options = parse("--case=basic-kv", "--service-address=oxia:6648");

    assertThat(options.caseName()).isEqualTo("basic-kv");
    assertThat(options.serviceAddress()).isEqualTo("oxia:6648");
    assertThat(options.namespace()).isEqualTo("oc-java-basic-kv");
    assertThat(options.duration()).isEqualTo(Duration.ofMinutes(10));
    assertThat(options.keyCount()).isEqualTo(10_000);
    assertThat(options.rate()).isEqualTo(258);
    assertThat(options.batchSize()).isEqualTo(100);
    assertThat(options.checkpointInterval()).isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  void parsesWorkloadOverrides() {
    Options options =
        parse(
            "--case=basic-kv",
            "--service-address=oxia:6648",
            "--duration=1500ms",
            "--key-count=100000",
            "--rate=0",
            "--batch-size=25",
            "--checkpoint-interval=30s");

    assertThat(options.duration()).isEqualTo(Duration.ofMillis(1500));
    assertThat(options.keyCount()).isEqualTo(100_000);
    assertThat(options.rate()).isZero();
    assertThat(options.batchSize()).isEqualTo(25);
    assertThat(options.checkpointInterval()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void requiresCaseAndServiceAddress() {
    assertThatThrownBy(() -> parse("--service-address=oxia:6648"))
        .isInstanceOf(CommandLine.MissingParameterException.class)
        .hasMessageContaining("--case");
    assertThatThrownBy(() -> parse("--case=basic-kv"))
        .isInstanceOf(CommandLine.MissingParameterException.class)
        .hasMessageContaining("--service-address");
  }

  @Test
  void rejectsUnknownCasesAndInvalidScale() {
    assertThatThrownBy(() -> parse("--case=unknown", "--service-address=oxia:6648"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported --case");
    assertThatThrownBy(
            () -> parse("--case=basic-kv", "--service-address=oxia:6648", "--key-count=0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--key-count");
  }

  @Test
  void rejectsRemovedNamespaceOptionAndMalformedDurations() {
    assertThatThrownBy(
            () -> parse("--case=basic-kv", "--service-address=oxia:6648", "--namespace=custom"))
        .isInstanceOf(CommandLine.UnmatchedArgumentException.class)
        .hasMessageContaining("--namespace");
    assertThatThrownBy(() -> parse("--case=basic-kv", "--service-address=oxia:6648", "--seed=123"))
        .isInstanceOf(CommandLine.UnmatchedArgumentException.class)
        .hasMessageContaining("--seed");
    assertThatThrownBy(
            () -> parse("--case=basic-kv", "--service-address=oxia:6648", "--duration=10"))
        .isInstanceOf(CommandLine.ParameterException.class)
        .hasMessageContaining("ms, s, m, h");
  }

  @Test
  void runsTheSelectedCaseOnANamedVirtualThread() throws Exception {
    AtomicReference<Thread> caseThread = new AtomicReference<>();

    OxiaChaosRunner.runOnVirtualThread(
        () -> {
          caseThread.set(Thread.currentThread());
          return null;
        });

    assertThat(caseThread.get().isVirtual()).isTrue();
    assertThat(caseThread.get().getName()).startsWith("oxia-chaos-case-");
  }

  private static Options parse(String... arguments) {
    OxiaChaosRunner runner = new OxiaChaosRunner();
    OxiaChaosRunner.commandLine(runner).parseArgs(arguments);
    return runner.options();
  }
}

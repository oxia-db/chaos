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

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.InferenceStore;
import io.oxia.chaos.inference.MemoryInferenceStore;
import io.oxia.chaos.observability.RunnerMetrics;
import io.oxia.chaos.testcase.BasicKv;
import io.oxia.chaos.util.DurationConverter;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/** Command-line entry point for Java Oxia chaos cases. */
@Command(
    name = "oxia-chaos",
    description = "Run correctness cases against Oxia while faults are injected.",
    mixinStandardHelpOptions = true,
    version = "oxia-chaos runner-java 0.1.0")
public final class OxiaChaosRunner implements Callable<Integer> {

  static final int CORRECTNESS_VIOLATION_EXIT_CODE = 3;
  private static final Logger LOGGER = LoggerFactory.getLogger(OxiaChaosRunner.class);
  private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

  @Spec private CommandSpec spec;

  @Option(
      names = "--case",
      required = true,
      paramLabel = "NAME",
      description = "Correctness case to run. Supported: basic-kv.")
  private String caseName;

  @Option(
      names = "--service-address",
      required = true,
      paramLabel = "HOST:PORT",
      description = "Oxia service address.")
  private String serviceAddress;

  @Option(
      names = "--duration",
      defaultValue = "10m",
      converter = DurationConverter.class,
      paramLabel = "DURATION",
      description = "Workload duration after warmup. Default: ${DEFAULT-VALUE}.")
  private Duration duration;

  @Option(
      names = "--key-count",
      defaultValue = "10000",
      paramLabel = "COUNT",
      description = "Number of keys initialized during warmup. Default: ${DEFAULT-VALUE}.")
  private int keyCount;

  @Option(
      names = "--rate",
      defaultValue = "258",
      paramLabel = "OPS_PER_SECOND",
      description = "Target operation rate; zero is unlimited. Default: ${DEFAULT-VALUE}.")
  private int rate;

  @Option(
      names = "--batch-size",
      defaultValue = "100",
      paramLabel = "COUNT",
      description = "Maximum operations per execution cycle. Default: ${DEFAULT-VALUE}.")
  private int batchSize;

  @Option(
      names = "--checkpoint-interval",
      defaultValue = "1m",
      converter = DurationConverter.class,
      paramLabel = "DURATION",
      description = "Full-state checkpoint interval. Default: ${DEFAULT-VALUE}.")
  private Duration checkpointInterval;

  public static void main(String[] args) {
    int exitCode = commandLine(new OxiaChaosRunner()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    final Options options;
    try {
      options = options();
    } catch (IllegalArgumentException error) {
      throw new ParameterException(spec.commandLine(), error.getMessage(), error);
    }

    try {
      return run(options);
    } catch (CorrectnessViolationException error) {
      LOGGER.error("basic-kv correctness violation", error);
      return CORRECTNESS_VIOLATION_EXIT_CODE;
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      LOGGER.error("runner interrupted", error);
      return ExitCode.SOFTWARE;
    } catch (Exception error) {
      LOGGER.error("runner failed", error);
      return ExitCode.SOFTWARE;
    }
  }

  Options options() {
    return new Options(
        caseName,
        serviceAddress,
        duration,
        keyCount,
        rate,
        batchSize,
        checkpointInterval,
        ThreadLocalRandom.current().nextLong());
  }

  private int run(Options options) throws Exception {
    String runId = UUID.randomUUID().toString();
    AutoConfiguredOpenTelemetrySdk configuredTelemetry = configureOpenTelemetry();
    OpenTelemetrySdk openTelemetry = configuredTelemetry.getOpenTelemetrySdk();
    InferenceStore inference = new MemoryInferenceStore();

    LOGGER
        .atInfo()
        .addKeyValue("case", options.caseName())
        .addKeyValue("namespace", options.namespace())
        .addKeyValue("run_id", runId)
        .addKeyValue("seed", options.seed())
        .addKeyValue("duration", options.duration())
        .addKeyValue("key_count", options.keyCount())
        .addKeyValue("rate", options.rate())
        .addKeyValue("batch_size", options.batchSize())
        .addKeyValue("checkpoint_interval", options.checkpointInterval())
        .log("starting chaos case");

    try (openTelemetry;
        RunnerMetrics metrics =
            new RunnerMetrics(openTelemetry, options.caseName(), inference::size);
        SyncOxiaClient client =
            OxiaClientBuilder.create(options.serviceAddress())
                .namespace(options.namespace())
                .clientIdentifier("oxia-chaos-java/" + options.caseName() + "/" + runId)
                .requestTimeout(REQUEST_TIMEOUT)
                .openTelemetry(openTelemetry)
                .syncClient()) {
      switch (options.caseName()) {
        case Options.BASIC_KV ->
            runOnVirtualThread(
                () -> {
                  new BasicKv(
                          options,
                          runId,
                          client,
                          openTelemetry,
                          inference,
                          metrics,
                          REQUEST_TIMEOUT)
                      .run();
                  return null;
                });
        default -> throw new IllegalArgumentException("unsupported case: " + options.caseName());
      }
    }

    LOGGER.atInfo().addKeyValue("run_id", runId).log("chaos case passed");
    return ExitCode.OK;
  }

  static CommandLine commandLine(OxiaChaosRunner runner) {
    return new CommandLine(runner);
  }

  static void runOnVirtualThread(Callable<Void> task) throws Exception {
    Thread.Builder.OfVirtual threads = Thread.ofVirtual().name("oxia-chaos-case-", 0);
    try (var executor = Executors.newThreadPerTaskExecutor(threads.factory())) {
      var future = executor.submit(task);
      try {
        future.get();
      } catch (InterruptedException error) {
        future.cancel(true);
        throw error;
      } catch (ExecutionException error) {
        Throwable cause = error.getCause();
        if (cause instanceof Exception exception) {
          throw exception;
        }
        if (cause instanceof Error fatal) {
          throw fatal;
        }
        throw new IllegalStateException("virtual case thread failed", cause);
      }
    }
  }

  private static AutoConfiguredOpenTelemetrySdk configureOpenTelemetry() {
    java.util.logging.Logger.getLogger("io.opentelemetry.api.GlobalOpenTelemetry")
        .setLevel(Level.WARNING);
    return AutoConfiguredOpenTelemetrySdk.builder()
        .disableShutdownHook()
        .addPropertiesSupplier(
            () ->
                Map.of(
                    "otel.service.name", "oxia-chaos-java",
                    "otel.metrics.exporter", "prometheus",
                    "otel.exporter.prometheus.host", "0.0.0.0",
                    "otel.exporter.prometheus.port", "9464",
                    "otel.traces.exporter", "none",
                    "otel.logs.exporter", "none"))
        .build();
  }
}

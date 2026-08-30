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
package io.oxia.chaos.runner;

import io.oxia.chaos.register.RegisterValue;
import io.oxia.chaos.register.VersionedRegister;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.OxiaException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

/** Bootstrap entry point for the Java chaos runner. */
public final class OxiaChaosRunner {

  private OxiaChaosRunner() {}

  public static void main(String[] args) throws Exception {
    RunnerOptions options = RunnerOptions.parse(args);
    String key = "/oxia-chaos/bootstrap/" + UUID.randomUUID();

    try (SyncOxiaClient client =
        OxiaClientBuilder.create(options.serviceAddress())
            .namespace(options.namespace())
            .requestTimeout(Duration.ofSeconds(10))
            .syncClient()) {
      try {
        runBootstrapRegisterCheck(new VersionedRegister(client), key);
        System.out.printf("versioned-register bootstrap passed: key=%s%n", key);
      } finally {
        client.delete(key);
      }
    }
  }

  static void runBootstrapRegisterCheck(VersionedRegister register, String key)
      throws OxiaException {
    byte[] initialValue = "initial".getBytes(StandardCharsets.UTF_8);
    long createdVersion = register.create(key, initialValue);

    RegisterValue created =
        register.read(key).orElseThrow(() -> new IllegalStateException("created key is missing"));
    requireValueAndVersion(created, initialValue, createdVersion);

    byte[] updatedValue = "updated".getBytes(StandardCharsets.UTF_8);
    long updatedVersion = register.compareAndSet(key, created.versionId(), updatedValue);
    RegisterValue updated =
        register.read(key).orElseThrow(() -> new IllegalStateException("updated key is missing"));
    requireValueAndVersion(updated, updatedValue, updatedVersion);
  }

  private static void requireValueAndVersion(
      RegisterValue actual, byte[] expectedValue, long expectedVersion) {
    if (!Arrays.equals(actual.value(), expectedValue) || actual.versionId() != expectedVersion) {
      throw new IllegalStateException(
          "register value or version does not match acknowledged write");
    }
  }

  record RunnerOptions(String serviceAddress, String namespace) {

    static RunnerOptions parse(String[] args) {
      String serviceAddress = null;
      String namespace = "default";
      for (String arg : args) {
        if (arg.startsWith("--service-address=")) {
          serviceAddress = valueOf(arg, "--service-address=");
        } else if (arg.startsWith("--namespace=")) {
          namespace = valueOf(arg, "--namespace=");
        } else {
          throw new IllegalArgumentException("unknown argument: " + arg);
        }
      }
      if (serviceAddress == null || serviceAddress.isBlank()) {
        throw new IllegalArgumentException("--service-address=<host:port> is required");
      }
      return new RunnerOptions(serviceAddress, namespace);
    }

    private static String valueOf(String argument, String prefix) {
      String value = argument.substring(prefix.length());
      if (value.isBlank()) {
        throw new IllegalArgumentException(prefix + " requires a value");
      }
      return value;
    }
  }
}

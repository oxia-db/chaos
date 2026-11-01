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

import java.util.Map;

/**
 * A correctness case. {@link TestCaseRunner} runs the phases in order and always shuts the case
 * down, whether or not an earlier phase failed.
 */
public interface TestCase {

  /** The case name, as accepted by the runner's case option. */
  String name();

  /** Prepares the state the workload starts from, such as warmup keys or subscriptions. */
  void bootstrap() throws InterruptedException;

  /** Runs the timed workload, including its periodic checkpoints. */
  void execute() throws InterruptedException;

  /** Checks the final state once the workload ended. */
  void verify() throws InterruptedException;

  /** Releases what the case holds and deletes what it wrote. */
  void shutdown();

  /** Counters logged when the case completes. */
  Map<String, Object> summary();
}

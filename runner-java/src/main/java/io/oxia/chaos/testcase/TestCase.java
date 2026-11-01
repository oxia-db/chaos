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

/**
 * A correctness case. {@link TestCaseRunner} starts it and always closes it, whether or not it
 * failed.
 */
public interface TestCase extends AutoCloseable {

  /** The case name, as accepted by the runner's case option. */
  String name();

  /** Runs the case to its end: warmup, the timed workload and the final check. */
  void start() throws InterruptedException;

  /** Releases what the case holds and deletes what it wrote. */
  @Override
  void close();
}

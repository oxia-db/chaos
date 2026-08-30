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

import java.util.Locale;

/** Generates run-scoped keys and ordered guard boundaries for correctness workloads. */
public final class KeyGenerator {

  private final String prefix;

  public KeyGenerator(final String runId) {
    this.prefix = "/oxia-chaos/runs/" + runId + "/keys/key-";
  }

  public String key(final int index) {
    return prefix + String.format(Locale.ROOT, "%08d", index);
  }

  public String lowerGuardKey() {
    return prefix + "-guard";
  }

  public String upperGuardKey() {
    return prefix + "z-guard";
  }

  public String afterUpperGuardKey() {
    return prefix + "zz";
  }
}

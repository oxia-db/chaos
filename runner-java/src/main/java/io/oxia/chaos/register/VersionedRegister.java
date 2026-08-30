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
package io.oxia.chaos.register;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.options.PutOption;
import java.util.Optional;
import java.util.Set;

/** Oxia operations used by the versioned-register chaos workload. */
public final class VersionedRegister {

  private final SyncOxiaClient client;

  public VersionedRegister(SyncOxiaClient client) {
    this.client = client;
  }

  public Optional<RegisterValue> read(String key) {
    GetResult result = client.get(key);
    if (result == null) {
      return Optional.empty();
    }
    return Optional.of(new RegisterValue(result.value(), result.version().versionId()));
  }

  public long create(String key, byte[] value) throws OxiaException {
    PutResult result = client.put(key, value, Set.of(PutOption.IfRecordDoesNotExist));
    return result.version().versionId();
  }

  public long compareAndSet(String key, long expectedVersionId, byte[] value) throws OxiaException {
    PutResult result =
        client.put(key, value, Set.of(PutOption.IfVersionIdEquals(expectedVersionId)));
    return result.version().versionId();
  }
}

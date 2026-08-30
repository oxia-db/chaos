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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class VersionedRegisterTest {

  @Mock SyncOxiaClient client;

  private VersionedRegister register;

  @BeforeEach
  void setUp() {
    register = new VersionedRegister(client);
  }

  @Test
  void returnsEmptyWhenKeyDoesNotExist() {
    when(client.get("key")).thenReturn(null);

    assertThat(register.read("key")).isEmpty();
  }

  @Test
  void returnsValueAndVersion() {
    when(client.get("key")).thenReturn(new GetResult("key", bytes("one"), version(7)));

    assertThat(register.read("key")).contains(new RegisterValue(bytes("one"), 7));
  }

  @Test
  void createsOnlyWhenRecordDoesNotExist() throws Exception {
    when(client.put(eq("key"), eq(bytes("one")), any()))
        .thenReturn(new PutResult("key", version(4)));

    assertThat(register.create("key", bytes("one"))).isEqualTo(4);

    ArgumentCaptor<Set<PutOption>> options = putOptionsCaptor();
    verify(client).put(eq("key"), eq(bytes("one")), options.capture());
    assertThat(options.getValue()).containsExactly(PutOption.IfRecordDoesNotExist);
  }

  @Test
  void updatesOnlyAtExpectedVersion() throws Exception {
    when(client.put(eq("key"), eq(bytes("two")), any()))
        .thenReturn(new PutResult("key", version(9)));

    assertThat(register.compareAndSet("key", 7, bytes("two"))).isEqualTo(9);

    ArgumentCaptor<Set<PutOption>> options = putOptionsCaptor();
    verify(client).put(eq("key"), eq(bytes("two")), options.capture());
    assertThat(options.getValue())
        .singleElement()
        .isEqualTo(new OptionVersionId.OptionVersionIdEqual(7));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ArgumentCaptor<Set<PutOption>> putOptionsCaptor() {
    return (ArgumentCaptor) ArgumentCaptor.forClass(Set.class);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(UTF_8);
  }

  private static Version version(long versionId) {
    return new Version(versionId, 0, 0, 0, Optional.empty(), Optional.empty());
  }
}

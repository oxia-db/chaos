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

import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Pins the Java client's ephemeral-record semantics that the ephemeral case relies on. */
@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
class EphemeralClientE2ETest {

  private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(5);
  private static final Set<PutOption> EPHEMERAL = Set.of(PutOption.AsEphemeralRecord);

  @Container
  private static final OxiaContainer OXIA =
      new OxiaContainer(DockerImageName.parse("oxia/oxia:0.16.8"), 3);

  @Test
  void reportsTheOwningSessionAndClientIdentifier() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient owner = owner("owner-a")) {
      final PutResult put = owner.put(prefix + "a", value("v1"), EPHEMERAL);
      final GetResult get = owner.get(prefix + "a");

      assertThat(put.version().sessionId()).isPresent();
      assertThat(put.version().clientIdentifier()).contains("owner-a");
      assertThat(get.version().sessionId()).isEqualTo(put.version().sessionId());
      assertThat(get.version().clientIdentifier()).contains("owner-a");
    }
  }

  @Test
  void keepsTheSessionAcrossUpdatesFromTheSameOwner() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient owner = owner("owner-a")) {
      final PutResult first = owner.put(prefix + "a", value("v1"), EPHEMERAL);
      final PutResult second = owner.put(prefix + "a", value("v2"), EPHEMERAL);

      assertThat(second.version().sessionId()).isEqualTo(first.version().sessionId());
      assertThat(owner.get(prefix + "a").value()).isEqualTo(value("v2"));
    }
  }

  @Test
  void removesOnlyTheClosedOwnersRecords() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient observer = observer();
        final SyncOxiaClient survivor = owner("owner-b")) {
      try (final SyncOxiaClient closed = owner("owner-a")) {
        for (int index = 0; index < 10; index++) {
          closed.put(prefix + "a-" + index, value("a"), EPHEMERAL);
          survivor.put(prefix + "b-" + index, value("b"), EPHEMERAL);
        }
        assertThat(observer.list(prefix, prefix + "~")).hasSize(20);
      }

      awaitKeyCount(observer, prefix, 10);
      assertThat(observer.list(prefix, prefix + "~"))
          .allMatch(key -> key.startsWith(prefix + "b-"));
    }
  }

  @Test
  void transfersOwnershipWhenAnotherOwnerOverwritesTheRecord() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient observer = observer();
        final SyncOxiaClient successor = owner("owner-b")) {
      final PutResult original;
      final PutResult taken;
      try (final SyncOxiaClient predecessor = owner("owner-a")) {
        original = predecessor.put(prefix + "a", value("v1"), EPHEMERAL);
        taken = successor.put(prefix + "a", value("v2"), EPHEMERAL);
      }

      assertThat(taken.version().sessionId()).isNotEqualTo(original.version().sessionId());
      // The predecessor's close must not delete a record it no longer owns.
      final GetResult afterClose = observer.get(prefix + "a");
      assertThat(afterClose).isNotNull();
      assertThat(afterClose.value()).isEqualTo(value("v2"));
      assertThat(afterClose.version().clientIdentifier()).contains("owner-b");
    }
  }

  @Test
  void keepsARecordThatWasOverwrittenAsPersistent() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient observer = observer()) {
      try (final SyncOxiaClient owner = owner("owner-a")) {
        owner.put(prefix + "a", value("v1"), EPHEMERAL);
        final PutResult persistent = observer.put(prefix + "a", value("v2"));
        assertThat(persistent.version().sessionId()).isEmpty();
      }

      final GetResult afterClose = observer.get(prefix + "a");
      assertThat(afterClose).isNotNull();
      assertThat(afterClose.value()).isEqualTo(value("v2"));
      observer.delete(prefix + "a");
    }
  }

  @Test
  void expiresACrashedOwnersRecordsOnlyAfterTheSessionTimeout() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient observer = observer();
        final OwnerProcess owner = ownerProcess("crashed-owner")) {
      for (int index = 0; index < 10; index++) {
        assertThat(owner.put(prefix + index, value("v")).sessionId()).isPresent();
      }

      final long killed = System.nanoTime();
      owner.crash();
      assertThat(observer.list(prefix, prefix + "~")).hasSize(10);

      awaitKeyCount(observer, prefix, 0);
      final Duration expiredAfter = Duration.ofNanos(System.nanoTime() - killed);
      assertThat(observer.list(prefix, prefix + "~")).isEmpty();
      assertThat(expiredAfter).isGreaterThanOrEqualTo(SESSION_TIMEOUT.dividedBy(2));
    }
  }

  @Test
  void deletesAGracefullyClosedOwnersRecordsBeforeTheSessionTimeout() throws Exception {
    final String prefix = prefix();
    try (final SyncOxiaClient observer = observer();
        final OwnerProcess owner = ownerProcess("closed-owner")) {
      for (int index = 0; index < 10; index++) {
        owner.put(prefix + index, value("v"));
      }

      final long closed = System.nanoTime();
      assertThat(owner.closeGracefully(Duration.ofSeconds(30))).isTrue();

      awaitKeyCount(observer, prefix, 0);
      final Duration removedAfter = Duration.ofNanos(System.nanoTime() - closed);
      assertThat(observer.list(prefix, prefix + "~")).isEmpty();
      assertThat(removedAfter).isLessThan(SESSION_TIMEOUT.dividedBy(2));
    }
  }

  private static OwnerProcess ownerProcess(final String identifier) {
    return OwnerProcess.start(
        OXIA.getServiceAddress(), "default", identifier, SESSION_TIMEOUT, Duration.ofSeconds(10));
  }

  private static SyncOxiaClient owner(final String identifier) throws Exception {
    return OxiaClientBuilder.create(OXIA.getServiceAddress())
        .namespace("default")
        .clientIdentifier(identifier)
        .sessionTimeout(SESSION_TIMEOUT)
        .syncClient();
  }

  private static SyncOxiaClient observer() throws Exception {
    return OxiaClientBuilder.create(OXIA.getServiceAddress()).namespace("default").syncClient();
  }

  private static void awaitKeyCount(
      final SyncOxiaClient observer, final String prefix, final int expected)
      throws InterruptedException {
    final long deadline = System.nanoTime() + SESSION_TIMEOUT.multipliedBy(3).toNanos();
    while (observer.list(prefix, prefix + "~").size() != expected && System.nanoTime() < deadline) {
      Thread.sleep(100);
    }
  }

  private static String prefix() {
    return "/ephemeral-client/" + UUID.randomUUID() + "/";
  }

  private static byte[] value(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}

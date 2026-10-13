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
package io.oxia.chaos.testcase.ephemeral;

import static io.oxia.chaos.testcase.ephemeral.Ephemeral.DrainVerdict.DRAINED;
import static io.oxia.chaos.testcase.ephemeral.Ephemeral.DrainVerdict.EARLY_EXPIRY;
import static io.oxia.chaos.testcase.ephemeral.Ephemeral.DrainVerdict.LEAKED;
import static io.oxia.chaos.testcase.ephemeral.Ephemeral.DrainVerdict.WAITING;
import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.chaos.inference.EphemeralRecord;
import io.oxia.chaos.ops.Operation;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnedValue;
import io.oxia.chaos.testcase.ephemeral.Owner.OwnedVersion;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class EphemeralTest {

  private static final Ephemeral.Settings SETTINGS =
      new Ephemeral.Settings(
          Duration.ofSeconds(30),
          Duration.ofSeconds(10),
          Duration.ofMinutes(2),
          Duration.ofSeconds(30));

  @Test
  void derivesTheExpiryWindowFromTheSessionTimeout() {
    assertThat(SETTINGS.earliestExpiry()).isEqualTo(Duration.ofSeconds(15));
    assertThat(SETTINGS.expiryDeadline()).isEqualTo(Duration.ofSeconds(90));
    assertThat(Ephemeral.Settings.DEFAULT).isEqualTo(SETTINGS);
  }

  @Test
  void derivesOwnerTimeoutsFromTheRequestTimeout() {
    assertThat(SETTINGS.startTimeout()).isEqualTo(Duration.ofMinutes(1));
    assertThat(SETTINGS.replyTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(SETTINGS.closeTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void checksEarlyExpiryOnlyForCrashesWithoutARecentFault() {
    final Duration timeout = Duration.ofSeconds(30);

    assertThat(Ephemeral.checksEarlyExpiry(Operation.OWNER_CRASH, Optional.empty(), timeout))
        .isTrue();
    assertThat(
            Ephemeral.checksEarlyExpiry(
                Operation.OWNER_CRASH, Optional.of(Duration.ofSeconds(31)), timeout))
        .isTrue();
    assertThat(
            Ephemeral.checksEarlyExpiry(
                Operation.OWNER_CRASH, Optional.of(Duration.ofSeconds(29)), timeout))
        .isFalse();
    assertThat(Ephemeral.checksEarlyExpiry(Operation.OWNER_CLOSE, Optional.empty(), timeout))
        .isFalse();
    assertThat(Ephemeral.checksEarlyExpiry(Operation.OWNER_LOST, Optional.empty(), timeout))
        .isFalse();
  }

  @Test
  void splitsTheKeyCountExactlyAcrossSlots() {
    assertThat(Ephemeral.slotKeyCount(1001, 2, 0)).isEqualTo(501);
    assertThat(Ephemeral.slotKeyCount(1001, 2, 1)).isEqualTo(500);
    assertThat(Ephemeral.slotKeyCount(2, 2, 0)).isOne();
    assertThat(Ephemeral.slotKeyCount(2, 2, 1)).isOne();
  }

  @Test
  void followsTheEphemeralDistribution() {
    final SplittableRandom random = new SplittableRandom(7);
    final Map<Operation, Integer> counts = new HashMap<>();
    final int samples = 250_000;

    for (int index = 0; index < samples; index++) {
      counts.merge(
          Ephemeral.selectOperation(random.nextDouble(Ephemeral.TOTAL_WEIGHT)), 1, Integer::sum);
    }

    assertThat(counts)
        .containsOnlyKeys(
            Operation.EPHEMERAL_PUT, Operation.OWNER_GET, Operation.GET, Operation.DELETE);
    assertShare(counts, samples, Operation.EPHEMERAL_PUT, 0.45);
    assertShare(counts, samples, Operation.OWNER_GET, 0.20);
    assertShare(counts, samples, Operation.GET, 0.25);
    assertShare(counts, samples, Operation.DELETE, 0.10);
  }

  @Test
  void flagsACrashedOwnersRecordsVanishingBeforeHalfTheSessionTimeout() {
    assertThat(crash(10, 9, seconds(13), seconds(14))).isEqualTo(EARLY_EXPIRY);
    assertThat(crash(10, 0, seconds(1), seconds(2))).isEqualTo(EARLY_EXPIRY);
  }

  @Test
  void acceptsACrashedOwnersRecordsExpiringInsideTheWindow() {
    assertThat(crash(10, 10, seconds(0), seconds(1))).isEqualTo(WAITING);
    assertThat(crash(10, 4, seconds(20), seconds(21))).isEqualTo(WAITING);
    assertThat(crash(10, 0, seconds(31), seconds(32))).isEqualTo(DRAINED);
    // The scan that ended at 15s cannot prove the records vanished before the boundary.
    assertThat(crash(10, 0, seconds(14), seconds(15))).isEqualTo(DRAINED);
  }

  @Test
  void flagsRecordsThatOutliveTheExpiryDeadline() {
    assertThat(crash(10, 1, seconds(91), seconds(92))).isEqualTo(LEAKED);
    assertThat(close(10, 10, seconds(91), seconds(92))).isEqualTo(LEAKED);
    // A scan that started before the deadline may legitimately still see the records.
    assertThat(crash(10, 1, seconds(89), seconds(95))).isEqualTo(WAITING);
  }

  @Test
  void allowsAGracefullyClosedOwnersRecordsToVanishImmediately() {
    assertThat(close(10, 0, seconds(0), seconds(1))).isEqualTo(DRAINED);
    assertThat(close(10, 3, seconds(0), seconds(1))).isEqualTo(WAITING);
  }

  @Test
  void matchesOnlyTheSameValueSessionAndOwner() {
    final EphemeralRecord expected = new EphemeralRecord("a/1", bytes("v"), "owner-a", 7);

    assertThat(Ephemeral.matches(expected, observed("v", 7L, "owner-a"))).isTrue();
    assertThat(Ephemeral.matches(expected, observed("x", 7L, "owner-a"))).isFalse();
    assertThat(Ephemeral.matches(expected, observed("v", 8L, "owner-a"))).isFalse();
    assertThat(Ephemeral.matches(expected, observed("v", 7L, "owner-b"))).isFalse();
    assertThat(Ephemeral.matches(expected, observed("v", null, "owner-a"))).isFalse();
  }

  private static Ephemeral.DrainVerdict crash(
      final int expected, final int present, final Duration started, final Duration ended) {
    return Ephemeral.judgeDrain(SETTINGS, true, expected, present, started, ended);
  }

  private static Ephemeral.DrainVerdict close(
      final int expected, final int present, final Duration started, final Duration ended) {
    return Ephemeral.judgeDrain(SETTINGS, false, expected, present, started, ended);
  }

  private static Duration seconds(final long seconds) {
    return Duration.ofSeconds(seconds);
  }

  private static OwnedValue observed(final String value, final Long sessionId, final String owner) {
    return new OwnedValue(
        bytes(value), new OwnedVersion(Optional.ofNullable(sessionId), Optional.of(owner)));
  }

  private static byte[] bytes(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static void assertShare(
      final Map<Operation, Integer> counts,
      final int samples,
      final Operation operation,
      final double expected) {
    assertThat(counts.getOrDefault(operation, 0) / (double) samples)
        .isCloseTo(expected, Offset.offset(0.005));
  }
}

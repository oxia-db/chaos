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
package io.oxia.chaos.inference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.NotificationChecker.Covered;
import io.oxia.chaos.inference.NotificationChecker.Delete;
import io.oxia.chaos.inference.NotificationChecker.Match;
import io.oxia.chaos.inference.NotificationChecker.Observed;
import io.oxia.chaos.inference.NotificationChecker.Put;
import io.oxia.chaos.inference.NotificationChecker.Write;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.api.Notification.KeyRangeDelete;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NotificationCheckerTest {

  private static final String PREFIX = "/run/key-";
  private static final String KEY = PREFIX + "00000000";
  private static final String OTHER = PREFIX + "00000001";
  private static final Match EXISTING = new Match(true, 10, 0, Optional.empty());
  private static final Match ABSENT = new Match(false, 10, 0, Optional.empty());

  @Test
  void matchesWritesNotifiedInOrder() {
    final Match match =
        match(
            EXISTING,
            List.of(new Put(12, 1, 1), new Delete(true, 1), new Put(15, 0, 1)),
            new KeyModified(KEY, 12),
            new KeyDeleted(KEY),
            new KeyCreated(KEY, 15));

    assertThat(match).isEqualTo(new Match(true, 15, 3, Optional.empty()));
  }

  @Test
  void reportsTheFirstMissingNotification() {
    final Match match =
        match(EXISTING, List.of(new Put(12, 1, 1), new Put(13, 2, 1)), new KeyModified(KEY, 12));

    assertThat(match.missing()).contains("the notification of version 13");
  }

  @Test
  void rejectsDuplicatedReorderedAndExtraNotifications() {
    final List<Write> writes = List.of(new Put(12, 1, 1), new Put(13, 2, 1));

    assertViolation(
        EXISTING,
        writes,
        new KeyModified(KEY, 12),
        new KeyModified(KEY, 12),
        new KeyModified(KEY, 13));
    assertViolation(EXISTING, writes, new KeyModified(KEY, 13), new KeyModified(KEY, 12));
    assertViolation(
        EXISTING, writes, new KeyModified(KEY, 12), new KeyModified(KEY, 13), new KeyDeleted(KEY));
  }

  @Test
  void rejectsAGapEvenWhenALaterVersionArrives() {
    assertViolation(
        EXISTING, List.of(new Put(12, 1, 1), new Put(13, 2, 1)), new KeyModified(KEY, 13));
  }

  @Test
  void rejectsTheWrongTypeOrVersion() {
    assertViolation(EXISTING, List.of(new Put(12, 1, 1)), new KeyCreated(KEY, 12));
    assertViolation(EXISTING, List.of(new Put(12, 1, 1)), new KeyModified(KEY, 11));
    assertViolation(EXISTING, List.of(new Delete(true, 1)), new KeyModified(KEY, 12));
  }

  @Test
  void acceptsTheNotificationsOfFailedAttemptsOnlyUpToTheirCount() {
    final List<Write> retried = List.of(new Put(13, 2, 2));

    assertThat(match(EXISTING, retried, new KeyModified(KEY, 13)).exists()).isTrue();
    assertThat(match(EXISTING, retried, new KeyModified(KEY, 12), new KeyModified(KEY, 13)))
        .isEqualTo(new Match(true, 13, 2, Optional.empty()));
    assertViolation(
        EXISTING,
        retried,
        new KeyModified(KEY, 11),
        new KeyModified(KEY, 12),
        new KeyModified(KEY, 13));
    assertViolation(
        EXISTING, List.of(new Put(13, 2, 1)), new KeyModified(KEY, 12), new KeyModified(KEY, 13));
  }

  @Test
  void aFailedPutThatCreatedTheKeyTurnsTheRetryIntoAModification() {
    assertThat(match(ABSENT, List.of(new Put(13, 1, 2)), new KeyCreated(KEY, 12), modified(13)))
        .isEqualTo(new Match(true, 13, 2, Optional.empty()));
    assertViolation(
        ABSENT, List.of(new Put(13, 1, 2)), new KeyCreated(KEY, 12), new KeyCreated(KEY, 13));
  }

  @Test
  void acceptsADeleteRetriedAfterAFailedAttemptDeletedTheKey() {
    assertThat(match(EXISTING, List.of(new Delete(false, 2)), new KeyDeleted(KEY)))
        .isEqualTo(new Match(false, 10, 1, Optional.empty()));
    assertThat(match(EXISTING, List.of(new Delete(false, 2))).missing()).isPresent();
    assertViolation(EXISTING, List.of(new Delete(false, 1)));
    assertViolation(ABSENT, List.of(new Delete(true, 1)), new KeyDeleted(KEY));
    assertThat(match(ABSENT, List.of(new Delete(false, 1))).exists()).isFalse();
  }

  @Test
  void aRangeDeleteMustBeNotifiedBetweenTheKeyNotificationsAroundIt() {
    // The key's notifications arrive 1st and 3rd: the range delete one arrives 2nd
    final List<Write> writes =
        List.of(new Put(12, 1, 1), new Covered("[a, b)", List.of(2L)), new Put(14, 0, 1));
    assertThat(
            NotificationChecker.match(
                KEY,
                EXISTING,
                writes,
                List.of(observed(modified(12), 1), observed(created(14), 3))))
        .isEqualTo(new Match(true, 14, 3, Optional.empty()));

    // Its only notification arrived before the key's previous one, or after its next one
    final List<Observed> around = List.of(observed(modified(12), 3), observed(created(14), 5));
    assertThatThrownBy(
            () ->
                NotificationChecker.match(
                    KEY,
                    EXISTING,
                    List.of(
                        new Put(12, 1, 1), new Covered("[a, b)", List.of(2L)), new Put(14, 0, 1)),
                    around))
        .isInstanceOf(CorrectnessViolationException.class);
    assertThatThrownBy(
            () ->
                NotificationChecker.match(
                    KEY,
                    EXISTING,
                    List.of(
                        new Put(12, 1, 1), new Covered("[a, b)", List.of(6L)), new Put(14, 0, 1)),
                    around))
        .isInstanceOf(CorrectnessViolationException.class);
  }

  @Test
  void waitsForARangeDeleteNotificationAfterTheKeyLastOne() {
    final List<Write> writes = List.of(new Put(12, 1, 1), new Covered("[a, b)", List.of(1L)));
    final List<Observed> observed = List.of(observed(modified(12), 2));

    assertThat(NotificationChecker.match(KEY, EXISTING, writes, observed).missing())
        .contains("a notification of the range delete [a, b)");
    assertThat(
            NotificationChecker.match(
                KEY,
                EXISTING,
                List.of(new Put(12, 1, 1), new Covered("[a, b)", List.of(1L, 3L))),
                observed))
        .isEqualTo(new Match(false, 12, 3, Optional.empty()));
  }

  @Test
  void aRangeDeleteRemovesTheKeyWithoutAKeyNotification() {
    assertViolation(EXISTING, List.of(new Covered("[a, b)", List.of(1L))), new KeyDeleted(KEY));
  }

  @Test
  void checksEveryKeyAndRangeOnceTheSubscriptionStarted() {
    final NotificationChecker checker = new NotificationChecker(PREFIX, List.of(KEY, OTHER));
    checker.accept(new KeyCreated(KEY, 3));
    checker.accept(new KeyCreated(OTHER, 4));
    assertThat(checker.observed(KEY, 3)).isTrue();
    assertThat(checker.observed(OTHER, 5)).isFalse();
    checker.start(Map.of(KEY, 3L, OTHER, 4L));

    checker.put(KEY, 7, 1, 1);
    final String end = OTHER + "~0";
    checker.registerRange(OTHER, end);
    checker.rangeDeleted(OTHER, end);
    checker.accept(new KeyModified(KEY, 7));
    checker.accept(new KeyModified("/elsewhere", 9));
    assertThat(checker.check())
        .containsExactlyInAnyOrder(
            "[" + OTHER + ", " + end + "): a range delete notification",
            OTHER + ": a notification of the range delete [" + OTHER + ", " + end + ")");

    checker.accept(new KeyRangeDelete(OTHER, end));
    checker.accept(new KeyRangeDelete(OTHER, end));
    assertThat(checker.check()).isEmpty();
    assertThat(checker.version(KEY)).contains(7L);
    assertThat(checker.version(OTHER)).isEmpty();
  }

  @Test
  void rejectsARangeDeleteNotifiedOnlyAfterTheKeyNextWrite() {
    final NotificationChecker checker = new NotificationChecker(PREFIX, List.of(KEY));
    checker.accept(new KeyCreated(KEY, 3));
    checker.start(Map.of(KEY, 3L));

    final String end = KEY + "~0";
    checker.registerRange(KEY, end);
    checker.rangeDeleted(KEY, end);
    checker.put(KEY, 5, 0, 1);
    // The key's shard notified the put that follows the range delete before the range delete
    checker.accept(new KeyCreated(KEY, 5));
    checker.accept(new KeyRangeDelete(KEY, end));
    assertThatThrownBy(checker::check).isInstanceOf(CorrectnessViolationException.class);
  }

  @Test
  void rejectsNotificationsOfKeysAndRangesTheCaseNeverWrote() {
    final NotificationChecker keys = new NotificationChecker(PREFIX, List.of(KEY));
    keys.start(Map.of(KEY, 3L));
    keys.accept(new KeyCreated(PREFIX + "unknown", 5));
    assertThatThrownBy(keys::check).isInstanceOf(CorrectnessViolationException.class);

    final NotificationChecker ranges = new NotificationChecker(PREFIX, List.of(KEY));
    ranges.start(Map.of(KEY, 3L));
    ranges.accept(new KeyRangeDelete(KEY, KEY + "~1"));
    assertThatThrownBy(ranges::check).isInstanceOf(CorrectnessViolationException.class);
  }

  private static KeyCreated created(final long versionId) {
    return new KeyCreated(KEY, versionId);
  }

  private static KeyModified modified(final long versionId) {
    return new KeyModified(KEY, versionId);
  }

  private static Observed observed(final Notification notification, final long arrival) {
    return new Observed(notification, arrival);
  }

  /** Matches notifications that arrived 1st, 2nd, and so on. */
  private static Match match(
      final Match previous, final List<Write> writes, final Notification... notifications) {
    final List<Observed> observed = new ArrayList<>();
    for (final Notification notification : notifications) {
      observed.add(new Observed(notification, observed.size() + 1));
    }
    return NotificationChecker.match(KEY, previous, writes, observed);
  }

  private static void assertViolation(
      final Match previous, final List<Write> writes, final Notification... notifications) {
    assertThatThrownBy(() -> match(previous, writes, notifications))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("key=" + KEY);
  }
}

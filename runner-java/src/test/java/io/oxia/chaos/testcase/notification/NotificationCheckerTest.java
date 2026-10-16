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
package io.oxia.chaos.testcase.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.testcase.notification.NotificationChecker.Covered;
import io.oxia.chaos.testcase.notification.NotificationChecker.Delete;
import io.oxia.chaos.testcase.notification.NotificationChecker.Match;
import io.oxia.chaos.testcase.notification.NotificationChecker.Put;
import io.oxia.chaos.testcase.notification.NotificationChecker.Write;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.api.Notification.KeyRangeDelete;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NotificationCheckerTest {

  private static final String PREFIX = "/run/key-";
  private static final String KEY = PREFIX + "00000000";
  private static final String OTHER = PREFIX + "00000001";

  @Test
  void matchesWritesNotifiedInOrder() {
    final Match match =
        match(
            true,
            10,
            List.of(new Put(12, 1, 1), new Delete(true, 1), new Put(15, 0, 1)),
            List.of(new KeyModified(KEY, 12), new KeyDeleted(KEY), new KeyCreated(KEY, 15)));

    assertThat(match).isEqualTo(new Match(true, 15, Optional.empty()));
  }

  @Test
  void reportsTheFirstMissingNotification() {
    final Match match =
        match(
            true,
            10,
            List.of(new Put(12, 1, 1), new Put(13, 2, 1)),
            List.of(new KeyModified(KEY, 12)));

    assertThat(match.missing()).contains("the notification of version 13");
  }

  @Test
  void rejectsDuplicatedReorderedAndExtraNotifications() {
    final List<Write> writes = List.of(new Put(12, 1, 1), new Put(13, 2, 1));

    assertViolation(
        writes,
        List.of(new KeyModified(KEY, 12), new KeyModified(KEY, 12), new KeyModified(KEY, 13)));
    assertViolation(writes, List.of(new KeyModified(KEY, 13), new KeyModified(KEY, 12)));
    assertViolation(
        writes, List.of(new KeyModified(KEY, 12), new KeyModified(KEY, 13), new KeyDeleted(KEY)));
  }

  @Test
  void rejectsAGapEvenWhenALaterVersionArrives() {
    assertViolation(
        List.of(new Put(12, 1, 1), new Put(13, 2, 1)), List.of(new KeyModified(KEY, 13)));
  }

  @Test
  void rejectsTheWrongTypeOrVersion() {
    assertViolation(List.of(new Put(12, 1, 1)), List.of(new KeyCreated(KEY, 12)));
    assertViolation(List.of(new Put(12, 1, 1)), List.of(new KeyModified(KEY, 11)));
    assertViolation(List.of(new Delete(true, 1)), List.of(new KeyModified(KEY, 12)));
  }

  @Test
  void acceptsTheNotificationsOfFailedAttemptsOnlyUpToTheirCount() {
    final List<Write> retried = List.of(new Put(13, 2, 2));

    assertThat(match(true, 10, retried, List.of(new KeyModified(KEY, 13))).exists()).isTrue();
    assertThat(
            match(true, 10, retried, List.of(new KeyModified(KEY, 12), new KeyModified(KEY, 13))))
        .isEqualTo(new Match(true, 13, Optional.empty()));
    assertViolation(
        retried,
        List.of(new KeyModified(KEY, 11), new KeyModified(KEY, 12), new KeyModified(KEY, 13)));
    assertViolation(
        List.of(new Put(13, 2, 1)), List.of(new KeyModified(KEY, 12), new KeyModified(KEY, 13)));
  }

  @Test
  void aFailedPutThatCreatedTheKeyTurnsTheRetryIntoAModification() {
    final Match match =
        match(
            false,
            10,
            List.of(new Put(13, 1, 2)),
            List.of(new KeyCreated(KEY, 12), new KeyModified(KEY, 13)));

    assertThat(match).isEqualTo(new Match(true, 13, Optional.empty()));
    assertViolation(
        false,
        List.of(new Put(13, 1, 2)),
        List.of(new KeyCreated(KEY, 12), new KeyCreated(KEY, 13)));
  }

  @Test
  void acceptsADeleteRetriedAfterAFailedAttemptDeletedTheKey() {
    assertThat(match(true, 10, List.of(new Delete(false, 2)), List.of(new KeyDeleted(KEY))))
        .isEqualTo(new Match(false, 10, Optional.empty()));
    assertThat(match(true, 10, List.of(new Delete(false, 2)), List.of()).missing()).isPresent();
    assertViolation(List.of(new Delete(false, 1)), List.of());
    assertViolation(false, List.of(new Delete(true, 1)), List.of(new KeyDeleted(KEY)));
    assertThat(match(false, 10, List.of(new Delete(false, 1)), List.of()).exists()).isFalse();
  }

  @Test
  void aRangeDeleteRemovesTheKeyWithoutAKeyNotification() {
    final Match match =
        match(
            true,
            10,
            List.of(Covered.INSTANCE, new Put(14, 0, 1)),
            List.of(new KeyCreated(KEY, 14)));

    assertThat(match).isEqualTo(new Match(true, 14, Optional.empty()));
    assertViolation(List.of(Covered.INSTANCE), List.of(new KeyDeleted(KEY)));
  }

  @Test
  void checksEveryKeyAndRangeOnceTheSubscriptionStarted() {
    final NotificationChecker checker = new NotificationChecker(PREFIX, List.of(KEY, OTHER));
    checker.accept(new KeyCreated(KEY, 3));
    assertThat(checker.observed(KEY, 3)).isTrue();
    assertThat(checker.observed(OTHER, 4)).isFalse();
    checker.start(Map.of(KEY, 3L, OTHER, 4L));

    checker.put(KEY, 7, 1, 1);
    final String end = OTHER + "~0";
    checker.registerRange(OTHER, end);
    checker.rangeDeleted(OTHER, end);
    checker.accept(new KeyModified(KEY, 7));
    checker.accept(new KeyModified("/elsewhere", 9));
    assertThat(checker.check())
        .containsExactly("[" + OTHER + ", " + end + "): a range delete notification");

    checker.accept(new KeyRangeDelete(OTHER, end));
    checker.accept(new KeyRangeDelete(OTHER, end));
    assertThat(checker.check()).isEmpty();
    assertThat(checker.version(KEY)).contains(7L);
    assertThat(checker.version(OTHER)).isEmpty();
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

  private static Match match(
      final boolean existed,
      final long versionId,
      final List<Write> writes,
      final List<Notification> observed) {
    return NotificationChecker.match(KEY, existed, versionId, writes, observed);
  }

  private static void assertViolation(final List<Write> writes, final List<Notification> observed) {
    assertViolation(true, writes, observed);
  }

  private static void assertViolation(
      final boolean existed, final List<Write> writes, final List<Notification> observed) {
    assertThatThrownBy(() -> match(existed, 10, writes, observed))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("key=" + KEY);
  }
}

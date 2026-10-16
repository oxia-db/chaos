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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.chaos.error.CorrectnessViolationException;
import io.oxia.chaos.inference.VersionModel.Attempt;
import io.oxia.chaos.inference.VersionModel.Kind;
import io.oxia.chaos.inference.VersionModel.Observed;
import io.oxia.chaos.inference.VersionModel.RecordVersion;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class VersionModelTest {

  private static final String KEY = "k";
  private static final RecordVersion CREATED = new RecordVersion(5, 100, 100, 0);
  private static final RecordVersion UPDATED = new RecordVersion(9, 100, 120, 1);

  @Test
  void acceptsAChainOfAppliedWrites() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    update(model, "b", CREATED, UPDATED);
    model.read(KEY, model.begin(), Optional.of(new Observed("b", UPDATED)));
    model.applied(
        KEY, model.attempt(KEY, Kind.DELETE, Optional.of(UPDATED), null), Optional.empty());

    model.checkpoint(Map.of());
    assertThat(model.size()).isZero();
  }

  @Test
  void rejectsTwoWritesReplacingTheSameVersion() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    update(model, "b", CREATED, UPDATED);

    assertThatThrownBy(() -> update(model, "c", CREATED, new RecordVersion(12, 100, 130, 1)))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("two writes replaced version 5");
  }

  @Test
  void rejectsAValueThatNoWriteWrote() {
    final VersionModel model = new VersionModel();

    assertThatThrownBy(
            () -> model.read(KEY, model.begin(), Optional.of(new Observed("ghost", CREATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("no accepted write");
  }

  @Test
  void rejectsAReadOfAnOlderVersionThanOneAlreadyCommitted() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    update(model, "b", CREATED, UPDATED);

    assertThatThrownBy(
            () -> model.read(KEY, model.begin(), Optional.of(new Observed("a", CREATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("read version 5 after version 9");
  }

  @Test
  void acceptsAnOlderVersionFromAReadThatOverlapsTheUpdate() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    final long readStarted = model.begin();
    update(model, "b", CREATED, UPDATED);

    assertThatCode(() -> model.read(KEY, readStarted, Optional.of(new Observed("a", CREATED))))
        .doesNotThrowAnyException();
  }

  @Test
  void checksTheMetadataOfEachVersion() {
    assertThat(VersionModel.metadataMismatch(Kind.CREATE, null, CREATED)).isNull();
    assertThat(VersionModel.metadataMismatch(Kind.CREATE, null, new RecordVersion(5, 100, 100, 1)))
        .contains("no modifications");
    assertThat(VersionModel.metadataMismatch(Kind.CREATE, null, new RecordVersion(5, 100, 101, 0)))
        .contains("modified when it is created");
    assertThat(VersionModel.metadataMismatch(Kind.UPDATE, CREATED, UPDATED)).isNull();
    assertThat(
            VersionModel.metadataMismatch(Kind.UPDATE, CREATED, new RecordVersion(5, 100, 120, 1)))
        .contains("higher version");
    assertThat(
            VersionModel.metadataMismatch(Kind.UPDATE, CREATED, new RecordVersion(9, 100, 120, 2)))
        .contains("one more modification");
    assertThat(
            VersionModel.metadataMismatch(Kind.UPDATE, CREATED, new RecordVersion(9, 101, 120, 1)))
        .contains("creation time");
    // Leaders' clocks differ: a later write may carry an earlier modification time
    assertThat(
            VersionModel.metadataMismatch(Kind.UPDATE, CREATED, new RecordVersion(9, 100, 90, 1)))
        .isNull();
  }

  @Test
  void rejectsAnAppliedWriteWithWrongMetadata() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);

    assertThatThrownBy(() -> update(model, "b", CREATED, new RecordVersion(9, 100, 120, 3)))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("one more modification");
  }

  @Test
  void adoptsTheVersionOfAPendingWriteThatAReadShows() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    final Attempt pending = model.attempt(KEY, Kind.UPDATE, Optional.of(CREATED), "b-0");
    final Attempt retry = model.attempt(KEY, Kind.UPDATE, Optional.of(CREATED), "b-1");
    model.rejected(retry, false);

    model.checkpoint(Map.of(KEY, new Observed("b-0", UPDATED)));
    assertThat(model.size()).isOne();
    assertThat(pending).isNotNull();
  }

  @Test
  void treatsAPendingWriteThatNoReadShowsAsNotApplied() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.attempt(KEY, Kind.UPDATE, Optional.of(CREATED), "b-0");

    assertThatCode(() -> model.checkpoint(Map.of(KEY, new Observed("a", CREATED))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsAWriteTheServerRejectedWhenItIsVisible() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.rejected(model.attempt(KEY, Kind.UPDATE, Optional.of(CREATED), "b"), false);

    assertThatThrownBy(() -> model.checkpoint(Map.of(KEY, new Observed("b", UPDATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("rejected is visible");
  }

  @Test
  void rejectsARejectionOfTheCurrentVersion() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.rejected(model.attempt(KEY, Kind.UPDATE, Optional.of(CREATED), "b"), true);

    assertThatThrownBy(() -> model.checkpoint(Map.of(KEY, new Observed("a", CREATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("no other write conflicts");
  }

  @Test
  void acceptsARejectionThatAConcurrentWriteExplains() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.rejected(model.attempt(KEY, Kind.DELETE, Optional.of(CREATED), null), true);
    update(model, "b", CREATED, UPDATED);

    assertThatCode(() -> model.checkpoint(Map.of(KEY, new Observed("b", UPDATED))))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsARetriedRejectionThatItsOwnEarlierAttemptExplains() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.attempt(KEY, Kind.DELETE, Optional.of(CREATED), null);
    model.rejected(model.attempt(KEY, Kind.DELETE, Optional.of(CREATED), null), false);

    assertThatCode(() -> model.checkpoint(Map.of())).doesNotThrowAnyException();
  }

  @Test
  void rejectsARejectedCreateOfAKeyThatNeverExisted() {
    final VersionModel model = new VersionModel();
    model.rejected(model.attempt(KEY, Kind.CREATE, Optional.empty(), "a"), true);

    assertThatThrownBy(() -> model.checkpoint(Map.of()))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("no other write conflicts");
  }

  @Test
  void rejectsAVersionThatVanishedWithoutADelete() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);

    assertThatThrownBy(() -> model.checkpoint(Map.of()))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("no write replaced it");
  }

  @Test
  void acceptsAVanishedVersionThatAPendingDeleteExplains() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.attempt(KEY, Kind.DELETE, Optional.of(CREATED), null);

    assertThatCode(() -> model.checkpoint(Map.of())).doesNotThrowAnyException();
  }

  @Test
  void rejectsARecordThatAnAppliedDeleteRemoved() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    model.applied(
        KEY, model.attempt(KEY, Kind.DELETE, Optional.of(CREATED), null), Optional.empty());

    assertThatThrownBy(() -> model.checkpoint(Map.of(KEY, new Observed("a", CREATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("still current");
  }

  @Test
  void forgetsReplacedValuesAtACheckpoint() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);
    update(model, "b", CREATED, UPDATED);
    model.checkpoint(Map.of(KEY, new Observed("b", UPDATED)));

    assertThatThrownBy(
            () -> model.read(KEY, model.begin(), Optional.of(new Observed("a", CREATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("no accepted write");
    assertThatCode(() -> update(model, "c", UPDATED, new RecordVersion(14, 100, 140, 2)))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsAnotherVersionForAnAppliedWrite() {
    final VersionModel model = new VersionModel();
    create(model, "a", CREATED);

    assertThatThrownBy(
            () -> model.read(KEY, model.begin(), Optional.of(new Observed("a", UPDATED))))
        .isInstanceOf(CorrectnessViolationException.class)
        .hasMessageContaining("observed");
  }

  private static void create(
      final VersionModel model, final String value, final RecordVersion version) {
    model.applied(
        KEY, model.attempt(KEY, Kind.CREATE, Optional.empty(), value), Optional.of(version));
  }

  private static void update(
      final VersionModel model,
      final String value,
      final RecordVersion predecessor,
      final RecordVersion version) {
    model.applied(
        KEY,
        model.attempt(KEY, Kind.UPDATE, Optional.of(predecessor), value),
        Optional.of(version));
  }
}

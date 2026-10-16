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
package io.oxia.chaos.ops;

/** Canonical operation names shared by runner test cases and their telemetry. */
public enum Operation {
  PUT("put"),
  GET("get"),
  FLOOR("floor"),
  CEILING("ceiling"),
  LOWER("lower"),
  HIGHER("higher"),
  DELETE("delete"),
  DELETE_RANGE("delete-range"),
  RANGE_SCAN("range-scan"),
  LIST("list"),
  EPHEMERAL_PUT("ephemeral-put"),
  OWNER_GET("owner-get"),
  OWNER_CLOSE("owner-close"),
  OWNER_CRASH("owner-crash"),
  OWNER_LOST("owner-lost"),
  INDEX_GET("index-get"),
  INDEX_LIST("index-list"),
  INDEX_RANGE_SCAN("index-range-scan"),
  SEQUENCE_PUT("sequence-put"),
  SEQUENCE_UPDATE("sequence-update");

  private final String label;

  Operation(final String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }
}

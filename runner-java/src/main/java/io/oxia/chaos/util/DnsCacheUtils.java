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

import java.security.Security;

/**
 * Keeps the JVM's DNS cache shorter than a pod restart. By default the JVM caches a lookup for 30 s
 * and a failed one for 10 s, so after a pod kill a new connection can keep dialing the old pod IP,
 * or keep failing to resolve the new pod, long after cluster DNS has the new address.
 */
public final class DnsCacheUtils {

  // Matches the TTL that the CoreDNS kubernetes plugin gives pod records.
  private static final String TTL_SECONDS = "5";
  private static final String NEGATIVE_TTL_SECONDS = "1";

  private DnsCacheUtils() {}

  /** Must run before the JVM's first host name lookup, which reads the cache policy once. */
  public static void limitCaching() {
    Security.setProperty("networkaddress.cache.ttl", TTL_SECONDS);
    Security.setProperty("networkaddress.cache.negative.ttl", NEGATIVE_TTL_SECONDS);
  }
}

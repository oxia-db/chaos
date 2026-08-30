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

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.TypeConversionException;

/** Parses compact CLI durations such as {@code 250ms}, {@code 30s}, {@code 5m}, or {@code 1h}. */
public final class DurationConverter implements ITypeConverter<Duration> {

  private static final Pattern DURATION = Pattern.compile("([0-9]+)(ms|s|m|h)");

  @Override
  public Duration convert(final String value) {
    final String normalized = value.toLowerCase(Locale.ROOT);
    final Matcher matcher = DURATION.matcher(normalized);
    if (!matcher.matches()) {
      throw new TypeConversionException("expected a duration with one of: ms, s, m, h");
    }

    final long amount;
    try {
      amount = Long.parseLong(matcher.group(1));
    } catch (final NumberFormatException error) {
      throw new TypeConversionException("duration is too large: " + value);
    }

    try {
      return switch (matcher.group(2)) {
        case "ms" -> Duration.ofMillis(amount);
        case "s" -> Duration.ofSeconds(amount);
        case "m" -> Duration.ofMinutes(amount);
        case "h" -> Duration.ofHours(amount);
        default -> throw new IllegalStateException("unreachable duration unit");
      };
    } catch (final ArithmeticException error) {
      throw new TypeConversionException("duration is too large: " + value);
    }
  }
}

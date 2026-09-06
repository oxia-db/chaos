#!/usr/bin/env python3

"""Shared Grafana HTTP client with bounded retries for transient failures."""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any


DEFAULT_MAX_ATTEMPTS = 6
DEFAULT_RETRY_DELAY_SECONDS = 5.0
MAX_RETRY_DELAY_SECONDS = 30.0


class GrafanaClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds cannot be negative")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.opener = opener
        self.sleep = sleep

    def retry_delay(self, error: urllib.error.HTTPError, attempt: int) -> float:
        retry_after = error.headers.get("Retry-After") if error.headers else None
        if retry_after is not None:
            try:
                return min(max(float(retry_after), 0), MAX_RETRY_DELAY_SECONDS)
            except ValueError:
                pass
        return min(
            self.retry_delay_seconds * (2 ** (attempt - 1)),
            MAX_RETRY_DELAY_SECONDS,
        )

    def retry(self, message: str, delay: float, attempt: int) -> None:
        print(
            f"{message}; retrying in {delay:g}s "
            f"(attempt {attempt + 1}/{self.max_attempts})",
            file=sys.stderr,
        )
        self.sleep(delay)

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, Any]:
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=payload,
            method=method,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        for attempt in range(1, self.max_attempts + 1):
            try:
                with self.opener(request, timeout=30) as response:
                    response_body = response.read()
                    return response.status, (
                        json.loads(response_body) if response_body else {}
                    )
            except urllib.error.HTTPError as error:
                error_body = error.read().decode("utf-8", errors="replace")
                error.close()
                message = (
                    f"Grafana API {method} {path} failed with HTTP "
                    f"{error.code}: {error_body}"
                )
                retryable = error.code == 429 or 500 <= error.code < 600
                if not retryable or attempt == self.max_attempts:
                    raise RuntimeError(message) from error
                self.retry(message, self.retry_delay(error, attempt), attempt)
            except (TimeoutError, urllib.error.URLError) as error:
                message = f"Grafana API {method} {path} failed: {error}"
                if attempt == self.max_attempts:
                    raise RuntimeError(message) from error
                delay = min(
                    self.retry_delay_seconds * (2 ** (attempt - 1)),
                    MAX_RETRY_DELAY_SECONDS,
                )
                self.retry(message, delay, attempt)

        raise AssertionError("Grafana request retry loop exited unexpectedly")

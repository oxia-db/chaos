#!/usr/bin/env python3
"""Publish Chaos Mesh resources as idempotent Grafana annotations."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DURATION_PATTERN = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ms|s|m|h)$")


def duration_seconds(value: str) -> float:
    match = DURATION_PATTERN.fullmatch(value)
    if not match:
        raise ValueError(f"unsupported chaos duration: {value}")
    multipliers = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    return float(match.group("value")) * multipliers[match.group("unit")]


def timestamp_milliseconds(value: str) -> int:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def annotation_for(
    resource: dict[str, Any], channel: str, run_url: str
) -> dict[str, Any]:
    metadata = resource.get("metadata", {})
    if resource.get("kind") == "WorkflowNode":
        spec = resource.get("spec", {})
        kind = str(spec.get("type", "Chaos"))
        name = str(spec.get("templateName", metadata.get("name", "unknown")))
        started = timestamp_milliseconds(spec["startTime"])
        ended = timestamp_milliseconds(spec.get("deadline", spec["startTime"]))
        chaos_spec = spec.get(f"{kind[0].lower()}{kind[1:]}", {})
        action = str(chaos_spec.get("action", kind))
    else:
        kind = str(resource.get("kind", "Chaos"))
        name = str(metadata.get("name", "unknown"))
        started = timestamp_milliseconds(metadata["creationTimestamp"])
        configured_duration = str(resource.get("spec", {}).get("duration", "10s"))
        ended = started + int(duration_seconds(configured_duration) * 1000)
        action = str(resource.get("spec", {}).get("action", kind))
    uid = str(metadata.get("uid", name))
    text = f"{kind} {name}: {action} on {channel}"
    if run_url:
        text = f"{text} — {run_url}"
    return {
        "time": started,
        "timeEnd": ended,
        "text": text,
        "tags": [
            "oxia-chaos",
            "chaos",
            channel,
            kind.lower(),
            f"oxia-chaos-event:{uid}",
        ],
    }


def is_injected_workflow_node(resource: dict[str, Any]) -> bool:
    if resource.get("kind") != "WorkflowNode":
        return True
    return any(
        condition.get("type") == "ChaosInjected" and condition.get("status") == "True"
        for condition in resource.get("status", {}).get("conditions", [])
    )


class GrafanaAnnotations:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> Any:
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
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response_body = response.read()
                return json.loads(response_body) if response_body else {}
        except urllib.error.HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Grafana annotations API failed with HTTP {error.code}: {error_body}"
            ) from error

    def publish_once(self, annotation: dict[str, Any]) -> bool:
        event_tag = next(
            tag for tag in annotation["tags"] if tag.startswith("oxia-chaos-event:")
        )
        query = urllib.parse.urlencode({"tags": event_tag, "limit": 1})
        if self.request("GET", f"/api/annotations?{query}"):
            return False
        self.request("POST", "/api/annotations", annotation)
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel", required=True)
    parser.add_argument("--run-url", default="")
    parser.add_argument(
        "--grafana-url", default=os.environ.get("GRAFANA_CLOUD_GRAFANA_URL", "")
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN", ""),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    resources = json.load(sys.stdin)
    items = resources.get("items", [])
    client = GrafanaAnnotations(args.grafana_url, args.token)
    published = 0
    for resource in items:
        if not is_injected_workflow_node(resource):
            continue
        annotation = annotation_for(resource, args.channel, args.run_url)
        if client.publish_once(annotation):
            published += 1
    print(f"Published {published} Grafana chaos annotations for {args.channel}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)

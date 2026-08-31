#!/usr/bin/env python3
"""Validate adapted server-dashboard PromQL against Grafana Cloud."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MACROS = {
    "$__rate_interval": "5m",
    "$__range": "1h",
}


@dataclass(frozen=True)
class DashboardQuery:
    dashboard: str
    panel: str
    expression: str
    channel: str


@dataclass(frozen=True)
class QueryResult:
    query: DashboardQuery
    error: str | None
    series: int


def walk_panels(panels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for panel in panels:
        flattened.append(panel)
        flattened.extend(walk_panels(panel.get("panels", [])))
    return flattened


def load_queries(dashboard_dir: Path, channels: list[str]) -> list[DashboardQuery]:
    queries: list[DashboardQuery] = []
    for path in sorted(dashboard_dir.glob("oxia-server-*.json")):
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        dashboard_variables = {
            variable["name"]: variable
            for variable in dashboard.get("templating", {}).get("list", [])
        }
        for panel in walk_panels(dashboard["panels"]):
            if panel.get("datasource", {}).get("type") != "prometheus":
                continue
            for target in panel.get("targets", []):
                expression = target.get("expr")
                if not expression:
                    continue
                for channel in channels:
                    rendered = expression
                    replacements = dict(MACROS)
                    replacements["$channel"] = channel
                    for name, variable in dashboard_variables.items():
                        if name in {"channel", "logs", "metrics"}:
                            continue
                        value = variable.get("allValue")
                        if value is None:
                            value = variable.get("current", {}).get("value", ".*")
                        if isinstance(value, list):
                            value = "|".join(str(item) for item in value)
                        replacements[f"${name}"] = str(value)
                    for variable in sorted(replacements, key=len, reverse=True):
                        rendered = rendered.replace(variable, replacements[variable])
                    queries.append(
                        DashboardQuery(path.name, panel.get("title", ""), rendered, channel)
                    )
    return queries


def execute_query(
    query: DashboardQuery,
    grafana_url: str,
    token: str,
    datasource_uid: str,
) -> QueryResult:
    parameters = urllib.parse.urlencode({"query": query.expression})
    url = (
        f"{grafana_url.rstrip('/')}/api/datasources/proxy/uid/"
        f"{urllib.parse.quote(datasource_uid)}/api/v1/query?{parameters}"
    )
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
    )
    payload: dict[str, Any] | None = None
    last_error: OSError | None = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            return QueryResult(query, f"HTTP {error.code}: {error_body}", 0)
        except OSError as error:
            last_error = error
            if attempt < 3:
                time.sleep(attempt)
    if payload is None:
        return QueryResult(query, str(last_error), 0)
    if payload.get("status") != "success":
        return QueryResult(query, str(payload.get("error", payload)), 0)
    result = payload.get("data", {}).get("result", [])
    return QueryResult(query, None, len(result))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--grafana-url", default=os.environ.get("GRAFANA_CLOUD_GRAFANA_URL", "")
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN", ""),
    )
    parser.add_argument("--datasource-uid", default="grafanacloud-prom")
    parser.add_argument("--channels", default="stable,beta")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--dashboard-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "observability" / "dashboards",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.grafana_url or not args.token:
        print("Grafana URL and service-account token are required", file=sys.stderr)
        return 2
    channels = [channel.strip() for channel in args.channels.split(",") if channel.strip()]
    queries = load_queries(args.dashboard_dir, channels)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        results = list(
            executor.map(
                lambda query: execute_query(
                    query, args.grafana_url, args.token, args.datasource_uid
                ),
                queries,
            )
        )

    failures = [result for result in results if result.error or result.series == 0]
    for result in failures:
        reason = result.error if result.error else "empty result"
        print(
            f"{result.query.dashboard} [{result.query.channel}] "
            f"{result.query.panel}: {reason}\n  {result.query.expression}",
            file=sys.stderr,
        )
    print(
        f"Validated {len(results) - len(failures)}/{len(results)} PromQL queries "
        f"across {len(channels)} channels"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

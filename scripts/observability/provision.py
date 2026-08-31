#!/usr/bin/env python3
"""Provision the repository-owned Grafana Cloud dashboards."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


FOLDER_UID = "oxia-chaos"
FOLDER_TITLE = "Oxia Chaos"


class GrafanaClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
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
                return response.status, (
                    json.loads(response_body) if response_body else {}
                )
        except urllib.error.HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Grafana API {method} {path} failed with HTTP {error.code}: {error_body}"
            ) from error

    def ensure_folder(self) -> None:
        try:
            self.request("GET", f"/api/folders/{FOLDER_UID}")
        except RuntimeError as error:
            if "HTTP 404" not in str(error):
                raise
            self.request(
                "POST", "/api/folders", {"uid": FOLDER_UID, "title": FOLDER_TITLE}
            )

    def provision_dashboard(self, dashboard: dict[str, Any]) -> str:
        _, response = self.request(
            "POST",
            "/api/dashboards/db",
            {
                "dashboard": dashboard,
                "folderUid": FOLDER_UID,
                "message": "Provisioned from oxia-db/chaos",
                "overwrite": True,
            },
        )
        return str(response.get("url", f"/d/{dashboard['uid']}"))


def load_dashboard(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as dashboard_file:
        dashboard = json.load(dashboard_file)
    for field in ("title", "uid", "schemaVersion", "panels"):
        if field not in dashboard:
            raise ValueError(f"{path} is missing dashboard field {field}")
    dashboard["id"] = None
    return dashboard


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--grafana-url", default=os.environ.get("GRAFANA_CLOUD_GRAFANA_URL", "")
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN", ""),
    )
    parser.add_argument(
        "--dashboard-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "observability" / "dashboards",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.grafana_url or not args.token:
        print(
            "Skipping Grafana dashboard provisioning; URL or service-account token is unavailable"
        )
        return 0

    dashboard_paths = sorted(args.dashboard_dir.glob("*.json"))
    if not dashboard_paths:
        raise RuntimeError(f"no dashboards found in {args.dashboard_dir}")

    client = GrafanaClient(args.grafana_url, args.token)
    client.ensure_folder()
    for dashboard_path in dashboard_paths:
        dashboard = load_dashboard(dashboard_path)
        url = client.provision_dashboard(dashboard)
        print(f"Provisioned {dashboard['title']}: {args.grafana_url.rstrip('/')}{url}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)

#!/usr/bin/env python3

"""Collect per-channel, per-testcase results from a correctness environment."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from status import PHASE_FAILURES, create_result, write_json


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True)


def parse_execution_plan(manifest: str) -> list[dict[str, str]]:
    prefix = "  test-cases.json: "
    for line in manifest.splitlines():
        if line.startswith(prefix):
            encoded = json.loads(line[len(prefix) :])
            value = json.loads(encoded)
            if not isinstance(value, list):
                break
            for item in value:
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("runner"), str)
                    or not isinstance(item.get("testCase"), str)
                ):
                    raise ValueError("execution plan contains an invalid testcase")
            return value
    raise ValueError("rendered chart does not contain a testcase execution plan")


def render_testcases(args: argparse.Namespace) -> list[dict[str, str]]:
    result = run_command(
        [
            args.helm,
            "template",
            "status-plan",
            args.chart,
            "--namespace",
            args.namespace,
            "--set",
            "oxia-cluster.enabled=false",
            "--set",
            "chaos-mesh.enabled=false",
            "--set",
            "runnerWorkloadsEnabled=false",
            "--set-string",
            "channel=status-plan",
            "--set-string",
            f"testProfile={args.workload_profile}",
            "--show-only",
            "templates/execution-plan-configmap.yaml",
        ]
    )
    if result.returncode != 0:
        raise RuntimeError(f"cannot render testcase execution plan: {result.stderr.strip()}")
    return parse_execution_plan(result.stdout)


def load_channels(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    plan_path = Path(args.channel_plan)
    source_path = plan_path if plan_path.is_file() else Path(args.channel_config)
    value = json.loads(source_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("channel plan must contain an object")
    channels: dict[str, dict[str, Any]] = {}
    for channel_id in args.channels:
        channel = value.get(channel_id)
        if not isinstance(channel, dict):
            raise ValueError(f"channel plan does not define {channel_id}")
        channels[channel_id] = channel
    return channels


def job_outcome(args: argparse.Namespace, channel: str, runner: str, test_case: str) -> str:
    release = f"{args.release_name}-{channel}"
    selector = ",".join(
        (
            f"app.kubernetes.io/instance={release}",
            f"oxia.chaos/runner={runner}",
            f"oxia.chaos/case={test_case}",
        )
    )
    result = run_command(
        [
            args.kubectl,
            "--context",
            args.kube_context,
            "--namespace",
            args.namespace,
            "get",
            "jobs",
            f"--selector={selector}",
            "--output=json",
        ]
    )
    if result.returncode != 0:
        return "skipped"
    value = json.loads(result.stdout)
    items = value.get("items", []) if isinstance(value, dict) else []
    if not items:
        return "skipped"
    latest = max(items, key=lambda item: item.get("metadata", {}).get("creationTimestamp", ""))
    status = latest.get("status", {})
    if status.get("succeeded", 0) > 0:
        return "success"
    if status.get("failed", 0) > 0:
        return "failure"
    return "skipped"


def collect(args: argparse.Namespace) -> None:
    testcases = render_testcases(args)
    channels = load_channels(args)
    base_phases = {
        "image": args.image_outcome,
        "verify": args.verify_outcome,
        "cluster": args.cluster_outcome,
        "preload": args.install_outcome,
        "install": args.install_outcome,
        "runner": args.install_outcome,
        "workflow": args.workflow_outcome,
        "chaos": args.chaos_outcome,
    }
    output_dir = Path(args.output_dir)
    for channel_id, channel in channels.items():
        server_version = channel.get("serverVersion")
        if not isinstance(server_version, str) or not server_version:
            raise ValueError(f"channel {channel_id} has no serverVersion")
        for testcase in testcases:
            runner = testcase["runner"]
            test_case = testcase["testCase"]
            phases = dict(base_phases)
            phases["result"] = job_outcome(args, channel_id, runner, test_case)
            job_status = "success" if all(value == "success" for value in phases.values()) else "failure"
            result = create_result(
                argparse.Namespace(
                    channel=channel_id,
                    server_version=server_version,
                    test_case=test_case,
                    run_date=args.run_date,
                    run_url=args.run_url,
                    job_status=job_status,
                    phases_json=json.dumps(phases),
                )
            )
            write_json(output_dir / f"{channel_id}-{runner}-{test_case}.json", result)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--channel-config", required=True)
    root.add_argument("--channel-plan", required=True)
    root.add_argument("--channels", nargs="+", required=True)
    root.add_argument("--chart", required=True)
    root.add_argument("--workload-profile", required=True)
    root.add_argument("--release-name", required=True)
    root.add_argument("--namespace", required=True)
    root.add_argument("--kube-context", required=True)
    root.add_argument("--output-dir", required=True)
    root.add_argument("--run-date", required=True)
    root.add_argument("--run-url", required=True)
    for phase in PHASE_FAILURES:
        if phase in {"preload", "runner", "result"}:
            continue
        root.add_argument(f"--{phase}-outcome", required=True)
    root.add_argument("--helm", default="helm")
    root.add_argument("--kubectl", default="kubectl")
    return root


def main() -> int:
    collect(parser().parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

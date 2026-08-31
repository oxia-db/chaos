#!/usr/bin/env python3

"""Create and maintain the public Oxia stability status document."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
WINDOW_DAYS = 90
CHANNELS_CONFIG = Path(__file__).resolve().parents[2] / "config" / "oxia-channels.json"


def load_channel_versions() -> dict[str, str]:
    try:
        channels = json.loads(CHANNELS_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot load channel configuration from {CHANNELS_CONFIG}: {error}") from error
    if not isinstance(channels, dict) or list(channels) != ["stable", "beta"]:
        raise RuntimeError("channel configuration must define stable and beta in order")
    versions: dict[str, str] = {}
    for channel_id, channel in channels.items():
        if not isinstance(channel, dict):
            raise RuntimeError(f"channel configuration for {channel_id} must be an object")
        server_version = channel.get("serverVersion")
        if not isinstance(server_version, str) or not server_version:
            raise RuntimeError(f"channel configuration for {channel_id} must define serverVersion")
        versions[channel_id] = server_version
    return versions


CHANNEL_VERSIONS = load_channel_versions()
TEST_CASES = (
    "basic-kv",
    "ephemeral",
    "notification",
    "sequences",
    "secondary-index",
    "versioning",
)
RESULTS = {"passed", "failed", "not_run"}
CHAOS = {
    "profile": "six-hour",
    "duration": "6h",
    "expectedInjections": 1,
    "injections": [
        {"type": "pod-kill", "count": 1},
    ],
}
LEGACY_CHAOS = {
    "profile": "five-cycle",
    "duration": "6h",
    "expectedInjections": 30,
    "injections": [
        {"type": "pod-kill", "count": 30},
    ],
}
LEGACY_MULTI_FAULT_CHAOS = {
    "profile": "five-cycle",
    "duration": "6h",
    "expectedInjections": 50,
    "injections": [
        {"type": "pod-kill", "count": 30},
        {"type": "network-partition", "count": 5},
        {"type": "cpu-pressure", "count": 5},
        {"type": "memory-pressure", "count": 5},
        {"type": "network-latency", "count": 5},
    ],
}
PHASE_FAILURES = {
    "image": (
        "Oxia server image could not be resolved",
        "The configured channel image was unavailable or its immutable digest could not be determined.",
    ),
    "verify": (
        "Runner or chart validation failed",
        "The repository checks did not pass before the test environment was created.",
    ),
    "cluster": (
        "Test cluster creation failed",
        "The kind cluster could not be created on the correctness runner.",
    ),
    "preload": (
        "Dependency image preparation failed",
        "The pinned Oxia or Chaos Mesh images could not be loaded into the test cluster.",
    ),
    "install": (
        "Oxia test environment failed to start",
        "The Oxia cluster or Chaos Mesh did not become ready before its installation deadline.",
    ),
    "runner": (
        "Correctness workload failed to start",
        "The Java basic-kv runner did not become ready with the released image.",
    ),
    "workflow": (
        "Chaos workflow failed to start",
        "The six-hour Chaos Mesh Workflow could not be submitted to the test cluster.",
    ),
    "chaos": (
        "Chaos pipeline did not complete",
        "The workflow failed, its recovery health check failed, or the server pod kill was not observed.",
    ),
    "result": (
        "Correctness workload failed",
        "The basic-kv workload reported a correctness, retry, timeout, or final checkpoint failure.",
    ),
}


class StatusValidationError(ValueError):
    """Raised when status input does not satisfy the public contract."""


def utc_timestamp(value: str | None = None) -> str:
    instant = datetime.now(UTC) if value is None else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise StatusValidationError("timestamp must include a timezone")
    return instant.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise StatusValidationError(f"invalid UTC date {value!r}") from error


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StatusValidationError(f"cannot read JSON from {path}: {error}") from error
    if not isinstance(value, dict):
        raise StatusValidationError(f"{path} must contain a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def expected_dates(generated_at: str) -> list[str]:
    today = datetime.fromisoformat(generated_at.replace("Z", "+00:00")).astimezone(UTC).date()
    first = today - timedelta(days=WINDOW_DAYS - 1)
    return [(first + timedelta(days=offset)).isoformat() for offset in range(WINDOW_DAYS)]


def validate_history_entry(entry: Any, location: str) -> None:
    if not isinstance(entry, dict):
        raise StatusValidationError(f"{location} must be an object")
    if set(entry) < {"date", "result"}:
        raise StatusValidationError(f"{location} must include date and result")
    parse_date(str(entry["date"]))
    result = entry["result"]
    if result not in RESULTS:
        raise StatusValidationError(f"{location}.result must be passed, failed, or not_run")
    if result == "not_run":
        if set(entry) != {"date", "result"}:
            raise StatusValidationError(f"{location} not_run entries cannot contain run metadata")
        return
    if not isinstance(entry.get("runUrl"), str) or not entry["runUrl"].startswith("https://github.com/"):
        raise StatusValidationError(f"{location}.runUrl must be a GitHub Actions URL")
    if result == "passed":
        if set(entry) != {"date", "result", "runUrl"}:
            raise StatusValidationError(f"{location} passed entries contain unexpected fields")
        return
    for field in ("title", "detail"):
        if not isinstance(entry.get(field), str) or not entry[field].strip():
            raise StatusValidationError(f"{location}.{field} is required for failed results")
    if set(entry) != {"date", "result", "title", "detail", "runUrl"}:
        raise StatusValidationError(f"{location} failed entries contain unexpected fields")


def validate_chaos(value: Any, location: str) -> None:
    if value != CHAOS:
        raise StatusValidationError(f"{location} must describe the canonical single-pod-kill six-hour profile")


def validate_summary(summary: dict[str, Any]) -> None:
    if set(summary) != {"schemaVersion", "generatedAt", "windowDays", "channels"}:
        raise StatusValidationError("summary has unexpected or missing top-level fields")
    if summary["schemaVersion"] != SCHEMA_VERSION:
        raise StatusValidationError(f"schemaVersion must be {SCHEMA_VERSION}")
    if summary["windowDays"] != WINDOW_DAYS:
        raise StatusValidationError(f"windowDays must be {WINDOW_DAYS}")
    generated_at = utc_timestamp(str(summary["generatedAt"]))
    dates = expected_dates(generated_at)
    channels = summary["channels"]
    if not isinstance(channels, list) or [item.get("id") for item in channels if isinstance(item, dict)] != list(CHANNEL_VERSIONS):
        raise StatusValidationError("channels must contain stable and beta exactly once in order")
    for channel in channels:
        location = f"channel[{channel['id']}]"
        if set(channel) != {"id", "serverVersion", "updatedAt", "testCases", "chaos"}:
            raise StatusValidationError(f"{location} has unexpected or missing fields")
        if not isinstance(channel["serverVersion"], str) or not channel["serverVersion"].strip():
            raise StatusValidationError(f"{location}.serverVersion must be non-empty")
        if channel["serverVersion"].lower().endswith(".x"):
            raise StatusValidationError(f"{location}.serverVersion cannot be a version range")
        utc_timestamp(str(channel["updatedAt"]))
        validate_chaos(channel["chaos"], f"{location}.chaos")
        test_cases = channel["testCases"]
        if not isinstance(test_cases, list) or [item.get("id") for item in test_cases if isinstance(item, dict)] != list(TEST_CASES):
            raise StatusValidationError(f"{location}.testCases must contain the canonical testcase IDs in order")
        for test_case in test_cases:
            case_location = f"{location}.testCases[{test_case['id']}]"
            if set(test_case) != {"id", "history"}:
                raise StatusValidationError(f"{case_location} has unexpected or missing fields")
            history = test_case["history"]
            if not isinstance(history, list) or len(history) != WINDOW_DAYS:
                raise StatusValidationError(f"{case_location}.history must contain exactly {WINDOW_DAYS} entries")
            if [entry.get("date") for entry in history if isinstance(entry, dict)] != dates:
                raise StatusValidationError(f"{case_location}.history must contain consecutive UTC dates oldest first")
            for index, entry in enumerate(history):
                validate_history_entry(entry, f"{case_location}.history[{index}]")


def validate_result(value: dict[str, Any]) -> None:
    required = {
        "schemaVersion",
        "channelId",
        "serverVersion",
        "testCaseId",
        "date",
        "result",
        "runUrl",
        "chaos",
    }
    optional = {"title", "detail"}
    if not required <= set(value) or not set(value) <= required | optional:
        raise StatusValidationError("normalized result has unexpected or missing fields")
    if value["schemaVersion"] != SCHEMA_VERSION:
        raise StatusValidationError(f"normalized result schemaVersion must be {SCHEMA_VERSION}")
    if value["channelId"] not in CHANNEL_VERSIONS:
        raise StatusValidationError("normalized result has an unsupported channelId")
    if value["testCaseId"] not in TEST_CASES:
        raise StatusValidationError("normalized result has an unsupported testCaseId")
    if not isinstance(value["serverVersion"], str) or not value["serverVersion"].strip():
        raise StatusValidationError("normalized result serverVersion must be non-empty")
    parse_date(str(value["date"]))
    validate_chaos(value["chaos"], "normalized result chaos")
    entry = {key: value[key] for key in ("date", "result", "title", "detail", "runUrl") if key in value}
    validate_history_entry(entry, "normalized result")


def create_result(args: argparse.Namespace) -> dict[str, Any]:
    phases = json.loads(args.phases_json)
    if not isinstance(phases, dict):
        raise StatusValidationError("phases-json must contain an object")
    failed_phase = next((phase for phase in PHASE_FAILURES if phases.get(phase) == "failure"), None)
    passed = args.job_status == "success" and all(phases.get(phase) == "success" for phase in PHASE_FAILURES)
    value: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "channelId": args.channel,
        "serverVersion": args.server_version,
        "testCaseId": args.test_case,
        "date": args.run_date,
        "result": "passed" if passed else "failed",
        "runUrl": args.run_url,
        "chaos": CHAOS,
    }
    if not passed:
        if failed_phase is not None:
            title, detail = PHASE_FAILURES[failed_phase]
        elif args.job_status == "cancelled":
            title = "Scheduled correctness run was cancelled"
            detail = "The six-hour correctness workload was cancelled before all required phases completed."
        else:
            title = "Scheduled correctness run did not complete"
            detail = "The workflow ended without successful chaos-pipeline and correctness results."
        value["title"] = title
        value["detail"] = detail
    validate_result(value)
    return value


def history_map(summary: dict[str, Any] | None) -> dict[tuple[str, str, str], dict[str, Any]]:
    values: dict[tuple[str, str, str], dict[str, Any]] = {}
    if summary is None:
        return values
    for channel in summary.get("channels", []):
        if isinstance(channel, dict) and channel.get("chaos") in (
            LEGACY_CHAOS,
            LEGACY_MULTI_FAULT_CHAOS,
        ):
            channel["chaos"] = CHAOS
    validate_summary(summary)
    for channel in summary["channels"]:
        for test_case in channel["testCases"]:
            for entry in test_case["history"]:
                values[(channel["id"], test_case["id"], entry["date"])] = entry
    return values


def merge_summary(args: argparse.Namespace) -> dict[str, Any]:
    generated_at = utc_timestamp(args.generated_at)
    dates = expected_dates(generated_at)
    existing_path = Path(args.existing)
    existing = read_json(existing_path) if existing_path.is_file() else None
    values = history_map(existing)
    result_paths = sorted(Path(args.results).rglob("*.json")) if Path(args.results).is_dir() else []
    normalized_results: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for result_path in result_paths:
        result = read_json(result_path)
        validate_result(result)
        key = (result["channelId"], result["testCaseId"], result["date"])
        if key in seen:
            raise StatusValidationError(f"duplicate result for {key}")
        seen.add(key)
        normalized_results.append(result)
    expected_results = getattr(args, "expected_result", [])
    for expected in expected_results:
        parts = expected.split(",", 2)
        if len(parts) != 3:
            raise StatusValidationError(f"invalid expected result {expected!r}")
        channel_id, server_version, test_case_id = parts
        key = (channel_id, test_case_id, args.fallback_date)
        if key in seen:
            continue
        fallback = {
            "schemaVersion": SCHEMA_VERSION,
            "channelId": channel_id,
            "serverVersion": server_version,
            "testCaseId": test_case_id,
            "date": args.fallback_date,
            "result": "failed",
            "title": "Scheduled result artifact was not published",
            "detail": "The correctness workflow completed without a readable normalized result artifact.",
            "runUrl": args.fallback_run_url,
            "chaos": CHAOS,
        }
        validate_result(fallback)
        normalized_results.append(fallback)
    versions = dict(CHANNEL_VERSIONS)
    updated_channels: set[str] = set()
    for result in normalized_results:
        if result["date"] not in dates:
            raise StatusValidationError(f"result date {result['date']} is outside the {WINDOW_DAYS}-day window")
        key = (result["channelId"], result["testCaseId"], result["date"])
        values[key] = {
            field: result[field]
            for field in ("date", "result", "title", "detail", "runUrl")
            if field in result
        }
        versions[result["channelId"]] = result["serverVersion"]
        updated_channels.add(result["channelId"])
    previous_channels = {channel["id"]: channel for channel in existing["channels"]} if existing else {}
    channels = []
    for channel_id in CHANNEL_VERSIONS:
        test_cases = []
        for test_case_id in TEST_CASES:
            history = [
                values.get((channel_id, test_case_id, day), {"date": day, "result": "not_run"})
                for day in dates
            ]
            test_cases.append({"id": test_case_id, "history": history})
        previous_updated_at = previous_channels.get(channel_id, {}).get("updatedAt", generated_at)
        channels.append(
            {
                "id": channel_id,
                "serverVersion": versions[channel_id],
                "updatedAt": generated_at if channel_id in updated_channels else previous_updated_at,
                "testCases": test_cases,
                "chaos": CHAOS,
            }
        )
    summary = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "windowDays": WINDOW_DAYS,
        "channels": channels,
    }
    validate_summary(summary)
    return summary


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    result = commands.add_parser("write-result", help="write one normalized testcase result")
    result.add_argument("--output", required=True)
    result.add_argument("--channel", required=True)
    result.add_argument("--server-version", required=True)
    result.add_argument("--test-case", required=True)
    result.add_argument("--run-date", required=True)
    result.add_argument("--run-url", required=True)
    result.add_argument("--job-status", required=True, choices=("success", "failure", "cancelled"))
    result.add_argument("--phases-json", required=True)

    merge = commands.add_parser("merge", help="merge normalized results into the 90-day summary")
    merge.add_argument("--existing", required=True)
    merge.add_argument("--results", required=True)
    merge.add_argument("--output", required=True)
    merge.add_argument("--generated-at")
    merge.add_argument("--fallback-date", required=True)
    merge.add_argument("--fallback-run-url", required=True)
    merge.add_argument(
        "--expected-result",
        action="append",
        default=[],
        help="expected channel,server-version,testcase tuple; missing tuples become failed",
    )

    validate = commands.add_parser("validate", help="validate a public summary document")
    validate.add_argument("summary")
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "write-result":
            write_json(Path(args.output), create_result(args))
        elif args.command == "merge":
            write_json(Path(args.output), merge_summary(args))
        else:
            validate_summary(read_json(Path(args.summary)))
    except (StatusValidationError, json.JSONDecodeError) as error:
        print(f"status data error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

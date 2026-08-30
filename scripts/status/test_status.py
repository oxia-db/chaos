import argparse
import json
import tempfile
import unittest
from pathlib import Path

from collect import parse_execution_plan
from status import CHAOS, TEST_CASES, WINDOW_DAYS, StatusValidationError, create_result, merge_summary, validate_summary


class StatusDataTest(unittest.TestCase):
    def test_execution_plan_is_read_from_rendered_configmap(self) -> None:
        manifest = '''
data:
  channel: "stable"
  test-cases.json: "[{\\"runner\\":\\"java\\",\\"testCase\\":\\"basic-kv\\"}]"
'''
        self.assertEqual(
            parse_execution_plan(manifest),
            [{"runner": "java", "testCase": "basic-kv"}],
        )

    def test_merge_backfills_all_channels_and_cases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "results"
            results.mkdir()
            (results / "stable-basic-kv.json").write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "channelId": "stable",
                        "serverVersion": "0.16.8",
                        "testCaseId": "basic-kv",
                        "date": "2026-08-31",
                        "result": "passed",
                        "runUrl": "https://github.com/oxia-db/chaos/actions/runs/123",
                        "chaos": CHAOS,
                    }
                ),
                encoding="utf-8",
            )
            summary = merge_summary(
                argparse.Namespace(
                    existing=str(root / "missing.json"),
                    results=str(results),
                    output=str(root / "summary.json"),
                    generated_at="2026-08-31T06:15:00Z",
                    fallback_date="2026-08-31",
                    fallback_run_url="https://github.com/oxia-db/chaos/actions/runs/123",
                )
            )
            validate_summary(summary)
            self.assertEqual([channel["id"] for channel in summary["channels"]], ["stable", "beta"])
            for channel in summary["channels"]:
                self.assertEqual([case["id"] for case in channel["testCases"]], list(TEST_CASES))
                for test_case in channel["testCases"]:
                    self.assertEqual(len(test_case["history"]), WINDOW_DAYS)
            latest = summary["channels"][0]["testCases"][0]["history"][-1]
            self.assertEqual(latest["result"], "passed")
            self.assertEqual(summary["channels"][1]["serverVersion"], "0.17")

    def test_missing_artifact_becomes_failed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = merge_summary(
                argparse.Namespace(
                    existing=str(root / "missing.json"),
                    results=str(root / "missing-results"),
                    output=str(root / "summary.json"),
                    generated_at="2026-08-31T06:15:00Z",
                    fallback_date="2026-08-31",
                    fallback_run_url="https://github.com/oxia-db/chaos/actions/runs/456",
                    expected_result=["stable,0.16,basic-kv"],
                )
            )
            latest = summary["channels"][0]["testCases"][0]["history"][-1]
            self.assertEqual(latest["result"], "failed")
            self.assertIn("artifact", latest["title"].lower())

    def test_missing_expected_channel_artifact_does_not_change_unsupported_cases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = merge_summary(
                argparse.Namespace(
                    existing=str(root / "missing.json"),
                    results=str(root / "missing-results"),
                    output=str(root / "summary.json"),
                    generated_at="2026-08-31T06:15:00Z",
                    fallback_date="2026-08-31",
                    fallback_run_url="https://github.com/oxia-db/chaos/actions/runs/456",
                    expected_result=[
                        "stable,0.16,basic-kv",
                        "beta,0.17,basic-kv",
                    ],
                )
            )
            for channel in summary["channels"]:
                self.assertEqual(channel["testCases"][0]["history"][-1]["result"], "failed")
                for test_case in channel["testCases"][1:]:
                    self.assertEqual(test_case["history"][-1]["result"], "not_run")

    def test_empty_merge_bootstraps_not_run_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = merge_summary(
                argparse.Namespace(
                    existing=str(root / "missing.json"),
                    results=str(root / "missing-results"),
                    output=str(root / "summary.json"),
                    generated_at="2026-08-31T06:15:00Z",
                    fallback_date="2026-08-31",
                    fallback_run_url="https://github.com/oxia-db/chaos/actions",
                )
            )
            for channel in summary["channels"]:
                for test_case in channel["testCases"]:
                    self.assertTrue(all(entry["result"] == "not_run" for entry in test_case["history"]))

    def test_result_reports_failed_phase(self) -> None:
        phases = {phase: "success" for phase in ("image", "verify", "cluster", "preload", "install")}
        phases.update({"runner": "failure", "workflow": "skipped", "chaos": "skipped", "result": "skipped"})
        result = create_result(
            argparse.Namespace(
                channel="stable",
                server_version="0.16.8",
                test_case="basic-kv",
                run_date="2026-08-31",
                run_url="https://github.com/oxia-db/chaos/actions/runs/789",
                job_status="failure",
                phases_json=json.dumps(phases),
            )
        )
        self.assertEqual(result["result"], "failed")
        self.assertEqual(result["title"], "Correctness workload failed to start")

    def test_summary_rejects_missing_failure_detail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = merge_summary(
                argparse.Namespace(
                    existing=str(root / "missing.json"),
                    results=str(root / "missing-results"),
                    output=str(root / "summary.json"),
                    generated_at="2026-08-31T06:15:00Z",
                    fallback_date="2026-08-31",
                    fallback_run_url="https://github.com/oxia-db/chaos/actions/runs/456",
                    expected_result=["stable,0.16,basic-kv"],
                )
            )
            del summary["channels"][0]["testCases"][0]["history"][-1]["detail"]
            with self.assertRaises(StatusValidationError):
                validate_summary(summary)


if __name__ == "__main__":
    unittest.main()

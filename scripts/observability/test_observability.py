import json
import tempfile
import unittest
from pathlib import Path

from provision import load_dashboard
from publish_annotations import annotation_for, duration_seconds, is_injected_workflow_node


class ProvisionTest(unittest.TestCase):
    def test_load_dashboard_requires_repository_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dashboard.json"
            path.write_text(
                json.dumps(
                    {
                        "id": 42,
                        "title": "Dashboard",
                        "uid": "dashboard",
                        "schemaVersion": 41,
                        "panels": [],
                    }
                ),
                encoding="utf-8",
            )

            dashboard = load_dashboard(path)

        self.assertIsNone(dashboard["id"])


class DashboardTest(unittest.TestCase):
    def setUp(self) -> None:
        dashboard_dir = Path(__file__).resolve().parents[2] / "observability" / "dashboards"
        self.cluster = json.loads((dashboard_dir / "oxia-cluster.json").read_text(encoding="utf-8"))
        self.client = json.loads((dashboard_dir / "oxia-client.json").read_text(encoding="utf-8"))

    @staticmethod
    def expressions(dashboard: dict[str, object]) -> list[str]:
        return [
            target["expr"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
            if "expr" in target
        ]

    def test_dashboards_keep_stable_and_beta_separate(self) -> None:
        for dashboard in (self.cluster, self.client):
            channel = next(
                variable
                for variable in dashboard["templating"]["list"]
                if variable["name"] == "channel"
            )
            self.assertFalse(channel["multi"])
            self.assertFalse(channel["includeAll"])
            self.assertEqual(channel["query"], "stable,beta")
            self.assertTrue(all("$channel" in expression for expression in self.expressions(dashboard)))

    def test_cluster_dashboard_covers_server_subsystems(self) -> None:
        expressions = "\n".join(self.expressions(self.cluster))
        for metric in (
            "oxia_server_db_puts_count_total",
            "oxia_server_follower_ack_offset_count",
            "oxia_server_wal_sync_latency_milliseconds_bucket",
            "oxia_server_kv_pebble_compaction_debt_bytes",
            "oxia_coordinator_leader_election_latency_milliseconds_bucket",
        ):
            self.assertIn(metric, expressions)

    def test_client_dashboard_exposes_sdk_metrics_and_all_sampled_traces(self) -> None:
        expressions = "\n".join(self.expressions(self.client))
        for metric in (
            "oxia_client_ops_seconds_bucket",
            "oxia_client_ops_req_seconds_bucket",
            "oxia_client_ops_pending",
            "oxia_client_ops_outstanding_bytes",
            "oxia_client_shard_assignments_count_total",
        ):
            self.assertIn(metric, expressions)
        trace_panel = next(
            panel for panel in self.client["panels"] if panel["title"] == "All sampled client traces"
        )
        self.assertNotIn("status = error", trace_panel["targets"][0]["query"])


class AnnotationTest(unittest.TestCase):
    def test_duration_seconds(self) -> None:
        self.assertEqual(0.25, duration_seconds("250ms"))
        self.assertEqual(120, duration_seconds("2m"))

    def test_annotation_has_range_and_idempotency_tag(self) -> None:
        annotation = annotation_for(
            {
                "kind": "PodChaos",
                "metadata": {
                    "name": "kill-one",
                    "uid": "abc-123",
                    "creationTimestamp": "2026-08-31T01:02:03Z",
                },
                "spec": {"action": "pod-kill", "duration": "10s"},
            },
            "stable",
            "https://github.com/oxia-db/chaos/actions/runs/1",
        )

        self.assertEqual(10_000, annotation["timeEnd"] - annotation["time"])
        self.assertIn("oxia-chaos-event:abc-123", annotation["tags"])
        self.assertIn("pod-kill", annotation["text"])

    def test_workflow_node_annotation_uses_persistent_injection_record(self) -> None:
        node = {
            "kind": "WorkflowNode",
            "metadata": {"name": "pod-kill-abc", "uid": "node-123"},
            "spec": {
                "type": "PodChaos",
                "templateName": "pod-kill",
                "startTime": "2026-08-31T01:02:03Z",
                "deadline": "2026-08-31T01:02:13Z",
                "podChaos": {"action": "pod-kill"},
            },
            "status": {
                "conditions": [
                    {"type": "ChaosInjected", "status": "True", "reason": "ChaosCRCreated"}
                ]
            },
        }

        self.assertTrue(is_injected_workflow_node(node))
        annotation = annotation_for(node, "stable", "")

        self.assertEqual(10_000, annotation["timeEnd"] - annotation["time"])
        self.assertIn("PodChaos pod-kill: pod-kill on stable", annotation["text"])
        self.assertIn("oxia-chaos-event:node-123", annotation["tags"])


if __name__ == "__main__":
    unittest.main()

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

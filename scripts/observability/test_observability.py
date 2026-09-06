import json
import tempfile
import unittest
import urllib.error
from email.message import Message
from io import BytesIO
from pathlib import Path

from grafana import GrafanaClient
from provision import load_dashboard
from publish_annotations import annotation_for, duration_seconds, is_injected_workflow_node


class GrafanaClientTest(unittest.TestCase):
    @staticmethod
    def response(status: int = 200, body: bytes = b"{}") -> object:
        class Response:
            def __enter__(self) -> object:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def read(self) -> bytes:
                return body

        response = Response()
        response.status = status
        return response

    @staticmethod
    def http_error(
        status: int, body: bytes, retry_after: str | None = None
    ) -> urllib.error.HTTPError:
        headers = Message()
        if retry_after is not None:
            headers["Retry-After"] = retry_after
        return urllib.error.HTTPError(
            "https://grafana.example/api/folders/oxia-chaos",
            status,
            "error",
            headers,
            BytesIO(body),
        )

    def test_retries_loading_instance_and_recovers(self) -> None:
        responses = iter(
            [
                self.http_error(
                    503,
                    b'{"code":"Loading","message":"Your instance is loading"}',
                    retry_after="7",
                ),
                self.response(body=b'{"uid":"oxia-chaos"}'),
            ]
        )
        delays = []

        def open_next(*_args: object, **_kwargs: object) -> object:
            response = next(responses)
            if isinstance(response, BaseException):
                raise response
            return response

        client = GrafanaClient(
            "https://grafana.example",
            "token",
            opener=open_next,
            sleep=delays.append,
        )

        status, body = client.request("GET", "/api/folders/oxia-chaos")

        self.assertEqual(status, 200)
        self.assertEqual(body, {"uid": "oxia-chaos"})
        self.assertEqual(delays, [7.0])

    def test_retries_server_error_with_exponential_backoff(self) -> None:
        attempts = []
        delays = []

        def unavailable(*_args: object, **_kwargs: object) -> object:
            attempts.append(1)
            raise self.http_error(525, b'{"error":"ssl_handshake_failed"}')

        client = GrafanaClient(
            "https://grafana.example",
            "token",
            max_attempts=3,
            retry_delay_seconds=2,
            opener=unavailable,
            sleep=delays.append,
        )

        with self.assertRaisesRegex(RuntimeError, "HTTP 525"):
            client.request("POST", "/api/annotations", {"text": "chaos"})

        self.assertEqual(len(attempts), 3)
        self.assertEqual(delays, [2, 4])

    def test_does_not_retry_authentication_failure(self) -> None:
        attempts = []

        def reject(*_args: object, **_kwargs: object) -> object:
            attempts.append(1)
            raise self.http_error(401, b'{"message":"Unauthorized"}')

        client = GrafanaClient(
            "https://grafana.example",
            "token",
            opener=reject,
            sleep=lambda _delay: self.fail("authentication failures must not be retried"),
        )

        with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
            client.request("GET", "/api/folders/oxia-chaos")

        self.assertEqual(len(attempts), 1)

    def test_rejects_negative_retry_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "retry_delay_seconds"):
            GrafanaClient("https://grafana.example", "token", retry_delay_seconds=-1)


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
        self.client = json.loads((dashboard_dir / "oxia-client.json").read_text(encoding="utf-8"))
        self.servers = {
            path.stem.removeprefix("oxia-server-"): json.loads(
                path.read_text(encoding="utf-8")
            )
            for path in dashboard_dir.glob("oxia-server-*.json")
        }

    @staticmethod
    def expressions(dashboard: dict[str, object]) -> list[str]:
        return [
            target["expr"]
            for panel in DashboardTest.panels(dashboard["panels"])
            for target in panel.get("targets", [])
            if "expr" in target
        ]

    @staticmethod
    def panels(panels: list[dict[str, object]]) -> list[dict[str, object]]:
        flattened = []
        for panel in panels:
            flattened.append(panel)
            flattened.extend(DashboardTest.panels(panel.get("panels", [])))
        return flattened

    def test_dashboards_keep_stable_and_beta_separate(self) -> None:
        for dashboard in (*self.servers.values(), self.client):
            channel = next(
                variable
                for variable in dashboard["templating"]["list"]
                if variable["name"] == "channel"
            )
            self.assertFalse(channel["multi"])
            self.assertFalse(channel["includeAll"])
            self.assertEqual(channel["query"], "stable,beta")
            self.assertTrue(all("$channel" in expression for expression in self.expressions(dashboard)))

    def test_server_dashboard_suite_matches_upstream(self) -> None:
        self.assertEqual(
            {
                "containers",
                "coordinator",
                "golang",
                "grpc",
                "nodes",
                "overview",
                "shards",
            },
            self.servers.keys(),
        )
        for name, dashboard in self.servers.items():
            self.assertEqual(f"oxia-chaos-server-{name}", dashboard["uid"])
            self.assertIn("upstream Oxia dashboard", dashboard["description"])
            self.assertIn("upstream-oxia", dashboard["tags"])

    def test_server_dashboards_cover_upstream_subsystems(self) -> None:
        expressions = "\n".join(
            expression
            for dashboard in self.servers.values()
            for expression in self.expressions(dashboard)
        )
        for metric in (
            "oxia_server_db_puts_count_total",
            "oxia_server_wal_sync_latency_milliseconds_bucket",
            "oxia_server_kv_pebble_compaction_debt_bytes",
            "oxia_coordinator_leader_election_latency_milliseconds_bucket",
            "grpc_server_handled_total",
            "go_gc_duration_seconds_sum",
            "container_cpu_usage",
        ):
            self.assertIn(metric, expressions)

    def test_server_dashboards_remove_incompatible_upstream_schema(self) -> None:
        expressions = "\n".join(
            expression
            for dashboard in self.servers.values()
            for expression in self.expressions(dashboard)
        )
        for legacy_token in (
            "oxia_cluster",
            "$cluster",
            "kubernetes_pod_name",
            "app_kubernetes_io_component",
            "grpc_server_client_total",
            "oxia_server_db_puts_total",
            "[1m]",
        ):
            self.assertNotIn(legacy_token, expressions)

    def test_server_overview_keeps_logs_and_channel_annotations(self) -> None:
        overview = self.servers["overview"]
        panel_titles = {panel["title"] for panel in self.panels(overview["panels"])}
        self.assertIn("Errors, elections, and recovery", panel_titles)
        self.assertIn("All server and coordinator logs", panel_titles)
        annotation_tags = overview["annotations"]["list"][0]["target"]["tags"]
        self.assertIn("channel:$channel", annotation_tags)

    def test_container_dashboard_includes_servers_and_clients(self) -> None:
        container = self.servers["containers"]
        expressions = "\n".join(self.expressions(container))
        self.assertIn('k8s_pod_name=~"oxia-chaos-$channel-.*"', expressions)
        self.assertIn("k8s_container_name", expressions)
        legends = [
            target.get("legendFormat", "")
            for panel in self.panels(container["panels"])
            for target in panel.get("targets", [])
        ]
        self.assertTrue(any("{{k8s_container_name}}" in legend for legend in legends))

    def test_client_dashboard_exposes_sdk_metrics_without_resource_or_log_panels(self) -> None:
        expressions = "\n".join(self.expressions(self.client))
        for metric in (
            "oxia_client_ops_seconds_bucket",
            "oxia_client_ops_req_seconds_bucket",
            "oxia_client_ops_pending",
            "oxia_client_ops_outstanding_bytes",
            "oxia_client_shard_assignments_count_total",
        ):
            self.assertIn(metric, expressions)
        panel_titles = {panel["title"] for panel in self.panels(self.client["panels"])}
        for removed_title in (
            "Runner resources",
            "Runner CPU",
            "Runner resident memory",
            "Logs and traces",
            "Runner logs",
            "Slow and failed client traces",
            "All sampled client traces",
        ):
            self.assertNotIn(removed_title, panel_titles)
        variable_names = {
            variable["name"] for variable in self.client["templating"]["list"]
        }
        self.assertNotIn("logs", variable_names)
        self.assertNotIn("traces", variable_names)
        annotation_tags = self.client["annotations"]["list"][0]["target"]["tags"]
        self.assertIn("channel:$channel", annotation_tags)


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

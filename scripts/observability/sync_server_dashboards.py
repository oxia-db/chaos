#!/usr/bin/env python3
"""Adapt the upstream Oxia dashboards for the chaos Grafana Cloud stack."""

from __future__ import annotations

import argparse
import copy
import json
import re
from pathlib import Path
from typing import Any


UPSTREAM_DASHBOARDS = (
    "containers",
    "coordinator",
    "golang",
    "grpc",
    "nodes",
    "overview",
    "shards",
)

DISPLAY_NAMES = {
    "containers": "Containers",
    "coordinator": "Coordinator",
    "golang": "Go Runtime",
    "grpc": "gRPC",
    "nodes": "Nodes",
    "overview": "Overview",
    "shards": "Shards",
}

METRIC_RENAMES = {
    "grpc_server_client_total": "grpc_client_handled_total",
    "oxia_coordinator_leader_election_failed_total": (
        "oxia_coordinator_leader_election_failed_count_total"
    ),
    "oxia_coordinator_node_running": "oxia_coordinator_node_running_count",
    "oxia_server_db_delete_ranges_total": "oxia_server_db_delete_ranges_count_total",
    "oxia_server_db_deletes_total": "oxia_server_db_deletes_count_total",
    "oxia_server_db_gets_total": "oxia_server_db_gets_count_total",
    "oxia_server_db_lists_total": "oxia_server_db_lists_count_total",
    "oxia_server_db_puts_total": "oxia_server_db_puts_count_total",
    "oxia_server_kv_pebble_block_cache_hits": (
        "oxia_server_kv_pebble_block_cache_hits_count"
    ),
    "oxia_server_kv_pebble_block_cache_misses": (
        "oxia_server_kv_pebble_block_cache_misses_count"
    ),
    "oxia_server_kv_pebble_compactions_total": (
        "oxia_server_kv_pebble_compactions_total_count"
    ),
    "oxia_server_kv_pebble_num_files_total": (
        "oxia_server_kv_pebble_num_files_total_count"
    ),
    "oxia_server_kv_pebble_read_iterators": (
        "oxia_server_kv_pebble_read_iterators_value"
    ),
    "oxia_server_kv_read_errors_total": "oxia_server_kv_read_errors_count_total",
    "oxia_server_kv_write_ops_total": "oxia_server_kv_write_ops_count_total",
    "oxia_server_wal_entries": "oxia_server_wal_entries_count",
}

CHAOS_ANNOTATION = {
    "builtIn": 1,
    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
    "enable": True,
    "hide": False,
    "iconColor": "red",
    "name": "Chaos injections",
    "target": {
        "limit": 100,
        "matchAny": False,
        "tags": ["oxia-chaos", "chaos", "channel:$channel"],
        "type": "tags",
    },
    "type": "dashboard",
}


def metrics_variable() -> dict[str, Any]:
    return {
        "current": {},
        "hide": 0,
        "includeAll": False,
        "label": "Metrics",
        "multi": False,
        "name": "metrics",
        "options": [],
        "query": "prometheus",
        "refresh": 1,
        "regex": "",
        "type": "datasource",
    }


def logs_variable() -> dict[str, Any]:
    return {
        "current": {},
        "hide": 0,
        "includeAll": False,
        "label": "Logs",
        "multi": False,
        "name": "logs",
        "options": [],
        "query": "loki",
        "refresh": 1,
        "regex": "",
        "type": "datasource",
    }


def channel_variable() -> dict[str, Any]:
    return {
        "current": {"selected": True, "text": "stable", "value": "stable"},
        "hide": 0,
        "includeAll": False,
        "label": "Channel",
        "multi": False,
        "name": "channel",
        "options": [
            {"selected": True, "text": "stable", "value": "stable"},
            {"selected": False, "text": "beta", "value": "beta"},
        ],
        "query": "stable,beta",
        "type": "custom",
    }


def query_variable(
    name: str,
    label: str,
    query: str,
    *,
    all_value: str = ".*",
) -> dict[str, Any]:
    return {
        "allValue": all_value,
        "current": {"selected": True, "text": "All", "value": "$__all"},
        "datasource": {"type": "prometheus", "uid": "${metrics}"},
        "definition": query,
        "hide": 0,
        "includeAll": True,
        "label": label,
        "multi": True,
        "name": name,
        "options": [],
        "query": {"query": query, "refId": "StandardVariableQuery"},
        "refresh": 2,
        "regex": "",
        "sort": 1,
        "type": "query",
    }


def dashboard_variables(name: str) -> list[dict[str, Any]]:
    variables = [metrics_variable(), channel_variable()]

    if name == "containers":
        variables.extend(
            [
                query_variable(
                    "k8s_namespace",
                    "Kubernetes namespace",
                    'label_values(container_cpu_time_seconds_total{'
                    'k8s_pod_name=~"oxia-chaos-$channel-.*"}, k8s_namespace_name)',
                ),
                query_variable(
                    "component",
                    "Component",
                    'label_values(container_cpu_time_seconds_total{'
                    'k8s_pod_name=~"oxia-chaos-$channel-.*"}, k8s_container_name)',
                ),
                query_variable(
                    "pod",
                    "Pod",
                    'label_values(container_cpu_time_seconds_total{'
                    'k8s_pod_name=~"oxia-chaos-$channel-.*",'
                    'k8s_namespace_name=~"$k8s_namespace",'
                    'k8s_container_name=~"$component"}, k8s_pod_name)',
                ),
            ]
        )
        return variables

    if name in {"golang", "grpc"}:
        base_metric = "process_resident_memory_bytes" if name == "golang" else "grpc_server_handled_total"
        variables.extend(
            [
                query_variable(
                    "component",
                    "Component",
                    f'label_values({base_metric}{{channel="$channel",'
                    'component=~"oxia-server|oxia-coordinator"}, component)',
                    all_value="oxia-server|oxia-coordinator",
                ),
                query_variable(
                    "pod",
                    "Pod",
                    f'label_values({base_metric}{{channel="$channel",'
                    'component=~"$component"}, pod)',
                ),
            ]
        )
        if name == "grpc":
            variables.extend(
                [
                    query_variable(
                        "grpc_service",
                        "gRPC service",
                        'label_values(grpc_server_handled_total{channel="$channel",'
                        'component=~"$component",pod=~"$pod"}, grpc_service)',
                    ),
                    query_variable(
                        "grpc_method",
                        "gRPC method",
                        'label_values(grpc_server_handled_total{channel="$channel",'
                        'component=~"$component",pod=~"$pod",'
                        'grpc_service=~"$grpc_service"}, grpc_method)',
                    ),
                ]
            )
        return variables

    namespace_query = (
        'label_values(oxia_server_wal_entries_count{channel="$channel"}, oxia_namespace)'
    )
    shard_query = (
        'label_values(oxia_server_wal_entries_count{channel="$channel",'
        'oxia_namespace=~"$namespace"}, shard)'
    )

    if name == "overview":
        variables.append(query_variable("namespace", "Namespace", namespace_query))
    elif name == "coordinator":
        variables.extend(
            [
                query_variable("namespace", "Namespace", namespace_query),
                query_variable("shard", "Shard", shard_query),
            ]
        )
    elif name == "nodes":
        variables.extend(
            [
                query_variable(
                    "pod",
                    "Pod",
                    'label_values(oxia_server_wal_entries_count{channel="$channel"}, pod)',
                ),
                query_variable(
                    "namespace",
                    "Namespace",
                    'label_values(oxia_server_wal_entries_count{channel="$channel",'
                    'pod=~"$pod"}, oxia_namespace)',
                ),
                query_variable(
                    "shard",
                    "Shard",
                    'label_values(oxia_server_wal_entries_count{channel="$channel",'
                    'pod=~"$pod",oxia_namespace=~"$namespace"}, shard)',
                ),
            ]
        )
    elif name == "shards":
        variables.extend(
            [
                query_variable("namespace", "Namespace", namespace_query),
                query_variable("shard", "Shard", shard_query),
                query_variable(
                    "pod",
                    "Pod",
                    'label_values(oxia_server_wal_entries_count{channel="$channel",'
                    'oxia_namespace=~"$namespace",shard=~"$shard"}, pod)',
                ),
            ]
        )
    else:
        raise ValueError(f"unsupported upstream dashboard {name}")
    return variables


def adapt_text(value: str) -> str:
    for old_name in sorted(METRIC_RENAMES, key=len, reverse=True):
        value = value.replace(old_name, METRIC_RENAMES[old_name])
    value = re.sub(
        r'oxia_cluster\s*=~?\s*"\$cluster"', 'channel="$channel"', value
    )
    value = value.replace("app_kubernetes_io_component", "component")
    value = value.replace("kubernetes_pod_name", "pod")
    value = value.replace("${DataSource}", "${metrics}")
    value = value.replace("[1m]", "[$__rate_interval]")
    value = re.sub(r",\s*}", "}", value)
    return value


def adapt_tree(value: Any) -> Any:
    if isinstance(value, str):
        return adapt_text(value)
    if isinstance(value, list):
        return [adapt_tree(item) for item in value]
    if isinstance(value, dict):
        adapted = {key: adapt_tree(item) for key, item in value.items()}
        datasource = adapted.get("datasource")
        if isinstance(datasource, dict) and datasource.get("type") == "prometheus":
            datasource["uid"] = "${metrics}"
        return adapted
    return value


def panel_by_title(dashboard: dict[str, Any], title: str) -> dict[str, Any]:
    for panel in dashboard["panels"]:
        if panel.get("title") == title:
            return panel
    raise ValueError(f"{dashboard['title']} is missing panel {title}")


def adapt_container_panels(dashboard: dict[str, Any]) -> None:
    selector = (
        'k8s_pod_name=~"oxia-chaos-$channel-.*",'
        'k8s_namespace_name=~"$k8s_namespace",'
        'k8s_container_name=~"$component",k8s_pod_name=~"$pod"'
    )
    pod_selector = (
        'k8s_pod_name=~"oxia-chaos-$channel-.*",'
        'k8s_namespace_name=~"$k8s_namespace",k8s_pod_name=~"$pod"'
    )

    cpu_panel = panel_by_title(dashboard, "CPU usage")
    cpu_panel["title"] = "CPU usage and allocation"
    cpu_panel["targets"] = [
        {
            "expr": "100 * sum by (k8s_pod_name, k8s_container_name) "
            f"(container_cpu_usage{{{selector}}})",
            "legendFormat": "{{k8s_pod_name}} / {{k8s_container_name}} · usage",
            "refId": "A",
        },
        {
            "expr": "100 * sum by (k8s_pod_name, k8s_container_name) "
            f"(k8s_container_cpu_request{{{selector}}})",
            "legendFormat": "{{k8s_pod_name}} / {{k8s_container_name}} · request",
            "refId": "B",
        },
        {
            "expr": "100 * sum by (k8s_pod_name, k8s_container_name) "
            f"(k8s_container_cpu_limit{{{selector}}})",
            "legendFormat": "{{k8s_pod_name}} / {{k8s_container_name}} · limit",
            "refId": "C",
        },
    ]

    memory_panel = panel_by_title(dashboard, "Memory")
    memory_metrics = (
        ("container_memory_rss_bytes", "RSS"),
        ("container_memory_usage_bytes", "Usage"),
        ("k8s_container_memory_limit_bytes", "Limit"),
        ("k8s_container_memory_request_bytes", "Request"),
        ("container_memory_working_set_bytes", "Working set"),
        ("container_memory_available_bytes", "Available"),
    )
    memory_panel["targets"] = [
        {
            "expr": "sum by (k8s_pod_name, k8s_container_name) "
            f"({metric}{{{selector}}})",
            "legendFormat": (
                "{{k8s_pod_name}} / {{k8s_container_name}} · " + legend.lower()
            ),
            "refId": chr(ord("A") + index),
        }
        for index, (metric, legend) in enumerate(memory_metrics)
    ]

    network_panel = panel_by_title(dashboard, "Network")
    network_panel["targets"] = [
        {
            "expr": "8 * sum by (k8s_pod_name) (rate("
            f'k8s_pod_network_io_bytes_total{{{pod_selector},direction="receive"}}'
            "[$__rate_interval]))",
            "legendFormat": "{{k8s_pod_name}} / network · in",
            "refId": "A",
        },
        {
            "expr": "-8 * sum by (k8s_pod_name) (rate("
            f'k8s_pod_network_io_bytes_total{{{pod_selector},direction="transmit"}}'
            "[$__rate_interval]))",
            "legendFormat": "{{k8s_pod_name}} / network · out",
            "refId": "B",
        },
    ]

    disk_panel = panel_by_title(dashboard, "Disk")
    disk_panel["title"] = "Filesystem"
    disk_panel["fieldConfig"] = {
        "defaults": {"unit": "bytes"},
        "overrides": [],
    }
    disk_panel["targets"] = [
        {
            "expr": f"sum by (k8s_pod_name) ({metric}{{{pod_selector}}})",
            "legendFormat": f"{{{{k8s_pod_name}}}} / filesystem · {legend}",
            "refId": chr(ord("A") + index),
        }
        for index, (metric, legend) in enumerate(
            (
                ("k8s_pod_filesystem_usage_bytes", "used"),
                ("k8s_pod_filesystem_available_bytes", "available"),
                ("k8s_pod_filesystem_capacity_bytes", "capacity"),
            )
        )
    ]


def adapt_go_panels(dashboard: dict[str, Any]) -> None:
    selector = 'channel="$channel",component=~"$component",pod=~"$pod"'
    mean_panel = panel_by_title(dashboard, "GC duration 50pct")
    mean_panel["title"] = "Mean GC duration"
    mean_panel["targets"][0]["expr"] = (
        "sum by (pod) (rate(go_gc_duration_seconds_sum{"
        + selector
        + "}[$__rate_interval])) / clamp_min(sum by (pod) "
        "(rate(go_gc_duration_seconds_count{"
        + selector
        + "}[$__rate_interval])), 1e-9)"
    )
    mean_panel["targets"][0]["legendFormat"] = "{{pod}}"

    cycles_panel = panel_by_title(dashboard, "GC duration 75pct")
    cycles_panel["title"] = "GC cycles / s"
    cycles_panel["targets"][0]["expr"] = (
        "sum by (pod) (rate(go_gc_duration_seconds_count{"
        + selector
        + "}[$__rate_interval]))"
    )
    cycles_panel["targets"][0]["legendFormat"] = "{{pod}}"
    cycles_panel["fieldConfig"]["defaults"]["unit"] = "ops"

    target_panel = panel_by_title(dashboard, "GC duration Max")
    target_panel["title"] = "GOGC target"
    target_panel["targets"][0]["expr"] = "go_gc_gogc_percent{" + selector + "}"
    target_panel["targets"][0]["legendFormat"] = "{{pod}}"
    target_panel["fieldConfig"]["defaults"]["unit"] = "percent"


def adapt_special_panels(name: str, dashboard: dict[str, Any]) -> None:
    if name == "containers":
        adapt_container_panels(dashboard)
    elif name == "golang":
        adapt_go_panels(dashboard)
    elif name == "overview":
        health_panel = panel_by_title(dashboard, "Healthy Server Pods")
        health_panel["targets"][0]["expr"] = (
            'sum(oxia_coordinator_node_running_count{channel="$channel"}) or vector(0)'
        )
    elif name == "coordinator":
        status_panel = panel_by_title(dashboard, "Nodes status")
        status_panel["targets"][0]["expr"] = (
            'sum by (data_server) (oxia_coordinator_node_running_count{channel="$channel"})'
        )
        status_panel["targets"][0]["legendFormat"] = "{{data_server}}"
        failed_panel = panel_by_title(dashboard, "Failed Leader Elections")
        failed_panel["targets"][0]["expr"] += " or on() vector(0)"

    if name == "grpc":
        for title in (
            "gRPC Server - Failed requests /s",
            "gRPC Client - Failed requests /s",
        ):
            failed_panel = panel_by_title(dashboard, title)
            failed_panel["description"] = "Zero means no failed RPCs matched the interval."
            failed_panel["targets"][0]["expr"] += " or on() vector(0)"

    if name in {"nodes", "shards"}:
        read_errors = panel_by_title(dashboard, "Read Errors")
        read_errors["targets"][0]["expr"] += " or on() vector(0)"


def dashboard_links() -> list[dict[str, Any]]:
    return [
        {
            "asDropdown": True,
            "icon": "external link",
            "includeVars": True,
            "keepTime": True,
            "tags": ["oxia-chaos-server"],
            "targetBlank": False,
            "title": "Server dashboards",
            "type": "dashboards",
        },
        {
            "asDropdown": False,
            "icon": "external link",
            "includeVars": False,
            "keepTime": True,
            "tags": [],
            "targetBlank": False,
            "title": "Stable overview",
            "type": "link",
            "url": "/d/oxia-chaos-server-overview/oxia-chaos-server-overview?var-channel=stable",
        },
        {
            "asDropdown": False,
            "icon": "external link",
            "includeVars": False,
            "keepTime": True,
            "tags": [],
            "targetBlank": False,
            "title": "Beta overview",
            "type": "link",
            "url": "/d/oxia-chaos-server-overview/oxia-chaos-server-overview?var-channel=beta",
        },
        {
            "asDropdown": False,
            "icon": "external link",
            "includeVars": False,
            "keepTime": True,
            "tags": [],
            "targetBlank": False,
            "title": "Stable client",
            "type": "link",
            "url": "/d/oxia-chaos-client/oxia-chaos-client?var-channel=stable&var-runner=java&var-case=basic-kv",
        },
        {
            "asDropdown": False,
            "icon": "external link",
            "includeVars": False,
            "keepTime": True,
            "tags": [],
            "targetBlank": False,
            "title": "Beta client",
            "type": "link",
            "url": "/d/oxia-chaos-client/oxia-chaos-client?var-channel=beta&var-runner=java&var-case=basic-kv",
        },
    ]


def append_log_panels(dashboard: dict[str, Any]) -> None:
    maximum_id = max(panel.get("id", 0) for panel in dashboard["panels"])
    maximum_y = max(
        panel.get("gridPos", {}).get("y", 0) + panel.get("gridPos", {}).get("h", 0)
        for panel in dashboard["panels"]
    )
    dashboard["panels"].extend(
        [
            {
                "collapsed": False,
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": maximum_y},
                "id": maximum_id + 1,
                "panels": [],
                "title": "Server logs",
                "type": "row",
            },
            {
                "datasource": {"type": "loki", "uid": "${logs}"},
                "gridPos": {"h": 10, "w": 12, "x": 0, "y": maximum_y + 1},
                "id": maximum_id + 2,
                "options": {
                    "dedupStrategy": "none",
                    "enableInfiniteScrolling": True,
                    "enableLogDetails": True,
                    "prettifyLogMessage": False,
                    "showCommonLabels": False,
                    "showLabels": False,
                    "showTime": True,
                    "sortOrder": "Descending",
                    "wrapLogMessage": True,
                },
                "targets": [
                    {
                        "editorMode": "code",
                        "expr": '{k8s_namespace_name="oxia-chaos",k8s_pod_name=~"oxia-chaos-$channel-[0-9]+",k8s_container_name=~"server|coordinator"} |~ `(?i)(error|panic|fatal|leader|election|recover|closed)`',
                        "queryType": "range",
                        "refId": "A",
                    }
                ],
                "title": "Errors, elections, and recovery",
                "type": "logs",
            },
            {
                "datasource": {"type": "loki", "uid": "${logs}"},
                "gridPos": {"h": 10, "w": 12, "x": 12, "y": maximum_y + 1},
                "id": maximum_id + 3,
                "options": {
                    "dedupStrategy": "none",
                    "enableInfiniteScrolling": True,
                    "enableLogDetails": True,
                    "prettifyLogMessage": False,
                    "showCommonLabels": False,
                    "showLabels": False,
                    "showTime": True,
                    "sortOrder": "Descending",
                    "wrapLogMessage": True,
                },
                "targets": [
                    {
                        "editorMode": "code",
                        "expr": '{k8s_namespace_name="oxia-chaos",k8s_pod_name=~"oxia-chaos-$channel-[0-9]+",k8s_container_name=~"server|coordinator"}',
                        "queryType": "range",
                        "refId": "A",
                    }
                ],
                "title": "All server and coordinator logs",
                "type": "logs",
            },
        ]
    )


def adapt_dashboard(
    name: str, dashboard: dict[str, Any], source_ref: str
) -> dict[str, Any]:
    adapted = adapt_tree(copy.deepcopy(dashboard))
    adapted["id"] = None
    adapted["uid"] = f"oxia-chaos-server-{name}"
    adapted["title"] = f"Oxia Chaos / Server / {DISPLAY_NAMES[name]}"
    adapted["description"] = (
        "Adapted from the upstream Oxia dashboard at "
        f"oxia-db/oxia@{source_ref}. Queries are translated for the chaos "
        "Grafana Cloud OTLP metric schema and isolated by Stable/Beta channel."
    )
    if name == "containers":
        adapted["description"] += (
            " Includes Oxia server, coordinator, and testcase client containers."
        )
    adapted["tags"] = sorted(
        set(adapted.get("tags", []))
        | {"oxia", "oxia-chaos", "oxia-chaos-server", "upstream-oxia"}
    )
    adapted["links"] = dashboard_links()
    adapted["annotations"] = {"list": [copy.deepcopy(CHAOS_ANNOTATION)]}
    adapted["refresh"] = "1m"
    adapted["version"] = 1
    adapted["templating"] = {"list": dashboard_variables(name)}
    adapt_special_panels(name, adapted)
    if name == "overview":
        adapted["templating"]["list"].insert(1, logs_variable())
        append_log_panels(adapted)
    return adapted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "observability" / "dashboards",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name in UPSTREAM_DASHBOARDS:
        source_path = args.source_dir / f"oxia-{name}.json"
        if not source_path.is_file():
            raise ValueError(f"missing upstream dashboard {source_path}")
        with source_path.open(encoding="utf-8") as source_file:
            source_dashboard = json.load(source_file)
        dashboard = adapt_dashboard(name, source_dashboard, args.source_ref)
        output_path = args.output_dir / f"oxia-server-{name}.json"
        output_path.write_text(
            json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Generated {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

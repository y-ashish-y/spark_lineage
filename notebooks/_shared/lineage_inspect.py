"""Helpers for inspecting Spline lineage via the Consumer REST API."""

from __future__ import annotations

import json
import urllib.request

CONSUMER_URL = "http://spline-rest:8080/consumer"
SPLINE_UI_URL = "http://localhost:9090"


def _fetch_json(path: str) -> dict | list:
    with urllib.request.urlopen(f"{CONSUMER_URL}{path}") as resp:
        return json.loads(resp.read())


def list_recent_events(limit: int = 8) -> list[dict]:
    """Print recent execution events and return the full list."""
    data = _fetch_json("/execution-events")
    items = data.get("items", data)
    print(f"captured events: {len(items)}")
    for event in items[:limit]:
        name = event.get("dataSourceName") or event.get("name")
        dtype = event.get("dataSourceType")
        plan_id = event.get("executionPlanId")
        print(f" - {name} ({dtype}) | plan {plan_id}")
    return items


def upstream_column_names(attribute_id: str) -> list[str]:
    """Return source column names for a Spline attribute id."""
    data = _fetch_json(f"/attribute-lineage-and-impact?attributeId={attribute_id}")
    lineage = data.get("lineage", {})
    nodes = {n["_id"]: n["name"] for n in lineage.get("nodes", [])}
    return [
        nodes[e["target"]]
        for e in lineage.get("edges", [])
        if e["source"] == attribute_id and e["target"] in nodes
    ]


def find_attribute_id(plan_id: str, column_name: str) -> str | None:
    """Pick the first attribute id for column_name on an execution plan."""
    plan = _fetch_json(f"/execution-plans/{plan_id}")
    attrs = plan["executionPlan"]["extra"]["attributes"]
    for attr in attrs:
        if attr["name"] == column_name:
            return attr["id"]
    return None


def inspect_column_lineage(plan_id: str, column_name: str) -> None:
    """Print upstream columns for one output column on an execution plan."""
    attr_id = find_attribute_id(plan_id, column_name)
    if not attr_id:
        print(f"column not found on plan: {column_name}")
        return
    upstream = upstream_column_names(attr_id)
    print(f"{column_name} <- {upstream}")
    print(f"UI: {SPLINE_UI_URL}/app/execution-plans/{plan_id}")


def inspect_event_column(data_source_name: str, column_name: str) -> None:
    """Find the latest event for a sink name and print column lineage."""
    data = _fetch_json("/execution-events")
    items = data.get("items", data)
    for event in items:
        if event.get("dataSourceName") == data_source_name:
            plan_id = event["executionPlanId"]
            print(f"event: {data_source_name} | plan {plan_id}")
            inspect_column_lineage(plan_id, column_name)
            return
    print(f"no execution event for data source: {data_source_name}")

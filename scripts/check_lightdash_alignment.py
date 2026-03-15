#!/usr/bin/env python3
"""Check dbt manifest vs Lightdash cached explores alignment.

Validates for each Lightdash explore/base table:
- dbt model exists in manifest
- all dbt model columns are present as Lightdash dimensions
- all dbt column-level meta.metrics are present as Lightdash metrics
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TableCheck:
    table: str
    missing_dimensions: list[str] = field(default_factory=list)
    missing_metrics: list[str] = field(default_factory=list)
    model_not_found: bool = False

    @property
    def ok(self) -> bool:
        return (
            not self.model_not_found
            and not self.missing_dimensions
            and not self.missing_metrics
        )


def run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip()


def load_manifest(manifest_path: Path) -> dict:
    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_lightdash_explores(project_uuid: str) -> list[dict]:
    query = (
        "select explores::text "
        "from cached_explores "
        f"where project_uuid='{project_uuid}';"
    )
    out = run(
        [
            "docker",
            "exec",
            "lightdash_postgres",
            "psql",
            "-U",
            "lightdash",
            "-d",
            "lightdash",
            "-Atc",
            query,
        ]
    )
    if not out:
        raise RuntimeError(
            f"No cached_explores row found for project_uuid={project_uuid}"
        )
    return json.loads(out)


def gather_dbt_models(manifest: dict) -> dict[str, dict]:
    models: dict[str, dict] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("package_name") != "wolt_assignment_dbt":
            continue
        models[node["name"]] = node
    return models


def expected_meta_metrics(model_node: dict) -> set[str]:
    expected: set[str] = set()
    for col in model_node.get("columns", {}).values():
        meta = col.get("meta", {}) or {}
        for metric in meta.get("metrics", []) or []:
            if isinstance(metric, str):
                name = metric
            elif isinstance(metric, dict):
                name = metric.get("name")
            else:
                name = None
            if name:
                expected.add(name)
    return expected


def check_alignment(models: dict[str, dict], explores: list[dict]) -> list[TableCheck]:
    checks: list[TableCheck] = []
    for explore in explores:
        base_table = explore.get("baseTable")
        if not base_table:
            continue

        c = TableCheck(table=base_table)
        model = models.get(base_table)
        if not model:
            c.model_not_found = True
            checks.append(c)
            continue

        table_obj = (explore.get("tables", {}) or {}).get(base_table, {})
        ld_dimensions = set((table_obj.get("dimensions", {}) or {}).keys())
        ld_metrics = set((table_obj.get("metrics", {}) or {}).keys())

        dbt_columns = set((model.get("columns", {}) or {}).keys())
        meta_metrics = expected_meta_metrics(model)

        c.missing_dimensions = sorted(dbt_columns - ld_dimensions)
        c.missing_metrics = sorted(meta_metrics - ld_metrics)
        checks.append(c)
    return checks


def write_report(checks: list[TableCheck], project_uuid: str, output_path: Path) -> None:
    ok_count = sum(1 for c in checks if c.ok)
    fail_count = len(checks) - ok_count

    lines = [
        "# Lightdash Alignment Report",
        "",
        f"- project_uuid: `{project_uuid}`",
        f"- explores_checked: `{len(checks)}`",
        f"- aligned: `{ok_count}`",
        f"- with_issues: `{fail_count}`",
        "",
        "## Details",
        "",
    ]

    for c in sorted(checks, key=lambda x: x.table):
        if c.ok:
            lines.append(f"- `{c.table}`: OK")
            continue
        lines.append(f"- `{c.table}`: ISSUE")
        if c.model_not_found:
            lines.append("  - model_not_found_in_manifest: true")
        if c.missing_dimensions:
            lines.append(
                "  - missing_dimensions: "
                + ", ".join(f"`{x}`" for x in c.missing_dimensions)
            )
        if c.missing_metrics:
            lines.append(
                "  - missing_meta_metrics: "
                + ", ".join(f"`{x}`" for x in c.missing_metrics)
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = repo_root / "wolt_assignment_dbt" / "target" / "manifest.json"
    report_path = repo_root / "docs" / "LIGHTDASH_ALIGNMENT_REPORT.md"

    if not manifest_path.exists():
        print(f"Missing manifest: {manifest_path}", file=sys.stderr)
        return 2

    project_uuid = os.environ.get(
        "LIGHTDASH_PROJECT_UUID", "7644af5b-1af9-4377-9bfd-0a826aaad8fd"
    )

    manifest = load_manifest(manifest_path)
    models = gather_dbt_models(manifest)
    explores = load_lightdash_explores(project_uuid)
    checks = check_alignment(models, explores)
    write_report(checks, project_uuid, report_path)

    fail_count = sum(1 for c in checks if not c.ok)
    print(
        f"Alignment check complete. explores={len(checks)} "
        f"ok={len(checks)-fail_count} issues={fail_count}"
    )
    print(f"Report: {report_path}")
    return 1 if fail_count else 0


if __name__ == "__main__":
    sys.exit(main())

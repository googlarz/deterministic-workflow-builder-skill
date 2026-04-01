#!/usr/bin/env python3
"""Migrate workflow manifests to the latest schema version."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from init_deterministic_workflow import DEFAULT_ALLOWLISTED_COMMANDS
from workflow_schema import SCHEMA_VERSION, load_manifest, resolve_workflow_dir


def migrate_contracts(entries: list[object]) -> list[object]:
    migrated: list[object] = []
    for entry in entries:
        if isinstance(entry, str):
            migrated.append(
                {
                    "type": "file",
                    "path": entry,
                    "required": True,
                    "retention": {"days": 30},
                }
            )
        else:
            migrated.append(entry)
    return migrated


def migrate_manifest(manifest: dict[str, object]) -> tuple[dict[str, object], list[str]]:
    changes: list[str] = []
    original_version = int(manifest.get("schema_version", 2))
    if original_version >= SCHEMA_VERSION:
        manifest.setdefault("migrations", {"current_from": None})
        return manifest, changes

    manifest["schema_version"] = SCHEMA_VERSION
    manifest.setdefault("goal", "TODO")
    manifest.setdefault("policy_pack", "strict-prod")
    manifest.setdefault("policy", {})
    manifest.setdefault("inputs", [])
    manifest.setdefault("outputs", [])
    manifest.setdefault("graph", {"execution_model": "dag"})
    manifest.setdefault("environment", {"network_mode": "inherit"})
    manifest.setdefault("tooling", {"allowlisted_commands": list(DEFAULT_ALLOWLISTED_COMMANDS)})
    manifest.setdefault("audit", {"enabled": True, "directory": "audit/runs"})
    manifest["migrations"] = {"current_from": original_version}
    changes.append(f"Upgraded schema_version from {original_version} to {SCHEMA_VERSION}")

    steps = manifest.get("steps", [])
    if isinstance(steps, list):
        previous_step_id: str | None = None
        for step in steps:
            if not isinstance(step, dict):
                continue
            step.setdefault("gate_type", "artifact")
            step.setdefault("timeout_seconds", 1800)
            step.setdefault("executor_config", {})
            step.setdefault("depends_on", [] if previous_step_id is None else [previous_step_id])
            step["produces"] = migrate_contracts(step.get("produces", []))
            step["consumes"] = migrate_contracts(step.get("consumes", []))
            if step.get("success_gate") in (None, "", "TODO"):
                step["success_gate"] = {
                    "type": "file_exists",
                    "path": f"artifacts/{step['id']}.done",
                }
            previous_step_id = (
                step.get("id") if isinstance(step.get("id"), str) else previous_step_id
            )

    sidecars = manifest.get("sidecars", [])
    if isinstance(sidecars, list):
        for sidecar in sidecars:
            if not isinstance(sidecar, dict):
                continue
            sidecar.setdefault("output_schema", {"type": "object", "required_keys": ["summary"]})
            sidecar.setdefault("validator", "json-object-required-keys")

    return manifest, changes


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate a deterministic workflow manifest to the latest schema."
    )
    parser.add_argument("path", help="Workflow directory or workflow.json path.")
    parser.add_argument(
        "--write", action="store_true", help="Write the migrated manifest back to workflow.json."
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workflow_dir = resolve_workflow_dir(Path(args.path))
    manifest_path = workflow_dir / "workflow.json"
    manifest = load_manifest(manifest_path)
    migrated, changes = migrate_manifest(manifest)
    if args.write:
        manifest_path.write_text(json.dumps(migrated, indent=2) + "\n", encoding="utf-8")
        print(f"[OK] Migrated {manifest_path} to schema {SCHEMA_VERSION}")
    else:
        print(json.dumps(migrated, indent=2))
    for change in changes:
        print(f"[CHANGE] {change}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

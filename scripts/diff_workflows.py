#!/usr/bin/env python3
"""Compare two workflow manifests and explain semantic changes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from workflow_schema import (
    load_manifest,
    resolve_workflow_dir,
    simulate_step_order,
    summarize_sidecars,
)

STEP_DETAIL_FIELDS = ("type", "gate_type", "requires_approval", "retry_limit", "timeout_seconds")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two deterministic workflows.")
    parser.add_argument("before", help="Old workflow directory or manifest path.")
    parser.add_argument("after", help="New workflow directory or manifest path.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    before_dir = resolve_workflow_dir(Path(args.before))
    after_dir = resolve_workflow_dir(Path(args.after))
    before_manifest = load_manifest(before_dir / "workflow.json")
    after_manifest = load_manifest(after_dir / "workflow.json")

    before_steps = simulate_step_order(before_manifest)
    after_steps = simulate_step_order(after_manifest)
    before_sidecars = [item["id"] for item in summarize_sidecars(before_manifest)]
    after_sidecars = [item["id"] for item in summarize_sidecars(after_manifest)]

    print(
        f"Policy pack: {before_manifest.get('policy_pack')} -> {after_manifest.get('policy_pack')}"
    )
    if before_steps != after_steps:
        print("Execution semantics changed:")
        print("before:", ",".join(before_steps))
        print("after :", ",".join(after_steps))
    else:
        print("Step order unchanged.")

    added_steps = [step for step in after_steps if step not in before_steps]
    removed_steps = [step for step in before_steps if step not in after_steps]
    if added_steps:
        print("Added steps:", ", ".join(added_steps))
    if removed_steps:
        print("Removed steps:", ", ".join(removed_steps))

    before_step_map = {
        step["id"]: step for step in before_manifest.get("steps", []) if isinstance(step, dict)
    }
    after_step_map = {
        step["id"]: step for step in after_manifest.get("steps", []) if isinstance(step, dict)
    }
    common_steps = set(before_step_map) & set(after_step_map)
    detail_changes: list[str] = []
    for step_id in sorted(common_steps):
        before_step = before_step_map[step_id]
        after_step = after_step_map[step_id]
        for field in STEP_DETAIL_FIELDS:
            before_val = before_step.get(field)
            after_val = after_step.get(field)
            if before_val != after_val:
                detail_changes.append(f"  {step_id}.{field}: {before_val} -> {after_val}")
    if detail_changes:
        print("Step detail changes:")
        for change in detail_changes:
            print(change)
    elif common_steps:
        print("Step details unchanged for common steps.")

    if before_sidecars != after_sidecars:
        print("Sidecar changes:")
        print("before:", ",".join(before_sidecars) or "none")
        print("after :", ",".join(after_sidecars) or "none")
    else:
        print("Sidecar risk unchanged.")

    before_residual = before_manifest.get("residual_nondeterminism", [])
    after_residual = after_manifest.get("residual_nondeterminism", [])
    if before_residual != after_residual:
        print("Residual nondeterminism changed:")
        print("before:", ", ".join(before_residual))
        print("after :", ", ".join(after_residual))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

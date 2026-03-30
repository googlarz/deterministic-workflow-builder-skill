#!/usr/bin/env python3
"""Verify workflow manifests against the strict schema and simulate execution order."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workflow_schema import Issue, load_manifest, resolve_workflow_dir, simulate_step_order, summarize_sidecars, validate_manifest


def print_issues(issues: list[Issue]) -> None:
    for issue in issues:
        location = f"{issue.path}:{issue.line}" if issue.line else issue.path
        print(f"[{issue.severity.upper()}] {location} - {issue.message}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify a deterministic workflow directory.")
    parser.add_argument("path", help="Workflow directory or a file inside it.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--simulate", action="store_true", help="Print simulated step order and sidecar routing.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workflow_dir = resolve_workflow_dir(Path(args.path))
    manifest_path = workflow_dir / "workflow.json"

    try:
        manifest = load_manifest(manifest_path)
    except FileNotFoundError:
        issues = [Issue(severity="error", message="Missing workflow.json manifest.", path=str(manifest_path))]
        if args.json:
            print(json.dumps([issue.to_dict() for issue in issues], indent=2))
        else:
            print_issues(issues)
        return 1
    except json.JSONDecodeError as exc:
        issues = [Issue(severity="error", message=f"Invalid JSON: {exc}", path=str(manifest_path))]
        if args.json:
            print(json.dumps([issue.to_dict() for issue in issues], indent=2))
        else:
            print_issues(issues)
        return 1

    issues = validate_manifest(manifest, manifest_path, workflow_dir=workflow_dir)
    policy_pack = manifest.get("policy_pack")
    if isinstance(policy_pack, str):
        policy_path = Path(__file__).resolve().parents[1] / "assets" / "policies" / f"{policy_pack}.json"
        if not policy_path.exists():
            issues.append(Issue(severity="error", message=f"Unknown policy pack `{policy_pack}`.", path=str(policy_path)))

    payload = {
        "workflow_dir": str(workflow_dir),
        "issues": [issue.to_dict() for issue in issues],
    }
    if args.simulate:
        payload["step_order"] = simulate_step_order(manifest)
        payload["sidecars"] = summarize_sidecars(manifest)

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        if issues:
            print_issues(issues)
        else:
            print(f"[OK] {workflow_dir} passed workflow verification.")
        print("[SCHEMA] version=" + str(manifest.get("schema_version", "")))
        if args.simulate:
            print("[SIMULATION] step_order=" + ",".join(payload["step_order"]))
            print("[SIMULATION] policy_pack=" + str(manifest.get("policy_pack", "")))
            if payload["sidecars"]:
                for sidecar in payload["sidecars"]:
                    print(
                        "[SIDECAR] "
                        f"{sidecar['id']} kind={sidecar['kind']} "
                        f"when={sidecar['when']} consumer_step={sidecar['consumer_step']}"
                    )

    return 1 if any(issue.severity == "error" for issue in issues) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

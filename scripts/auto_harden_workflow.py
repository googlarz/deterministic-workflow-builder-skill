#!/usr/bin/env python3
"""Auto-harden a workflow manifest by filling deterministic defaults and safe sidecars."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from compile_workflow import choose_policy_pack, load_sidecar_registry
from init_deterministic_workflow import DEFAULT_ALLOWLISTED_COMMANDS
from workflow_schema import load_manifest, resolve_workflow_dir


def infer_kind_from_steps(step_names: list[str]) -> str:
    joined = " ".join(step_names)
    if any(token in joined for token in ("publish", "release", "deploy")):
        return "release"
    if any(token in joined for token in ("test", "candidate-fixes", "reproduce")):
        return "code-fix"
    if any(token in joined for token in ("draft", "variants", "content")):
        return "content-review"
    if any(token in joined for token in ("extract", "transform", "load", "reconcile")):
        return "etl"
    return "generic"


def make_registry_sidecar(sidecar_id: str, consumer_step: str, when: str) -> dict[str, object]:
    registry = load_sidecar_registry()
    entry = registry[sidecar_id]
    sidecar = {
        "id": sidecar_id,
        "name": sidecar_id.replace("-", " "),
        "kind": entry["kind"],
        "when": when,
        "purpose": entry["purpose"],
        "consumer_step": consumer_step,
        "prompt_asset": entry.get("prompt_asset"),
        "containment": {
            "mode": entry["containment_mode"],
            "enforced_by": "Auto-hardened deterministic containment.",
            "notes": "Sidecar output remains advisory and must pass deterministic gates or approval before adoption.",
        },
        "output_schema": entry["output_schema"],
        "validator": entry["validator"],
    }
    if "prompt_asset" in entry:
        sidecar["prompt_asset"] = entry["prompt_asset"]
    if "prompt_sha256" in entry:
        sidecar["prompt_sha256"] = entry["prompt_sha256"]
    return sidecar


def harden_manifest(manifest: dict[str, object]) -> tuple[dict[str, object], list[str]]:
    changes: list[str] = []
    steps = manifest.get("steps", [])
    if not isinstance(steps, list):
        return manifest, changes

    manifest["schema_version"] = max(int(manifest.get("schema_version", 4)), 4)
    if "policy_pack" not in manifest:
        kind = infer_kind_from_steps(
            [str(step.get("name", "")) for step in steps if isinstance(step, dict)]
        )
        manifest["policy_pack"] = choose_policy_pack(kind, manifest.get("goal", ""))
        changes.append(f"Set policy_pack to {manifest['policy_pack']}")
    manifest.setdefault("policy", {})
    manifest.setdefault("graph", {"execution_model": "dag"})
    manifest.setdefault("inputs", [])
    manifest.setdefault("outputs", [])
    manifest.setdefault("audit", {"enabled": True, "directory": "audit/runs"})
    manifest.setdefault("environment", {"network_mode": "inherit"})
    manifest.setdefault("tooling", {"allowlisted_commands": list(DEFAULT_ALLOWLISTED_COMMANDS)})
    manifest.setdefault("migrations", {"current_from": None})

    order = [step["id"] for step in steps if isinstance(step, dict) and "id" in step]
    seen_sidecars = {
        sidecar["id"]
        for sidecar in manifest.get("sidecars", [])
        if isinstance(sidecar, dict) and "id" in sidecar
    }
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        step.setdefault("type", "shell")
        step.setdefault("gate_type", "artifact")
        step.setdefault("retry_limit", 0)
        step.setdefault("timeout_seconds", 1800)
        step.setdefault("depends_on", [] if index == 0 else [order[index - 1]])
        step.setdefault("script", f"steps/{step['id']}.sh")
        step.setdefault("executor_config", {})
        step.setdefault("commands", [f"./{step['script']}"])
        step.setdefault(
            "validation_checks", [{"type": "file_exists", "path": f"artifacts/{step['id']}.done"}]
        )
        step.setdefault("consumes", [])
        step.setdefault(
            "produces",
            [
                {
                    "type": "file",
                    "path": f"artifacts/{step['id']}.done",
                    "required": True,
                    "min_size_bytes": 0,
                    "retention": {"days": 30},
                }
            ],
        )
        step.setdefault(
            "rollback",
            {
                "script": f"steps/{step['id']}.rollback.sh",
                "when": "manual",
                "preconditions": [f"artifacts/{step['id']}.done"],
            },
        )
        if step.get("success_gate") in (None, "", "TODO"):
            step["success_gate"] = {"type": "file_exists", "path": f"artifacts/{step['id']}.done"}
            changes.append(f"Strengthened success_gate for {step['id']}")

    if not manifest.get("residual_nondeterminism"):
        manifest["residual_nondeterminism"] = ["none"]
        changes.append("Set residual_nondeterminism to none")

    sidecars = manifest.setdefault("sidecars", [])
    if not seen_sidecars and order:
        last_step = order[-1]
        sidecars.append(make_registry_sidecar("approval-brief", last_step, f"before {last_step}"))
        changes.append(f"Added approval-brief sidecar for {last_step}")

    return manifest, changes


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-harden a deterministic workflow manifest.")
    parser.add_argument("path", help="Workflow directory or workflow.json path.")
    parser.add_argument("--write", action="store_true", help="Write changes back to workflow.json.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workflow_dir = resolve_workflow_dir(Path(args.path))
    manifest_path = workflow_dir / "workflow.json"
    manifest = load_manifest(manifest_path)
    hardened, changes = harden_manifest(manifest)

    if args.write:
        manifest_path.write_text(json.dumps(hardened, indent=2) + "\n", encoding="utf-8")
        print(f"[OK] Hardened {manifest_path}")
    else:
        print(json.dumps(hardened, indent=2))

    if changes:
        for change in changes:
            print(f"[CHANGE] {change}", file=sys.stderr)
    else:
        print("[CHANGE] No hardening changes were needed.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

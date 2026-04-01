#!/usr/bin/env python3
"""Compile a user request into a deterministic workflow package."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from init_deterministic_workflow import build_manifest, build_spec, scaffold, slugify

SKILL_DIR = Path(__file__).resolve().parents[1]
PROMPT_ASSET_DIR = SKILL_DIR / "assets" / "prompts"
SIDECAR_REGISTRY_PATH = SKILL_DIR / "assets" / "sidecar-registry.json"


def choose_kind(request: str) -> str:
    text = request.lower()
    scoring = {
        "release": sum(token in text for token in ("release", "deploy", "publish", "ship")),
        "code-fix": sum(
            token in text for token in ("bug", "fix", "failing", "test", "ci", "regression")
        ),
        "content-review": sum(
            token in text
            for token in ("copy", "content", "article", "landing page", "review", "approve")
        ),
        "etl": sum(
            token in text
            for token in (
                "csv",
                "json",
                "etl",
                "ingest",
                "transform",
                "export",
                "database",
                "table",
            )
        ),
        "file-transform": sum(
            token in text
            for token in ("rename", "convert", "files", "folder", "batch", "directory")
        ),
    }
    best_kind = max(scoring, key=scoring.get)
    return best_kind if scoring[best_kind] > 0 else "generic"


def choose_policy_pack(kind: str, request: str) -> str:
    text = request.lower()
    if kind == "release":
        return "strict-prod"
    if kind == "content-review":
        return "human-approval-heavy"
    if kind == "etl":
        return "offline-only" if "offline" in text else "ci-optimized"
    if kind in {"code-fix", "generic"}:
        return "ai-sidecar-safe"
    return "strict-prod"


def workflow_name_from_request(request: str, explicit_name: str | None) -> str:
    if explicit_name:
        return slugify(explicit_name)
    words = [slugify(part) for part in request.split() if part.strip()]
    joined = "-".join(word for word in words if word)
    return slugify(joined[:48]) or "compiled-workflow"


def infer_contract(request: str, kind: str) -> tuple[list[str], list[str]]:
    text = request.lower()
    inputs: list[str] = []
    outputs: list[str] = []

    if any(token in text for token in ("ci", "test", "bug", "fix")):
        inputs.extend(["repository checkout", "test logs"])
        outputs.extend(["passing test report", "applied patch summary"])
    if any(token in text for token in ("release", "deploy", "publish")):
        inputs.extend(["build artifacts", "release configuration"])
        outputs.extend(["release approval record", "post-release verification report"])
    if any(token in text for token in ("content", "copy", "landing page", "article")):
        inputs.extend(["source content brief"])
        outputs.extend(["approved content variant"])
    if any(token in text for token in ("csv", "json", "etl", "database", "table")):
        inputs.extend(["source dataset"])
        outputs.extend(["reconciled output dataset"])
    if not inputs:
        inputs.append("user request context")
    if not outputs:
        outputs.append(f"{kind} workflow completion evidence")
    return sorted(dict.fromkeys(inputs)), sorted(dict.fromkeys(outputs))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_sidecar_registry() -> dict[str, dict[str, object]]:
    payload = json.loads(SIDECAR_REGISTRY_PATH.read_text(encoding="utf-8"))
    return {entry["id"]: entry for entry in payload["sidecars"]}


def make_sidecar(
    sidecar_id: str,
    *,
    kind: str,
    when: str,
    purpose: str,
    consumer_step: str,
    prompt_asset: str | None = None,
    skill_path: str | None = None,
    notes: str,
    output_schema: dict[str, object] | None = None,
    validator: str | None = None,
) -> dict[str, object]:
    prompt_sha256 = sha256_file(SKILL_DIR / prompt_asset) if prompt_asset else None
    sidecar = {
        "id": sidecar_id,
        "name": sidecar_id.replace("-", " "),
        "kind": kind,
        "when": when,
        "purpose": purpose,
        "consumer_step": consumer_step,
        "containment": {
            "mode": "advisory-only",
            "enforced_by": "Deterministic step order, fixed success gates, and explicit approval or tests.",
            "notes": notes,
        },
        "output_schema": output_schema or {"type": "object", "required_keys": ["summary"]},
        "validator": validator or "json-object-required-keys",
    }
    if prompt_asset:
        sidecar["prompt_asset"] = prompt_asset
        sidecar["prompt_sha256"] = prompt_sha256
    if skill_path:
        sidecar["skill_path"] = skill_path
    return sidecar


def template_for_kind(kind: str, request: str) -> dict[str, object]:
    registry = load_sidecar_registry()
    templates: dict[str, dict[str, object]] = {
        "release": {
            "steps": ["collect", "validate", "review", "publish", "verify"],
            "goal": f"Ship the requested release safely and reproducibly: {request}",
            "residual": ["none"],
            "sidecars": [
                make_sidecar(
                    "approval-brief",
                    kind="prompt",
                    when="after 02-validate and before 03-review",
                    purpose=str(registry["approval-brief"]["purpose"]),
                    consumer_step="03-review",
                    prompt_asset=str(registry["approval-brief"]["prompt_asset"]),
                    notes="Use only as a human review packet; publication still requires explicit approval and deterministic verification.",
                    output_schema=dict(registry["approval-brief"]["output_schema"]),
                    validator=str(registry["approval-brief"]["validator"]),
                )
            ],
            "assets": ["approval-brief.prompt.md"],
        },
        "code-fix": {
            "steps": ["collect", "reproduce", "candidate-fixes", "apply", "test", "review"],
            "goal": f"Fix the requested code issue with deterministic validation: {request}",
            "residual": ["none"],
            "sidecars": [
                make_sidecar(
                    "candidate-generation",
                    kind="prompt",
                    when="after 02-reproduce and before 03-candidate-fixes",
                    purpose=str(registry["candidate-generation"]["purpose"]),
                    consumer_step="03-candidate-fixes",
                    prompt_asset=str(registry["candidate-generation"]["prompt_asset"]),
                    notes="Treat results as proposals only; only candidates that pass fixed tests may be adopted.",
                    output_schema=dict(registry["candidate-generation"]["output_schema"]),
                    validator=str(registry["candidate-generation"]["validator"]),
                ),
                make_sidecar(
                    "edge-case-discovery",
                    kind="prompt",
                    when="after 05-test and before 06-review",
                    purpose=str(registry["edge-case-discovery"]["purpose"]),
                    consumer_step="06-review",
                    prompt_asset=str(registry["edge-case-discovery"]["prompt_asset"]),
                    notes="Sidecar suggestions do not count until encoded as deterministic tests or reviewed explicitly.",
                    output_schema=dict(registry["edge-case-discovery"]["output_schema"]),
                    validator=str(registry["edge-case-discovery"]["validator"]),
                ),
            ],
            "assets": ["candidate-generation.prompt.md", "edge-case-discovery.prompt.md"],
        },
        "content-review": {
            "steps": ["collect", "draft", "variants", "review", "publish"],
            "goal": f"Produce approved content variants with deterministic review gates: {request}",
            "residual": ["human approval required for final selection"],
            "sidecars": [
                make_sidecar(
                    "content-variants",
                    kind="prompt",
                    when="after 02-draft and before 03-variants",
                    purpose=str(registry["content-variants"]["purpose"]),
                    consumer_step="03-variants",
                    prompt_asset=str(registry["content-variants"]["prompt_asset"]),
                    notes="Variants are advisory drafts only; publication remains gated by explicit review and final approval.",
                    output_schema=dict(registry["content-variants"]["output_schema"]),
                    validator=str(registry["content-variants"]["validator"]),
                ),
                make_sidecar(
                    "approval-brief",
                    kind="prompt",
                    when="before 04-review",
                    purpose=str(registry["approval-brief"]["purpose"]),
                    consumer_step="04-review",
                    prompt_asset=str(registry["approval-brief"]["prompt_asset"]),
                    notes="The brief is informational only and cannot approve on behalf of the reviewer.",
                    output_schema=dict(registry["approval-brief"]["output_schema"]),
                    validator=str(registry["approval-brief"]["validator"]),
                ),
            ],
            "assets": ["content-variants.prompt.md", "approval-brief.prompt.md"],
        },
        "etl": {
            "steps": ["collect", "extract", "transform", "load", "reconcile"],
            "goal": f"Run a deterministic data pipeline for the request: {request}",
            "residual": ["external source availability"],
            "sidecars": [
                make_sidecar(
                    "edge-case-discovery",
                    kind="prompt",
                    when="before 03-transform",
                    purpose=str(registry["edge-case-discovery"]["purpose"]),
                    consumer_step="03-transform",
                    prompt_asset=str(registry["edge-case-discovery"]["prompt_asset"]),
                    notes="Every accepted suggestion must be converted into deterministic checks before affecting the load step.",
                    output_schema=dict(registry["edge-case-discovery"]["output_schema"]),
                    validator=str(registry["edge-case-discovery"]["validator"]),
                )
            ],
            "assets": ["edge-case-discovery.prompt.md"],
        },
        "file-transform": {
            "steps": ["collect", "inventory", "transform", "verify"],
            "goal": f"Perform deterministic file transformations for the request: {request}",
            "residual": ["none"],
            "sidecars": [],
            "assets": [],
        },
        "generic": {
            "steps": ["collect", "validate", "execute", "verify"],
            "goal": f"Execute the request with deterministic controls: {request}",
            "residual": ["none"],
            "sidecars": [
                make_sidecar(
                    "candidate-generation",
                    kind="prompt",
                    when="before 03-execute",
                    purpose=str(registry["candidate-generation"]["purpose"]),
                    consumer_step="03-execute",
                    prompt_asset=str(registry["candidate-generation"]["prompt_asset"]),
                    notes="Any output remains advisory until selected by a fixed rule or explicit approval.",
                    output_schema=dict(registry["candidate-generation"]["output_schema"]),
                    validator=str(registry["candidate-generation"]["validator"]),
                )
            ],
            "assets": ["candidate-generation.prompt.md"],
        },
    }
    return templates[kind]


def build_compiled_step_script(step_id: str, step_name: str, goal: str) -> str:
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        'ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"\n'
        f'STEP_ID="{step_id}"\n\n'
        f"# Goal context: {goal}\n"
        f"# Replace this generated placeholder for `{step_name}` with deterministic commands.\n"
        "# Keep outputs stable and make sure the workflow.json success gate matches the observable check.\n"
        'echo "Generated step placeholder: $STEP_ID" >&2\n'
        'echo "Update this script with deterministic commands before running the full workflow." >&2\n'
        "exit 1\n"
    )


def compile_workflow(request: str, output_root: Path, name: str | None = None) -> tuple[Path, str]:
    kind = choose_kind(request)
    template = template_for_kind(kind, request)
    policy_pack = choose_policy_pack(kind, request)
    inferred_inputs, inferred_outputs = infer_contract(request, kind)
    workflow_name = workflow_name_from_request(request, name)
    steps: list[str] = list(template["steps"])
    manifest = json.loads(
        build_manifest(
            workflow_name,
            steps,
            goal=str(template["goal"]),
            residual_nondeterminism=list(template["residual"]),
            sidecars=list(template["sidecars"]),
            policy_pack=policy_pack,
            inputs=inferred_inputs,
            outputs=inferred_outputs,
        )
    )
    for step in manifest["steps"]:
        step_name = str(step["name"])
        step["success_gate"] = {"type": "file_exists", "path": f"artifacts/{step['id']}.done"}
        step["validation_checks"] = [
            {
                "type": "file_exists",
                "path": f"artifacts/{step['id']}.done",
            }
        ]
        step["produces"] = [
            {
                "type": "file",
                "path": f"artifacts/{step['id']}.done",
                "required": True,
                "min_size_bytes": 0,
                "retention": {"days": 30},
            }
        ]
        step["timeout_seconds"] = 1800
        if step["id"] == manifest["steps"][0]["id"]:
            step["consumes"] = [
                {"type": "report", "path": "inputs/request-context.txt", "required": False}
            ]
        elif step["depends_on"]:
            step["consumes"] = [
                {"type": "file", "path": f"artifacts/{dep}.done", "required": True}
                for dep in step["depends_on"]
            ]
        else:
            step["consumes"] = []
        step["rollback"] = {
            "script": f"steps/{step['id']}.rollback.sh",
            "when": "manual",
            "preconditions": [f"artifacts/{step['id']}.done"],
        }
        if step_name in {"review"}:
            step["type"] = "approval"
            step["gate_type"] = "approval"
            step["requires_approval"] = True
        elif step_name in {"publish"}:
            step["type"] = "publish"
            step["gate_type"] = "approval"
            step["requires_approval"] = True
            step["rollback"]["when"] = "on_failure"
        elif step_name in {"test", "verify", "reconcile"}:
            step["type"] = "test"
            step["gate_type"] = "test"
            step["retry_limit"] = 1
        elif step_name in {"transform", "extract", "load", "inventory"}:
            step["type"] = "transform"
            step["gate_type"] = "artifact"
        elif "candidate" in step_name or "variants" in step_name:
            step["type"] = "sidecar-consume"
            step["gate_type"] = "review"
            step["validation_checks"].append(
                {"type": "file_exists", "path": f"artifacts/{step['id']}.done"}
            )

    manifest_override = json.dumps(manifest, indent=2) + "\n"
    spec_override = (
        build_spec(workflow_name, steps)
        + "\n## Compiler Notes\n\n"
        + f"- Workflow kind: `{kind}`\n"
        + f"- Policy pack: `{policy_pack}`\n"
        + f"- Inferred inputs: {', '.join(inferred_inputs)}\n"
        + f"- Inferred outputs: {', '.join(inferred_outputs)}\n"
        + f"- Source request: `{request}`\n"
    )
    step_contents = {
        f"{index:02d}-{step}.sh": build_compiled_step_script(
            f"{index:02d}-{step}", step, str(template["goal"])
        )
        for index, step in enumerate(steps, start=1)
    }
    step_contents.update(
        {
            f"{index:02d}-{step}.rollback.sh": (
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n\n"
                f'echo "Rollback placeholder for {index:02d}-{step}" >&2\n'
                "exit 0\n"
            )
            for index, step in enumerate(steps, start=1)
        }
    )
    copied_assets = [
        (PROMPT_ASSET_DIR / asset_name, f"assets/prompts/{asset_name}")
        for asset_name in template["assets"]
    ]

    workflow_dir = scaffold(
        workflow_name,
        output_root,
        steps,
        manifest_override=manifest_override,
        spec_override=spec_override,
        step_contents=step_contents,
        copied_assets=copied_assets,
    )
    return workflow_dir, kind


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compile a request into a deterministic workflow package."
    )
    parser.add_argument("request", help="The user request to compile.")
    parser.add_argument(
        "--path", default=".", help="Output directory that will contain the workflow folder."
    )
    parser.add_argument("--name", default=None, help="Optional workflow name override.")
    parser.add_argument(
        "--json", action="store_true", help="Emit JSON summary instead of plain text."
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_root = Path(os.path.expanduser(args.path)).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    workflow_dir, kind = compile_workflow(args.request, output_root, name=args.name)
    summary = {"workflow_dir": str(workflow_dir), "workflow_kind": kind}
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"[OK] Compiled {kind} workflow at {workflow_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

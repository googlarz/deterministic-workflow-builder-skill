#!/usr/bin/env python3
"""Scaffold a deterministic workflow package with a typed manifest and Python runtime wrapper."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from inspect import cleandoc
from pathlib import Path

from workflow_schema import SCHEMA_VERSION


DEFAULT_ALLOWLISTED_COMMANDS = [
    "bash",
    "cat",
    "cp",
    "echo",
    "find",
    "git",
    "grep",
    "jq",
    "mkdir",
    "mv",
    "python3",
    "rm",
    "sed",
    "sleep",
    "sort",
    "touch",
    "xargs",
]


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "step"


def parse_steps(raw_steps: str) -> list[str]:
    steps = [slugify(part) for part in raw_steps.split(",") if part.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for step in steps:
        if step not in seen:
            deduped.append(step)
            seen.add(step)
    if not deduped:
        raise ValueError("At least one step is required.")
    return deduped


def write_file(path: Path, content: str, executable: bool = False) -> None:
    path.write_text(content, encoding="utf-8")
    if executable:
        current_mode = path.stat().st_mode
        path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    workflow_name: str,
    steps: list[str],
    *,
    goal: str = "TODO",
    residual_nondeterminism: list[str] | None = None,
    sidecars: list[dict[str, object]] | None = None,
    policy_pack: str = "strict-prod",
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "workflow_name": workflow_name,
        "version": 1,
        "goal": goal,
        "policy_pack": policy_pack,
        "policy": {},
        "working_directory": ".",
        "inputs": inputs or [],
        "outputs": outputs or [],
        "graph": {
            "execution_model": "dag",
        },
        "environment": {
            "network_mode": "inherit",
        },
        "tooling": {
            "allowlisted_commands": DEFAULT_ALLOWLISTED_COMMANDS,
        },
        "migrations": {
            "current_from": None,
        },
        "failure_policy": {
            "on_error": "stop",
            "max_retries": 0,
        },
        "audit": {
            "enabled": True,
            "directory": "audit/runs",
        },
        "residual_nondeterminism": residual_nondeterminism or ["TODO"],
        "steps": [
            {
                "id": f"{index:02d}-{step}",
                "name": step,
                "type": "shell",
                "script": f"steps/{index:02d}-{step}.sh",
                "executor_config": {},
                "commands": [f"./steps/{index:02d}-{step}.sh"],
                "success_gate": "TODO",
                "gate_type": "artifact",
                "validation_checks": [
                    {
                        "type": "file_exists",
                        "path": f"artifacts/{index:02d}-{step}.done",
                    }
                ],
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 1800,
                "depends_on": [] if index == 1 else [f"{index - 1:02d}-{steps[index - 2]}"],
                "consumes": [],
                "produces": [
                    {
                        "type": "file",
                        "path": f"artifacts/{index:02d}-{step}.done",
                        "required": True,
                        "min_size_bytes": 0,
                        "retention": {"days": 30},
                    }
                ],
            }
            for index, step in enumerate(steps, start=1)
        ],
        "sidecars": sidecars or [],
    }
    return json.dumps(payload, indent=2) + "\n"


def build_spec(workflow_name: str, steps: list[str]) -> str:
    step_lines = "\n".join(
        f"{index}. `{step}` - command: `steps/{index:02d}-{step}.sh` - success gate: TODO"
        for index, step in enumerate(steps, start=1)
    )
    return (
        f"# {workflow_name}\n\n"
        "## Deterministic Workflow Contract\n\n"
        "Goal: TODO\n\n"
        "Inputs:\n"
        "- TODO\n\n"
        "Outputs:\n"
        "- TODO\n\n"
        "Runtime:\n"
        "- `./run_workflow.sh`\n"
        "- `./workflow.json`\n\n"
        "Policy Pack:\n"
        "- `strict-prod`\n\n"
        "Steps:\n"
        f"{step_lines}\n\n"
        "Failure policy:\n"
        "- Stop on first failed step unless `failure_policy.on_error` says otherwise.\n"
        "- Retry only when the retry condition is explicit and bounded.\n"
        "- Use `requires_approval: true` in `workflow.json` for manual gates.\n\n"
        "Operational controls:\n"
        "- Steps should declare real `produces` / `consumes` contracts.\n"
        "- `validation_checks` should be machine-checkable, not narrative.\n"
        "- Use `rollback` blocks for publish/apply steps.\n"
        "- Use `./run_workflow.sh --doctor` and `--repair` for crash recovery.\n\n"
        "Residual nondeterminism:\n"
        "- TODO: write `none` or list the exact boundary.\n\n"
        "Optional nondeterministic assists:\n"
        "- Where: none\n"
        "- Why: n/a\n"
        "- Containment: n/a\n"
        "- Bulletproof prompt or skill: n/a\n\n"
        "Validation:\n"
        "- Run `python \"$CODEX_HOME/skills/deterministic-workflow-builder/scripts/verify_workflow.py\" . --simulate`\n"
        "- Run `python \"$CODEX_HOME/skills/deterministic-workflow-builder/scripts/lint_determinism.py\" .`\n"
        "- Run `./run_workflow.sh --dry-run`\n"
    )


def build_step_script(step_name: str, index: int) -> str:
    return cleandoc(
        f"""
        #!/usr/bin/env bash
        set -euo pipefail

        ROOT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/.." && pwd)"
        STEP_ID="{index:02d}-{step_name}"

        # Replace this stub with deterministic commands only.
        # Keep success conditions explicit and produce stable artifacts.
        # Update the matching success_gate in $ROOT_DIR/workflow.json when you finalize this step.
        echo "Step $STEP_ID ({step_name}) is not implemented yet." >&2
        echo "Update $ROOT_DIR/steps/{index:02d}-{step_name}.sh before running the full workflow." >&2
        exit 1
        """
    ) + "\n"


def build_runner(skill_home: Path | None = None) -> str:
    resolved_skill_home = (skill_home or Path(__file__).resolve().parents[1]).resolve()
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        'ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\n'
        f'DEFAULT_SKILL_HOME="{resolved_skill_home}"\n'
        'CODEX_SKILL_HOME="${CODEX_HOME:-$HOME/.codex}/skills/deterministic-workflow-builder"\n'
        'if [[ ! -f "$CODEX_SKILL_HOME/scripts/run_workflow.py" ]]; then\n'
        '  CODEX_SKILL_HOME="$DEFAULT_SKILL_HOME"\n'
        'fi\n'
        'exec python3 "$CODEX_SKILL_HOME/scripts/run_workflow.py" --workflow-dir "$ROOT_DIR" "$@"\n'
    )


def scaffold(
    workflow_name: str,
    output_root: Path,
    steps: list[str],
    *,
    manifest_override: str | None = None,
    spec_override: str | None = None,
    step_contents: dict[str, str] | None = None,
    copied_assets: list[tuple[Path, str]] | None = None,
) -> Path:
    workflow_dir = output_root / slugify(workflow_name)
    if workflow_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing workflow directory: {workflow_dir}")

    (workflow_dir / "steps").mkdir(parents=True)
    (workflow_dir / "logs").mkdir()
    (workflow_dir / "state").mkdir()
    (workflow_dir / "audit" / "runs").mkdir(parents=True)
    (workflow_dir / "assets" / "prompts").mkdir(parents=True)

    step_contents = step_contents or {}
    for index, step_name in enumerate(steps, start=1):
        step_filename = f"{index:02d}-{step_name}.sh"
        write_file(
            workflow_dir / "steps" / step_filename,
            step_contents.get(step_filename, build_step_script(step_name, index)),
            executable=True,
        )
    for relative_name, content in step_contents.items():
        destination = workflow_dir / "steps" / relative_name
        if destination.exists():
            continue
        write_file(destination, content, executable=True)

    write_file(workflow_dir / "WORKFLOW_SPEC.md", spec_override or build_spec(workflow_name, steps))
    write_file(workflow_dir / "workflow.json", manifest_override or build_manifest(workflow_name, steps))
    write_file(workflow_dir / "run_workflow.sh", build_runner(), executable=True)
    write_file(
        workflow_dir / "state" / "step-status.tsv",
        "".join(f"{index:02d}-{step}\tpending\n" for index, step in enumerate(steps, start=1)),
    )
    write_file(
        workflow_dir / "state" / "approval-status.tsv",
        "".join(f"{index:02d}-{step}\tnot-required\n" for index, step in enumerate(steps, start=1)),
    )
    write_file(workflow_dir / "state" / "run-counter.txt", "0\n")
    write_file(workflow_dir / "state" / "sidecar-records.jsonl", "")
    write_file(workflow_dir / "state" / "approval-records.jsonl", "")
    write_file(
        workflow_dir / "state" / "runtime-state.json",
        json.dumps({"active_run_id": None, "steps": {}, "updated_at": None}, indent=2) + "\n",
    )
    write_file(
        workflow_dir / "state" / "metrics.json",
        json.dumps({"workflow_runs": 0, "steps": {}}, indent=2) + "\n",
    )

    for source_path, relative_destination in copied_assets or []:
        destination = workflow_dir / relative_destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, destination)
        if destination.is_file() and destination.suffix == ".md":
            pass

    return workflow_dir


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a deterministic workflow package with typed manifest-backed shell steps."
    )
    parser.add_argument("workflow_name", help="Workflow name; used for the directory and title.")
    parser.add_argument(
        "--path",
        default=".",
        help="Output directory that will contain the workflow folder. Defaults to the current directory.",
    )
    parser.add_argument(
        "--steps",
        required=True,
        help="Comma-separated step names, for example: fetch,validate,test,publish",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_root = Path(os.path.expanduser(args.path)).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    try:
        steps = parse_steps(args.steps)
        workflow_dir = scaffold(args.workflow_name, output_root, steps)
    except (ValueError, FileExistsError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"[OK] Created deterministic workflow scaffold at {workflow_dir}")
    print("[OK] Next: fill WORKFLOW_SPEC.md, update workflow.json, and replace each step stub before execution")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

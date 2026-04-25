#!/usr/bin/env python3
"""Python execution engine for deterministic workflows."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

from workflow_schema import (
    load_manifest,
    normalize_contract,
    resolve_workflow_dir,
    simulate_step_order,
    summarize_sidecars,
    validate_manifest,
)

SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(secret|token|password|apikey|api_key|private_key)=([^\s]+)"),
    re.compile(r"(?i)(authorization:\s*bearer\s+)(\S+)"),
)
NETWORK_COMMANDS = {"curl", "wget", "http", "https", "ssh", "scp", "rsync", "nc", "telnet"}
STATE_IO_LOCK = threading.Lock()
SHELL_BUILTINS = {
    ".",
    ":",
    "[",
    "[[",
    "alias",
    "bg",
    "bind",
    "break",
    "builtin",
    "cd",
    "command",
    "continue",
    "declare",
    "dirs",
    "disown",
    "echo",
    "elif",
    "else",
    "enable",
    "eval",
    "exec",
    "exit",
    "export",
    "false",
    "fc",
    "fg",
    "fi",
    "for",
    "function",
    "getopts",
    "hash",
    "help",
    "history",
    "if",
    "in",
    "jobs",
    "kill",
    "let",
    "local",
    "logout",
    "popd",
    "printf",
    "pushd",
    "pwd",
    "read",
    "readonly",
    "return",
    "select",
    "set",
    "shift",
    "source",
    "suspend",
    "test",
    "then",
    "times",
    "trap",
    "true",
    "type",
    "typeset",
    "ulimit",
    "umask",
    "unalias",
    "unset",
    "until",
    "wait",
    "while",
}


@dataclass
class WorkflowPaths:
    workflow_dir: Path
    manifest_path: Path
    state_dir: Path
    lock_path: Path
    step_state_path: Path
    approval_state_path: Path
    runtime_state_path: Path
    approval_records_path: Path
    metrics_path: Path
    sidecar_records_path: Path
    run_counter_path: Path
    log_dir: Path
    audit_root: Path
    mutations_path: Path


@dataclass
class RunContext:
    run_id: str | None
    run_dir: Path | None
    dry_run: bool
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    step_id: str
    returncode: int
    category: str
    message: str
    duration_seconds: float


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_paths(workflow_dir: Path) -> WorkflowPaths:
    state_dir = workflow_dir / "state"
    return WorkflowPaths(
        workflow_dir=workflow_dir,
        manifest_path=workflow_dir / "workflow.json",
        state_dir=state_dir,
        lock_path=workflow_dir / ".workflow.lock",
        step_state_path=state_dir / "step-status.tsv",
        approval_state_path=state_dir / "approval-status.tsv",
        runtime_state_path=state_dir / "runtime-state.json",
        approval_records_path=state_dir / "approval-records.jsonl",
        metrics_path=state_dir / "metrics.json",
        sidecar_records_path=state_dir / "sidecar-records.jsonl",
        run_counter_path=state_dir / "run-counter.txt",
        log_dir=workflow_dir / "logs",
        audit_root=workflow_dir / "audit" / "runs",
        mutations_path=state_dir / "proposed-mutations.json",
    )


def load_policy(skill_dir: Path, policy_name: str) -> dict[str, Any]:
    policy_path = skill_dir / "assets" / "policies" / f"{policy_name}.json"
    if not policy_path.exists():
        raise FileNotFoundError(f"Unknown policy pack: {policy_name}")
    return json.loads(policy_path.read_text(encoding="utf-8"))


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def redact_text(text: str) -> str:
    redacted = text
    for pattern in SENSITIVE_PATTERNS:
        redacted = pattern.sub(
            lambda match: f"{match.group(1)}=[REDACTED]" if match.lastindex == 2 else "[REDACTED]",
            redacted,
        )
    return redacted


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        Path(temp_name).replace(path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def read_json_file_with_errors(
    path: Path, default: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[str]]:
    if not path.exists():
        return default or {}, []
    try:
        return json.loads(path.read_text(encoding="utf-8")), []
    except json.JSONDecodeError as exc:
        return default or {}, [f"Invalid JSON in {path.name}: {exc}"]


def read_json_file(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    payload, _ = read_json_file_with_errors(path, default)
    return payload


def set_read_only(path: Path) -> None:
    current_mode = path.stat().st_mode
    path.chmod(current_mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)


class WorkflowLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Any | None = None

    def __enter__(self) -> "WorkflowLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.handle is None:
            return
        if fcntl is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()
        self.handle = None


def ensure_state(paths: WorkflowPaths, manifest: dict[str, Any]) -> None:
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    paths.log_dir.mkdir(parents=True, exist_ok=True)
    paths.audit_root.mkdir(parents=True, exist_ok=True)
    paths.lock_path.touch(exist_ok=True)
    if not paths.step_state_path.exists():
        atomic_write_text(
            paths.step_state_path, "".join(f"{step['id']}\tpending\n" for step in manifest["steps"])
        )
    if not paths.approval_state_path.exists():
        atomic_write_text(
            paths.approval_state_path,
            "".join(
                f"{step['id']}\t{'pending' if step.get('requires_approval') else 'not-required'}\n"
                for step in manifest["steps"]
            ),
        )
    if not paths.runtime_state_path.exists():
        atomic_write_json(
            paths.runtime_state_path, {"active_run_id": None, "steps": {}, "updated_at": utc_now()}
        )
    if not paths.metrics_path.exists():
        atomic_write_json(paths.metrics_path, {"workflow_runs": 0, "steps": {}})
    if not paths.run_counter_path.exists():
        atomic_write_text(paths.run_counter_path, "0\n")
    if not paths.sidecar_records_path.exists():
        atomic_write_text(paths.sidecar_records_path, "")
    if not paths.approval_records_path.exists():
        atomic_write_text(paths.approval_records_path, "")


def read_tsv_state_with_errors(path: Path) -> tuple[dict[str, str], list[str]]:
    if not path.exists():
        return {}, []
    result: dict[str, str] = {}
    errors: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        if "\t" not in raw_line:
            errors.append(f"Malformed TSV line in {path.name}: {raw_line}")
            continue
        key, value = raw_line.split("\t", 1)
        result[key] = value
    return result, errors


def read_tsv_state(path: Path) -> dict[str, str]:
    state, _ = read_tsv_state_with_errors(path)
    return state


def write_tsv_state(path: Path, state: dict[str, str]) -> None:
    atomic_write_text(path, "".join(f"{key}\t{value}\n" for key, value in state.items()))


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def append_text(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def load_latest_approval_record(paths: WorkflowPaths, step_id: str) -> dict[str, Any] | None:
    if not paths.approval_records_path.exists():
        return None
    latest: dict[str, Any] | None = None
    for raw_line in paths.approval_records_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("step_id") == step_id:
            latest = payload
    return latest


def attach_approval_to_run(paths: WorkflowPaths, run_context: RunContext, step_id: str) -> None:
    if run_context.run_dir is None:
        return
    approval_record = load_latest_approval_record(paths, step_id)
    record = {
        "timestamp": utc_now(),
        "step_id": step_id,
        "approver": "",
        "reason": "",
        "change_ref": "",
        "run_id": run_context.run_id or "",
    }
    if approval_record is not None:
        record.update(
            {
                "timestamp": approval_record.get("timestamp", record["timestamp"]),
                "approver": str(approval_record.get("approver", "")),
                "reason": str(approval_record.get("reason", "")),
                "change_ref": str(approval_record.get("change_ref", "")),
            }
        )

    approval_line = (
        f"APPROVED\t{step_id}\tapprover={record['approver']}\treason={record['reason']}"
        f"\tchange_ref={record['change_ref']}"
    )
    approvals_log = run_context.run_dir / "approvals.log"
    existing = approvals_log.read_text(encoding="utf-8") if approvals_log.exists() else ""
    if approval_line not in existing:
        append_text(approvals_log, approval_line)
        append_jsonl(
            run_context.run_dir / "events.jsonl",
            {"timestamp": utc_now(), "event": "approval_used", **record},
        )


def next_run_dir(paths: WorkflowPaths) -> tuple[str, Path]:
    current = int(paths.run_counter_path.read_text(encoding="utf-8").strip() or "0")
    next_value = current + 1
    while True:
        run_id = f"run-{next_value:04d}"
        run_dir = paths.audit_root / run_id
        if not run_dir.exists():
            break
        next_value += 1
    atomic_write_text(paths.run_counter_path, f"{next_value}\n")
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_id, run_dir


def summarize_policy(policy: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(policy))


def audit_enabled(manifest: dict[str, Any], policy: dict[str, Any]) -> bool:
    manifest_audit = manifest.get("audit", {})
    if isinstance(manifest_audit, dict) and manifest_audit.get("enabled") is False:
        return False
    policy_audit = policy.get("audit", {})
    if isinstance(policy_audit, dict) and policy_audit.get("enabled") is False:
        return False
    return True


def get_steps_by_id(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {step["id"]: step for step in manifest["steps"]}


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    for row in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def resolve_safe_path(base_dir: Path, relative_path: str) -> Path:
    candidate = (base_dir / relative_path).resolve()
    if base_dir.resolve() not in (candidate, *candidate.parents):
        raise ValueError(f"Path escapes workflow directory: {relative_path}")
    return candidate


def normalize_contracts(entries: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        contract = normalize_contract(entry)
        if contract is not None:
            normalized.append(contract)
    return normalized


def normalize_validation_checks(entries: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        if isinstance(entry, str):
            continue
        if isinstance(entry, dict):
            normalized.append(entry)
    return normalized


def load_runtime_state(paths: WorkflowPaths) -> dict[str, Any]:
    return read_json_file(
        paths.runtime_state_path, {"active_run_id": None, "steps": {}, "updated_at": utc_now()}
    )


def write_runtime_state(paths: WorkflowPaths, payload: dict[str, Any]) -> None:
    payload["updated_at"] = utc_now()
    with STATE_IO_LOCK:
        atomic_write_json(paths.runtime_state_path, payload)


def update_metrics(paths: WorkflowPaths, mutate: Any) -> dict[str, Any]:
    with STATE_IO_LOCK:
        payload = read_json_file(paths.metrics_path, {"workflow_runs": 0, "steps": {}})
        mutate(payload)
        atomic_write_json(paths.metrics_path, payload)
        return payload


def record_event(run_context: RunContext, event: dict[str, Any]) -> None:
    if run_context.run_dir is None:
        return
    enriched = {"timestamp": utc_now(), **event}
    append_jsonl(run_context.run_dir / "events.jsonl", enriched)


def update_runtime_step(paths: WorkflowPaths, step_id: str, payload: dict[str, Any]) -> None:
    with STATE_IO_LOCK:
        runtime = load_runtime_state(paths)
        runtime.setdefault("steps", {})
        step_state = runtime["steps"].get(step_id, {})
        step_state.update(payload)
        runtime["steps"][step_id] = step_state
        payload = runtime
        payload["updated_at"] = utc_now()
        atomic_write_json(paths.runtime_state_path, payload)


def setup_run_audit(
    paths: WorkflowPaths, manifest: dict[str, Any], policy: dict[str, Any]
) -> RunContext:
    run_id, run_dir = next_run_dir(paths)
    manifest_snapshot = run_dir / "workflow.snapshot.json"
    policy_snapshot = run_dir / "policy.snapshot.json"
    sidecars_snapshot = run_dir / "sidecars.snapshot.json"
    env_snapshot = run_dir / "env.snapshot.txt"
    prompt_digests: dict[str, str] = {}

    shutil.copyfile(paths.manifest_path, manifest_snapshot)
    atomic_write_json(policy_snapshot, summarize_policy(policy))
    atomic_write_text(sidecars_snapshot, json.dumps(summarize_sidecars(manifest), indent=2) + "\n")
    atomic_write_text(
        env_snapshot,
        "\n".join(f"{key}={redact_text(str(value))}" for key, value in sorted(os.environ.items()))
        + "\n",
    )
    for sidecar in manifest.get("sidecars", []):
        prompt_asset = sidecar.get("prompt_asset")
        if isinstance(prompt_asset, str):
            prompt_path = paths.workflow_dir / prompt_asset
            if prompt_path.exists():
                prompt_digests[prompt_asset] = sha256_file(prompt_path)

    (run_dir / "logs").mkdir(exist_ok=True)
    atomic_write_text(run_dir / "commands.log", "")
    atomic_write_text(run_dir / "approvals.log", "")
    atomic_write_text(run_dir / "simulation.log", "")
    atomic_write_text(run_dir / "events.jsonl", "")
    atomic_write_json(run_dir / "metrics.json", {"steps": {}, "run_id": run_id})
    atomic_write_json(
        run_dir / "digests.json",
        {
            "manifest_sha256": sha256_file(manifest_snapshot),
            "policy_sha256": sha256_file(policy_snapshot),
            "sidecars_sha256": sha256_file(sidecars_snapshot),
            "prompt_assets": prompt_digests,
        },
    )
    for snapshot_path in (
        manifest_snapshot,
        policy_snapshot,
        sidecars_snapshot,
        env_snapshot,
        run_dir / "digests.json",
    ):
        set_read_only(snapshot_path)

    runtime = load_runtime_state(paths)
    runtime["active_run_id"] = run_id
    write_runtime_state(paths, runtime)
    update_metrics(
        paths,
        lambda payload: payload.__setitem__(
            "workflow_runs", int(payload.get("workflow_runs", 0)) + 1
        ),
    )
    return RunContext(
        run_id=run_id, run_dir=run_dir, dry_run=False, metrics={"steps": {}, "run_id": run_id}
    )


def finalize_run_audit(
    paths: WorkflowPaths, manifest: dict[str, Any], run_context: RunContext
) -> None:
    if run_context.run_dir is None:
        return
    atomic_write_json(run_context.run_dir / "metrics.json", run_context.metrics)
    digests = read_json_file(run_context.run_dir / "digests.json", {})
    outputs: dict[str, list[dict[str, Any]]] = {}
    for step in manifest["steps"]:
        step_outputs: list[dict[str, Any]] = []
        for contract in normalize_contracts(step.get("produces", [])):
            output_path = paths.workflow_dir / contract["path"]
            if output_path.exists() and output_path.is_file():
                step_outputs.append(
                    {
                        "path": contract["path"],
                        "sha256": sha256_file(output_path),
                        "size_bytes": output_path.stat().st_size,
                    }
                )
        if step_outputs:
            outputs[step["id"]] = step_outputs
    digests["outputs"] = outputs
    atomic_write_json(run_context.run_dir / "digests.json", digests)
    set_read_only(run_context.run_dir / "digests.json")
    runtime = load_runtime_state(paths)
    runtime["active_run_id"] = None
    write_runtime_state(paths, runtime)
    _write_workflow_viz(paths)


def _write_workflow_viz(paths: WorkflowPaths) -> None:
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from visualize_workflow import generate_html  # type: ignore[import]

        html = generate_html(paths.workflow_dir)
        (paths.workflow_dir / "workflow-graph.html").write_text(html, encoding="utf-8")
    except Exception:
        pass


def verify_manifest_or_die(manifest: dict[str, Any], paths: WorkflowPaths) -> None:
    issues = validate_manifest(manifest, paths.manifest_path, workflow_dir=paths.workflow_dir)
    errors = [issue for issue in issues if issue.severity == "error"]
    if errors:
        for issue in errors:
            location = f"{issue.path}:{issue.line}" if issue.line else issue.path
            print(f"[ERROR] {location} - {issue.message}", file=sys.stderr)
        raise SystemExit(1)


def list_steps(manifest: dict[str, Any], paths: WorkflowPaths) -> int:
    ensure_state(paths, manifest)
    step_state = read_tsv_state(paths.step_state_path)
    approval_state = read_tsv_state(paths.approval_state_path)
    rows = []
    for step in manifest["steps"]:
        rows.append(
            [
                step["id"],
                step_state.get(step["id"], "missing"),
                approval_state.get(step["id"], "missing"),
                step["type"],
                ",".join(step.get("depends_on", [])) or "-",
                step.get("script", "-") or "-",
            ]
        )
    print_table(["step", "status", "approval", "type", "depends_on", "script"], rows)
    return 0


def list_sidecars(manifest: dict[str, Any]) -> int:
    rows = [
        [
            sidecar["id"],
            sidecar["kind"],
            sidecar["when"],
            sidecar["consumer_step"],
            sidecar.get("validator", ""),
        ]
        for sidecar in manifest.get("sidecars", [])
    ]
    if not rows:
        print("No sidecars configured.")
        return 0
    print_table(["sidecar", "kind", "when", "consumer_step", "validator"], rows)
    return 0


def list_runs(paths: WorkflowPaths) -> int:
    paths.audit_root.mkdir(parents=True, exist_ok=True)
    runs = sorted(path.name for path in paths.audit_root.iterdir() if path.is_dir())
    if not runs:
        print("No audit runs recorded.")
        return 0
    for run_id in runs:
        print(run_id)
    return 0


def detect_run_dir(paths: WorkflowPaths) -> tuple[str | None, Path | None]:
    runs = (
        sorted(path for path in paths.audit_root.iterdir() if path.is_dir())
        if paths.audit_root.exists()
        else []
    )
    if not runs:
        return None, None
    latest = runs[-1]
    return latest.name, latest


def replay_run(paths: WorkflowPaths, run_id: str, simulate: bool = False) -> int:
    run_dir = paths.audit_root / run_id
    if not run_dir.exists():
        print(f"Unknown run: {run_id}", file=sys.stderr)
        return 1
    print(f"[RUN] {run_id}")
    for label, filename in (
        ("EVENTS", "events.jsonl"),
        ("COMMANDS", "commands.log"),
        ("APPROVALS", "approvals.log"),
        ("SIMULATION", "simulation.log"),
    ):
        file_path = run_dir / filename
        if file_path.exists():
            print(f"[{label}]")
            print(file_path.read_text(encoding="utf-8"), end="")
    if simulate:
        snapshot_path = run_dir / "workflow.snapshot.json"
        if snapshot_path.exists():
            manifest = load_manifest(snapshot_path)
            print("[EXPECTED_STEP_ORDER]")
            for step_id in simulate_step_order(manifest):
                print(step_id)
    return 0


def should_require_approval(step: dict[str, Any], policy: dict[str, Any]) -> bool:
    required_for = set(policy.get("approval", {}).get("required_for", []))
    return (
        bool(step.get("requires_approval"))
        or step.get("type") in required_for
        or step.get("name") in required_for
    )


def parse_success_gate(success_gate: Any) -> dict[str, Any]:
    if isinstance(success_gate, dict):
        return success_gate
    if isinstance(success_gate, str):
        stripped = success_gate.strip()
        lowered = stripped.lower()
        if lowered == "todo":
            return {"type": "noop"}
        if lowered.startswith("log contains "):
            return {"type": "log_contains", "value": stripped[len("log contains ") :]}
        if lowered.startswith("file exists "):
            return {"type": "file_exists", "path": stripped[len("file exists ") :]}
        if lowered.startswith("artifact exists "):
            return {"type": "file_exists", "path": stripped[len("artifact exists ") :]}
        return {"type": "description", "value": stripped}
    return {"type": "noop"}


def record_sidecars(paths: WorkflowPaths, manifest: dict[str, Any], consumer_step: str) -> None:
    for sidecar in manifest.get("sidecars", []):
        if sidecar.get("consumer_step") == consumer_step:
            append_jsonl(
                paths.sidecar_records_path,
                {
                    "timestamp": utc_now(),
                    "sidecar_id": sidecar["id"],
                    "consumer_step": consumer_step,
                    "status": "available",
                    "kind": sidecar["kind"],
                    "containment": sidecar["containment"]["mode"],
                    "validator": sidecar.get("validator", ""),
                },
            )


def scan_mutation_proposals(
    output: str, sidecar_id: str, run_context: RunContext
) -> list[dict[str, Any]]:
    """Parse ---PROPOSE_MUTATION--- blocks from sidecar output, validate and return list."""
    import uuid

    VALID_MUTATION_TYPES = {"add_step", "modify_step", "add_sidecar"}
    ALLOWED_MODIFY_KEYS = {"retry_limit", "timeout_seconds", "failure_policy", "script"}
    proposals: list[dict[str, Any]] = []
    blocks = re.findall(r"---PROPOSE_MUTATION---\s*(.*?)\s*---END_MUTATION---", output, re.DOTALL)
    for block in blocks:
        try:
            data = json.loads(block)
        except json.JSONDecodeError as exc:
            print(f"[runner] Sidecar {sidecar_id}: invalid mutation JSON: {exc}", file=sys.stderr)
            continue
        if not isinstance(data, dict):
            print(
                f"[runner] Sidecar {sidecar_id}: mutation must be a JSON object.", file=sys.stderr
            )
            continue
        if data.get("version") != 1:
            print(f"[runner] Sidecar {sidecar_id}: mutation version must be 1.", file=sys.stderr)
            continue
        for required in ("description", "type", "payload"):
            if required not in data:
                print(
                    f"[runner] Sidecar {sidecar_id}: mutation missing field '{required}'.",
                    file=sys.stderr,
                )
                break
        else:
            mutation_type = data.get("type")
            if mutation_type not in VALID_MUTATION_TYPES:
                print(
                    f"[runner] Sidecar {sidecar_id}: unknown mutation type '{mutation_type}'.",
                    file=sys.stderr,
                )
                continue
            payload = data.get("payload", {})
            if not isinstance(payload, dict):
                print(
                    f"[runner] Sidecar {sidecar_id}: mutation payload must be a dict.",
                    file=sys.stderr,
                )
                continue
            if mutation_type == "add_step" and "step" not in payload:
                print(
                    f"[runner] Sidecar {sidecar_id}: add_step payload must have 'step'.",
                    file=sys.stderr,
                )
                continue
            if mutation_type == "modify_step":
                if "step_id" not in payload or "changes" not in payload:
                    print(
                        f"[runner] Sidecar {sidecar_id}: modify_step payload needs 'step_id' and 'changes'.",
                        file=sys.stderr,
                    )
                    continue
                bad_keys = set(payload.get("changes", {}).keys()) - ALLOWED_MODIFY_KEYS
                if bad_keys:
                    print(
                        f"[runner] Sidecar {sidecar_id}: modify_step disallowed keys: {bad_keys}.",
                        file=sys.stderr,
                    )
                    continue
            if mutation_type == "add_sidecar" and "sidecar" not in payload:
                print(
                    f"[runner] Sidecar {sidecar_id}: add_sidecar payload must have 'sidecar'.",
                    file=sys.stderr,
                )
                continue
            mut_id = f"mut-{uuid.uuid4().hex[:8]}"
            proposals.append(
                {
                    "id": mut_id,
                    "proposed_by": sidecar_id,
                    "proposed_at": utc_now(),
                    "run_id": run_context.run_id or "",
                    "description": str(data["description"]),
                    "type": mutation_type,
                    "payload": payload,
                    "status": "pending",
                }
            )
    return proposals


def store_mutation_proposals(paths: WorkflowPaths, proposals: list[dict[str, Any]]) -> None:
    """Append new proposals to proposed-mutations.json atomically."""
    with STATE_IO_LOCK:
        existing = read_json_file(paths.mutations_path, {"mutations": []})
        existing.setdefault("mutations", [])
        existing["mutations"].extend(proposals)
        atomic_write_json(paths.mutations_path, existing)


def run_sidecar_script(
    sc: dict[str, Any],
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    run_context: RunContext,
) -> str | None:
    """Execute a sidecar script if present, capture output, scan for mutation proposals."""
    if "script" not in sc:
        return None
    script_path_raw = sc["script"]
    try:
        script_path = resolve_safe_path(paths.workflow_dir, script_path_raw)
    except ValueError as exc:
        print(f"[runner] Sidecar script path error: {exc}", file=sys.stderr)
        return None
    log_path = paths.log_dir / f"sidecar-{sc['id']}.log"
    try:
        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=paths.workflow_dir,
            text=True,
            capture_output=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        output = result.stdout or ""
    except Exception as exc:
        output = f"[runner] Sidecar script error: {exc}\n"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(log_path, output)
    proposals = scan_mutation_proposals(output, sc["id"], run_context)
    if proposals:
        store_mutation_proposals(paths, proposals)
    return output


def apply_mutation(manifest_path: Path, mutation: dict[str, Any]) -> dict[str, Any]:
    """Apply a mutation to workflow.json, backing up first. Returns updated manifest."""
    import time as _time

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ts = str(int(_time.time()))
    backup_path = manifest_path.parent / f"workflow.json.bak-{ts}"
    backup_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    mutation_type = mutation["type"]
    payload = mutation["payload"]

    if mutation_type == "add_step":
        new_step = payload["step"]
        new_step.setdefault("name", new_step.get("id", "unnamed-step"))
        new_step.setdefault("type", "shell")
        new_step.setdefault("success_gate", "")
        new_step.setdefault("gate_type", "artifact")
        new_step.setdefault("requires_approval", False)
        new_step.setdefault("retry_limit", 0)
        new_step.setdefault("timeout_seconds", 300)
        after_id = payload.get("after")
        before_id = payload.get("before")
        steps = manifest["steps"]
        if after_id:
            idx = next((i for i, s in enumerate(steps) if s["id"] == after_id), None)
            insert_at = (idx + 1) if idx is not None else len(steps)
        elif before_id:
            idx = next((i for i, s in enumerate(steps) if s["id"] == before_id), None)
            insert_at = idx if idx is not None else len(steps)
            # Update depends_on of the before step to include new_step
            if idx is not None:
                existing_deps = steps[idx].get("depends_on", [])
                new_step_id = new_step.get("id", "")
                if new_step_id and new_step_id not in existing_deps:
                    new_step_deps = new_step.get("depends_on", [])
                    for dep in new_step_deps:
                        if dep in existing_deps:
                            existing_deps.remove(dep)
                    existing_deps.insert(0, new_step_id)
                    steps[idx]["depends_on"] = existing_deps
        else:
            insert_at = len(steps)
        steps.insert(insert_at, new_step)

    elif mutation_type == "modify_step":
        step_id = payload["step_id"]
        changes = payload["changes"]
        ALLOWED = {"retry_limit", "timeout_seconds", "failure_policy", "script"}
        step = next((s for s in manifest["steps"] if s["id"] == step_id), None)
        if step is None:
            backup_path.unlink(missing_ok=True)
            raise ValueError(f"modify_step: step '{step_id}' not found")
        for key, value in changes.items():
            if key in ALLOWED:
                step[key] = value

    elif mutation_type == "add_sidecar":
        manifest.setdefault("sidecars", []).append(payload["sidecar"])

    from workflow_schema import validate_manifest

    issues = validate_manifest(manifest, manifest_path)
    errors = [i for i in issues if i.severity == "error"]
    if errors:
        backup_path.replace(manifest_path)
        raise ValueError(f"Mutation validation failed: {errors[0].message}")

    atomic_write_json(manifest_path, manifest)
    return manifest


def list_mutations(paths: WorkflowPaths) -> int:
    """List pending mutation proposals."""
    data = read_json_file(paths.mutations_path, {"mutations": []})
    mutations = data.get("mutations", [])
    if not mutations:
        print("No mutation proposals.")
        return 0
    rows = [
        [m["id"], m["proposed_by"], m["type"], m["description"][:50], m["status"]]
        for m in mutations
    ]
    print_table(["id", "proposed_by", "type", "description", "status"], rows)
    return 0


def approve_mutation(paths: WorkflowPaths, mut_id: str) -> int:
    """Apply a pending mutation and mark it applied."""
    with STATE_IO_LOCK:
        data = read_json_file(paths.mutations_path, {"mutations": []})
        mutations = data.get("mutations", [])
        mutation = next((m for m in mutations if m["id"] == mut_id), None)
        if mutation is None:
            print(f"Unknown mutation: {mut_id}", file=sys.stderr)
            return 1
        if mutation["status"] != "pending":
            print(
                f"Mutation {mut_id} is not pending (status={mutation['status']}).", file=sys.stderr
            )
            return 1
        try:
            apply_mutation(paths.manifest_path, mutation)
        except ValueError as exc:
            print(f"Failed to apply mutation: {exc}", file=sys.stderr)
            return 1
        mutation["status"] = "applied"
        atomic_write_json(paths.mutations_path, data)
    _write_workflow_viz(paths)
    print(f"Applied mutation {mut_id}: {mutation['description']}")
    return 0


def reject_mutation(paths: WorkflowPaths, mut_id: str) -> int:
    """Mark a mutation as rejected."""
    with STATE_IO_LOCK:
        data = read_json_file(paths.mutations_path, {"mutations": []})
        mutations = data.get("mutations", [])
        mutation = next((m for m in mutations if m["id"] == mut_id), None)
        if mutation is None:
            print(f"Unknown mutation: {mut_id}", file=sys.stderr)
            return 1
        if mutation["status"] != "pending":
            print(
                f"Mutation {mut_id} is not pending (status={mutation['status']}).", file=sys.stderr
            )
            return 1
        mutation["status"] = "rejected"
        atomic_write_json(paths.mutations_path, data)
    print(f"Rejected mutation {mut_id}")
    return 0


def run_improvement_cycle(
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    policy: dict[str, Any],
    *,
    max_risk: str = "low",
    verbose: bool = True,
) -> int:
    """Auto-approve pending mutations that meet the risk threshold; print a summary.

    Returns the count of mutations auto-approved.
    """
    try:
        import importlib.util  # noqa: PLC0415
        import sys  # noqa: PLC0415

        spec = importlib.util.spec_from_file_location(
            "mutation_classifier",
            Path(__file__).parent / "mutation_classifier.py",
        )
        if spec is None or spec.loader is None:
            raise ImportError("mutation_classifier not found")
        mc = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mc)  # type: ignore[union-attr]
    except Exception as exc:
        print(f"[improve] Cannot load mutation_classifier: {exc}", file=sys.stderr)
        return 0

    data = read_json_file(paths.mutations_path, {"mutations": []})
    mutations = data.get("mutations", [])
    pending = [m for m in mutations if m.get("status") == "pending"]

    history = mc.analyze_run_history(paths.audit_root)
    summary = mc.improvement_summary(pending, history, max_risk=max_risk)

    auto = summary["auto_approvable"]
    needs = summary["needs_review"]
    unhealthy = summary["unhealthy_steps"]

    if verbose:
        print(f"\n[improve] Pending mutations: {len(pending)}")
        print(f"[improve] Auto-approvable (risk ≤ {max_risk}): {len(auto)}")
        print(f"[improve] Needs human review: {len(needs)}")
        if unhealthy:
            print(f"[improve] Unhealthy steps (>20% failure rate): {', '.join(unhealthy)}")

    approved = 0
    for mut in auto:
        rc = approve_mutation(paths, mut["id"])
        if rc == 0:
            approved += 1
            if verbose:
                risk = mc.classify_risk(mut)
                print(
                    f"[improve] ✓ auto-approved {mut['id']}  [{risk}]  {mut.get('description', '')[:60]}"
                )

    if approved and verbose:
        print(f"[improve] Applied {approved} mutation(s). Re-run the workflow to use them.")
    elif not pending and verbose:
        print("[improve] No pending mutations.")

    return approved


_AUTO_HEAL_PROMPT_TEMPLATE = """\
A step in a deterministic workflow failed. Propose a mutation to fix it.

Step config:
{step_json}

Error output (last 2000 chars):
{error_output}

Respond with ONLY a mutation proposal block in this exact format:
---PROPOSE_MUTATION---
{{
  "type": "modify_step",
  "description": "<one-line description of the fix>",
  "payload": {{
    "step_id": "{step_id}",
    "changes": {{
      "retry_limit": <int if retry makes sense>,
      "timeout_seconds": <int if timeout was the issue>
    }}
  }}
}}
---END_MUTATION---

Only include fields that should actually change. If the failure is not fixable by
modifying step parameters (e.g. it requires manual intervention), output nothing.
"""


def auto_heal_step(
    step: dict[str, Any],
    result: "StepResult",
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    run_context: "RunContext",
) -> None:
    """If auto_heal is enabled, ask Claude to propose a fix mutation for the failed step."""
    if not (manifest.get("auto_heal") or step.get("auto_heal")):
        return
    claude_bin = shutil.which("claude")
    if not claude_bin:
        return
    step_id = step["id"]
    log_path = paths.logs_dir / f"{step_id}.log"
    error_output = ""
    if log_path.exists():
        text = log_path.read_text(encoding="utf-8", errors="replace")
        error_output = text[-2000:] if len(text) > 2000 else text
    prompt = _AUTO_HEAL_PROMPT_TEMPLATE.format(
        step_json=json.dumps({k: v for k, v in step.items() if k != "auto_heal"}, indent=2),
        error_output=error_output,
        step_id=step_id,
    )
    try:
        proc = subprocess.run(
            [claude_bin, "-p", prompt, "--model", "claude-haiku-4-5-20251001"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        output = proc.stdout or ""
    except Exception:
        return
    proposals = scan_mutation_proposals(output, "auto-heal", run_context)
    if proposals:
        store_mutation_proposals(paths, proposals)
        print(
            f"  🔧 auto-heal: stored {len(proposals)} proposal(s) for {step_id}"
            f" — run --list-mutations to review"
        )


def enforce_path_contract(
    paths: WorkflowPaths, contract: dict[str, Any], *, allow_missing: bool = False
) -> tuple[bool, str]:
    target = resolve_safe_path(paths.workflow_dir, contract["path"])
    if not target.exists():
        if allow_missing or not contract.get("required", True):
            return True, "optional-missing"
        return False, f"Missing required artifact {contract['path']}"
    if target.is_dir():
        return False, f"Artifact must be a file, not a directory: {contract['path']}"
    size_bytes = target.stat().st_size
    min_size = contract.get("min_size_bytes")
    max_size = contract.get("max_size_bytes")
    if isinstance(min_size, int) and size_bytes < min_size:
        return False, f"Artifact {contract['path']} is smaller than min_size_bytes"
    if isinstance(max_size, int) and size_bytes > max_size:
        return False, f"Artifact {contract['path']} is larger than max_size_bytes"
    expected_sha = contract.get("sha256")
    if isinstance(expected_sha, str):
        actual_sha = sha256_file(target)
        if actual_sha != expected_sha:
            return False, f"Artifact {contract['path']} sha256 mismatch"
    if contract.get("type") == "json":
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return False, f"Artifact {contract['path']} is not valid JSON: {exc}"
        schema = contract.get("schema", {})
        required_keys = schema.get("required_keys", []) if isinstance(schema, dict) else []
        if required_keys:
            if not isinstance(payload, dict):
                return False, f"Artifact {contract['path']} must contain a JSON object"
            missing_keys = [key for key in required_keys if key not in payload]
            if missing_keys:
                return (
                    False,
                    f"Artifact {contract['path']} is missing keys: {', '.join(missing_keys)}",
                )
    return True, "ok"


def run_validation_checks(
    step: dict[str, Any], paths: WorkflowPaths, log_path: Path
) -> tuple[bool, str]:
    for check in normalize_validation_checks(step.get("validation_checks", [])):
        check_type = check["type"]
        if check_type == "file_exists":
            ok, message = enforce_path_contract(
                paths, {"path": check["path"], "type": "file", "required": True}
            )
            if not ok:
                return False, message
        elif check_type == "path_absent":
            target = resolve_safe_path(paths.workflow_dir, check["path"])
            if target.exists():
                return False, f"Path should be absent: {check['path']}"
        elif check_type == "json_required_keys":
            ok, message = enforce_path_contract(
                paths,
                {
                    "path": check["path"],
                    "type": "json",
                    "required": True,
                    "schema": {"required_keys": check["required_keys"]},
                },
            )
            if not ok:
                return False, message
        elif check_type == "log_contains":
            log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
            if check["value"] not in log_text:
                return False, f"Step log does not contain expected text: {check['value']}"
        elif check_type == "command":
            result = subprocess.run(
                ["bash", "-lc", check["command"]],
                cwd=paths.workflow_dir,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                return False, f"Validation command failed: {check['command']}"
    return True, "ok"


def verify_success_gate(
    step: dict[str, Any], paths: WorkflowPaths, log_path: Path
) -> tuple[bool, str]:
    gate = parse_success_gate(step.get("success_gate"))
    gate_type = gate.get("type")
    if gate_type in {"noop", "description"}:
        return True, "ok"
    if gate_type == "log_contains":
        log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
        expected = gate.get("value", "")
        if expected and expected not in log_text:
            return False, f"Success gate not satisfied: missing log text `{expected}`"
        return True, "ok"
    if gate_type == "file_exists":
        return enforce_path_contract(
            paths, {"path": gate["path"], "type": "file", "required": True}
        )
    return True, "ok"


def verify_step_contracts(
    step: dict[str, Any], paths: WorkflowPaths, log_path: Path
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for contract in normalize_contracts(step.get("produces", [])):
        ok, message = enforce_path_contract(paths, contract)
        if not ok:
            errors.append(message)
    ok, message = run_validation_checks(step, paths, log_path)
    if not ok:
        errors.append(message)
    ok, message = verify_success_gate(step, paths, log_path)
    if not ok:
        errors.append(message)
    return not errors, errors


def verify_consumes(step: dict[str, Any], paths: WorkflowPaths) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for contract in normalize_contracts(step.get("consumes", [])):
        ok, message = enforce_path_contract(paths, contract)
        if not ok:
            errors.append(message)
    return not errors, errors


def detect_used_commands(script_path: Path) -> set[str]:
    commands: set[str] = set()
    if not script_path.exists():
        return commands
    for raw_line in script_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", stripped):
            continue
        try:
            tokens = shlex.split(stripped, comments=False, posix=True)
        except ValueError:
            tokens = stripped.split()
        while tokens and "=" in tokens[0] and not tokens[0].startswith((">", "<", "|")):
            name, _, _ = tokens[0].partition("=")
            if name.isidentifier():
                tokens.pop(0)
                continue
            break
        if not tokens:
            continue
        token = Path(tokens[0]).name
        if token in SHELL_BUILTINS:
            continue
        if token:
            commands.add(token)
    return commands


def enforce_security_policy(
    step: dict[str, Any], paths: WorkflowPaths, manifest: dict[str, Any], policy: dict[str, Any]
) -> tuple[bool, str]:
    working_directory = step.get("working_directory", manifest.get("working_directory", "."))
    resolved_workdir = resolve_safe_path(paths.workflow_dir, working_directory)
    allowed_workdirs = policy.get("environment", {}).get("allowed_working_directories", ["."])
    if isinstance(allowed_workdirs, list):
        allowed_paths = [
            resolve_safe_path(paths.workflow_dir, entry)
            for entry in allowed_workdirs
            if isinstance(entry, str)
        ]
        if allowed_paths and not any(
            allowed in (resolved_workdir, *resolved_workdir.parents) for allowed in allowed_paths
        ):
            return False, f"Working directory not allowed by policy: {working_directory}"
    if step["type"] == "mcp":
        # Enforce network policy: MCP steps that use remote SSE URLs are network calls
        network_mode = policy.get("environment", {}).get(
            "network_mode", manifest.get("environment", {}).get("network_mode", "inherit")
        )
        executor_config = step.get("executor_config", {})
        server_name = executor_config.get("server", "")
        # Validate server is in an explicit allowlist if one is configured
        allowed_mcp_servers = policy.get("tooling", {}).get("allowed_mcp_servers")
        if isinstance(allowed_mcp_servers, list) and allowed_mcp_servers:
            if server_name not in allowed_mcp_servers:
                return (
                    False,
                    f"MCP server '{server_name}' not in policy allowed_mcp_servers allowlist.",
                )
        # Block MCP steps that would make network calls under offline policy
        # (We can't know without the registry config, so we conservatively block all MCP in offline mode)
        if network_mode == "offline":
            return False, "Offline policy blocks MCP steps (may involve network I/O)."
        return True, "ok"
    if step["type"] in {"shell", "test", "transform", "publish", "sidecar-consume", "approval"}:
        script_path = resolve_safe_path(paths.workflow_dir, step["script"])
        commands = detect_used_commands(script_path)
        network_mode = policy.get("environment", {}).get(
            "network_mode", manifest.get("environment", {}).get("network_mode", "inherit")
        )
        if network_mode == "offline":
            online_commands = sorted(command for command in commands if command in NETWORK_COMMANDS)
            if online_commands:
                return (
                    False,
                    f"Offline policy blocks network commands: {', '.join(online_commands)}",
                )
        allowlisted = set(policy.get("tooling", {}).get("allowlisted_commands", []))
        if allowlisted:
            disallowed = sorted(command for command in commands if command not in allowlisted)
            if disallowed:
                return False, f"Script uses commands outside allowlist: {', '.join(disallowed)}"
    return True, "ok"


def build_step_env(policy: dict[str, Any]) -> dict[str, str]:
    env_policy = policy.get("environment", {})
    allowed_env = env_policy.get("allowed_env")
    if not isinstance(allowed_env, list) or not allowed_env:
        print(
            "[runner] WARNING: No `allowed_env` in policy; step inherits full environment.",
            file=sys.stderr,
        )
        return dict(os.environ)
    prefixes = [
        entry[:-1] for entry in allowed_env if isinstance(entry, str) and entry.endswith("*")
    ]
    exact = {entry for entry in allowed_env if isinstance(entry, str) and not entry.endswith("*")}
    result: dict[str, str] = {}
    for key, value in os.environ.items():
        if key in exact or any(key.startswith(prefix) for prefix in prefixes):
            result[key] = value
    return result


def load_mcp_registry(manifest: dict[str, Any], workflow_dir: Path) -> dict[str, Any]:
    """Return {server_name: server_config}. First match wins: manifest key, workflow .mcp.json, cwd .mcp.json."""
    if "mcp_servers" in manifest:
        return manifest["mcp_servers"].get("mcpServers", manifest["mcp_servers"])
    for candidate in (workflow_dir / ".mcp.json", Path.cwd() / ".mcp.json"):
        if candidate.exists():
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
                return data.get("mcpServers", {})
            except (json.JSONDecodeError, OSError):
                pass
    return {}


def expand_mcp_params(params: Any, workflow_dir: Path) -> Any:
    """Recursively expand {{...}} template patterns in string values."""
    if isinstance(params, dict):
        return {k: expand_mcp_params(v, workflow_dir) for k, v in params.items()}
    if isinstance(params, list):
        return [expand_mcp_params(item, workflow_dir) for item in params]
    if not isinstance(params, str):
        return params

    def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
        expr = match.group(1)
        if expr.startswith("env:"):
            return os.environ.get(expr[4:], "")
        if ":" in expr:
            file_part, _, key_part = expr.partition(":")
            file_path = workflow_dir / file_part
            if not file_path.exists():
                raise ValueError(f"MCP param template: artifact not found: {file_part}")
            data = json.loads(file_path.read_text(encoding="utf-8"))
            for part in key_part.split("."):
                if not isinstance(data, dict) or part not in data:
                    raise ValueError(
                        f"MCP param template: key '{key_part}' not found in {file_part}"
                    )
                data = data[part]
            return str(data)
        # Raw file content
        file_path = workflow_dir / expr
        if not file_path.exists():
            raise ValueError(f"MCP param template: artifact not found: {expr}")
        return file_path.read_text(encoding="utf-8").strip()

    return re.sub(r"\{\{([^}]+)\}\}", _replace, params)


def run_mcp_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    policy: dict[str, Any],
    log_path: Path,
) -> tuple[int, str]:
    executor_config = step.get("executor_config", {})
    server_name = executor_config.get("server", "")
    tool_name = executor_config.get("tool", "")
    params = executor_config.get("params", {})
    output_artifact = executor_config.get("output_artifact")

    registry = load_mcp_registry(manifest, paths.workflow_dir)
    if server_name not in registry:
        msg = f"[runner] MCP server '{server_name}' not found in registry.\n"
        atomic_write_text(log_path, msg)
        return 1, "mcp-error"

    try:
        import importlib

        importlib.import_module("mcp")
    except ImportError:
        msg = "[runner] mcp step requires: pip install 'mcp>=1.0'\n"
        atomic_write_text(log_path, msg)
        return 1, "missing-dependency"

    try:
        expanded_params = expand_mcp_params(params, paths.workflow_dir)
    except ValueError as exc:
        atomic_write_text(log_path, f"[runner] MCP param expansion error: {exc}\n")
        return 1, "mcp-error"

    server_config = registry[server_name]

    try:
        import asyncio  # noqa: PLC0415

        from mcp import ClientSession  # type: ignore[import]  # noqa: PLC0415, I001
        from mcp.client.sse import sse_client  # type: ignore[import]  # noqa: PLC0415
        from mcp.client.stdio import (  # type: ignore[import]  # noqa: PLC0415
            StdioServerParameters,
            stdio_client,
        )

        async def _call_tool() -> Any:
            if "url" in server_config:
                async with sse_client(server_config["url"]) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        return await session.call_tool(tool_name, expanded_params)
            else:
                cmd = server_config.get("command", "")
                args = server_config.get("args", [])
                env_override = server_config.get("env", {})
                merged_env = {**os.environ, **env_override}
                server_params = StdioServerParameters(command=cmd, args=args, env=merged_env)
                async with stdio_client(server_params) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        return await session.call_tool(tool_name, expanded_params)

        result = asyncio.run(_call_tool())
        result_json = json.dumps({"result": str(result)}, indent=2)
        atomic_write_text(log_path, f"[runner] MCP tool '{tool_name}' succeeded.\n{result_json}\n")
        if output_artifact:
            artifact_path = paths.workflow_dir / output_artifact
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_text(artifact_path, result_json + "\n")
        return 0, "mcp"
    except Exception as exc:
        atomic_write_text(log_path, f"[runner] MCP tool call failed: {exc}\n")
        return 1, "mcp-error"


def expand_claude_template(template: str, paths: WorkflowPaths) -> str:
    """Expand {{artifact:step-id}} (reads artifacts/{step-id}.out) and {{env:VAR}} in a prompt."""

    def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
        expr = match.group(1)
        if expr.startswith("env:"):
            return os.environ.get(expr[4:], "")
        if expr.startswith("artifact:"):
            artifact_id = expr[9:]
            artifact_path = paths.workflow_dir / "artifacts" / f"{artifact_id}.out"
            if not artifact_path.exists():
                raise ValueError(f"claude template: artifact not found: {artifact_id}.out")
            return artifact_path.read_text(encoding="utf-8").strip()
        return match.group(0)

    return re.sub(r"\{\{([^}]+)\}\}", _replace, template)


def validate_output_schema(response: str, schema: dict[str, Any]) -> tuple[bool, str]:
    """Parse response as JSON and check required_keys from schema."""
    required_keys = schema.get("required_keys", [])
    if not required_keys:
        return True, ""
    try:
        parsed = json.loads(response)
    except json.JSONDecodeError:
        return False, "claude step output_schema: response is not valid JSON"
    if not isinstance(parsed, dict):
        return False, "claude step output_schema: response JSON must be an object"
    missing = [k for k in required_keys if k not in parsed]
    if missing:
        return False, f"claude step output_schema: missing required keys: {missing}"
    return True, ""


def run_claude_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Run a claude step: expand template, call Claude CLI or SDK, write artifact."""
    step_id = step["id"]
    model = step.get("model", "claude-sonnet-4-6")
    timeout_seconds = int(step.get("timeout_seconds", 300))

    try:
        prompt = expand_claude_template(step.get("prompt", ""), paths)
    except ValueError as exc:
        atomic_write_text(log_path, f"[runner] claude template expansion error: {exc}\n")
        return 1, "template-error"

    response_text: str | None = None
    claude_bin = shutil.which("claude")

    if claude_bin:
        try:
            result = subprocess.run(
                [claude_bin, "-p", prompt, "--model", model],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                cwd=paths.workflow_dir,
            )
            response_text = result.stdout
            if result.returncode != 0:
                msg = f"[runner] claude CLI exited {result.returncode}\n{result.stderr}\n"
                atomic_write_text(log_path, msg)
                return result.returncode, "claude-error"
        except subprocess.TimeoutExpired:
            atomic_write_text(log_path, "[runner] claude step timed out\n")
            return 1, "timeout"
    else:
        try:
            import anthropic  # type: ignore[import]  # noqa: PLC0415

            client = anthropic.Anthropic()
            msg = client.messages.create(
                model=model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = msg.content[0].text
        except ImportError:
            atomic_write_text(
                log_path,
                "[runner] claude step requires the claude CLI or: pip install anthropic\n",
            )
            return 1, "missing-dependency"
        except Exception as exc:
            atomic_write_text(log_path, f"[runner] anthropic SDK error: {exc}\n")
            return 1, "claude-error"

    artifact_path = paths.workflow_dir / "artifacts" / f"{step_id}.out"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, (response_text or "") + "\n")
    atomic_write_text(log_path, f"[runner] claude step completed.\n{response_text or ''}\n")

    output_schema = step.get("output_schema")
    if output_schema and isinstance(output_schema, dict):
        ok, err = validate_output_schema(response_text or "", output_schema)
        if not ok:
            atomic_write_text(log_path, f"[runner] {err}\n")
            return 1, "schema-violation"

    return 0, "claude"


def run_branch_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str, list[str]]:
    """Run condition script. Returns (returncode, category, skipped_step_ids)."""
    condition_script = resolve_safe_path(paths.workflow_dir, step["condition"])
    timeout_seconds = int(step.get("timeout_seconds", 30))
    on_true: list[str] = step.get("on_true", [])
    on_false: list[str] = step.get("on_false", [])
    try:
        with log_path.open("w", encoding="utf-8") as handle:
            result = subprocess.run(
                ["bash", str(condition_script)],
                cwd=paths.workflow_dir,
                text=True,
                stdout=handle,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=timeout_seconds,
            )
        branch_taken = "true" if result.returncode == 0 else "false"
        skipped = on_false if result.returncode == 0 else on_true
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(
                f"\n[branch] condition exited {result.returncode} → branch={branch_taken}\n"
            )
        return 0, f"branch:{branch_taken}", skipped
    except subprocess.TimeoutExpired:
        atomic_write_text(log_path, "[runner] branch condition timed out\n")
        return 1, "timeout", []
    except Exception as exc:
        atomic_write_text(log_path, f"[runner] branch condition error: {exc}\n")
        return 1, "branch-error", []


def _read_artifact(paths: WorkflowPaths, artifact_id: str) -> str | None:
    """Read the first existing artifact file for the given id (.json, .out, or bare)."""
    for suffix in (".json", ".out", ""):
        p = paths.workflow_dir / "artifacts" / f"{artifact_id}{suffix}"
        if p.exists():
            return p.read_text(encoding="utf-8")
    return None


def run_http_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Full HTTP request step: method, url, headers, body, auth, response → artifact."""
    import urllib.error  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    method = step.get("method", "GET").upper()
    timeout_seconds = int(step.get("timeout_seconds", 30))
    fail_on_error = step.get("fail_on_error", True)

    try:
        url = expand_claude_template(step.get("url", ""), paths)
        raw_headers: dict[str, str] = {
            k: expand_claude_template(v, paths) for k, v in step.get("headers", {}).items()
        }
        raw_body = step.get("body")
        if isinstance(raw_body, dict):
            body_bytes: bytes | None = json.dumps(raw_body).encode()
            raw_headers.setdefault("Content-Type", "application/json")
        elif isinstance(raw_body, str):
            body_bytes = expand_claude_template(raw_body, paths).encode()
        else:
            body_bytes = None
    except ValueError as exc:
        atomic_write_text(log_path, f"[http] template expansion error: {exc}\n")
        return 1, "template-error"

    auth = step.get("auth", {})
    if isinstance(auth, dict):
        auth_type = auth.get("type", "none")
        if auth_type == "bearer":
            token = expand_claude_template(auth.get("token", ""), paths)
            raw_headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "basic":
            import base64  # noqa: PLC0415

            username = expand_claude_template(auth.get("username", ""), paths)
            password = expand_claude_template(auth.get("password", ""), paths)
            creds = base64.b64encode(f"{username}:{password}".encode()).decode()
            raw_headers["Authorization"] = f"Basic {creds}"

    req = urllib.request.Request(url, data=body_bytes, headers=raw_headers, method=method)
    status_code = 0
    response_body = ""
    response_headers: dict[str, str] = {}

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
            status_code = resp.status
            response_body = resp.read().decode(errors="replace")
            response_headers = dict(resp.headers)
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        response_body = exc.read().decode(errors="replace")
    except Exception as exc:
        atomic_write_text(log_path, f"[http] request failed: {exc}\n")
        return 1, "http-error"

    result = {"status_code": status_code, "headers": response_headers, "body": response_body}
    artifact_path = paths.workflow_dir / "artifacts" / f"{step['id']}.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, json.dumps(result, indent=2) + "\n")
    atomic_write_text(
        log_path,
        f"[http] {method} {url} → {status_code}\n{response_body[:1000]}\n",
    )

    if fail_on_error and status_code >= 400:
        return 1, "http-error"
    return 0, "http"


def run_switch_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str, list[str]]:
    """Multi-way branch: evaluate expression, skip all non-matching case steps."""
    try:
        expression = expand_claude_template(step.get("expression", ""), paths)
    except ValueError as exc:
        atomic_write_text(log_path, f"[switch] template expansion error: {exc}\n")
        return 1, "template-error", []

    cases: list[dict[str, Any]] = step.get("cases", [])
    default_steps: list[str] = step.get("default", [])

    matched_steps: list[str] | None = None
    for case in cases:
        if str(case.get("value", "")) == expression:
            matched_steps = case.get("steps", [])
            break
    if matched_steps is None:
        matched_steps = default_steps

    all_case_steps: set[str] = set()
    for case in cases:
        all_case_steps.update(case.get("steps", []))
    all_case_steps.update(default_steps)
    skipped = [s for s in all_case_steps if s not in matched_steps]

    atomic_write_text(
        log_path,
        f"[switch] expression={expression!r} → {len(matched_steps)} step(s) active,"
        f" {len(skipped)} skipped\n",
    )
    return 0, f"switch:{expression}", skipped


def run_loop_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Iterate over items from an artifact, running a script once per item."""
    items_from = step.get("items_from", "")
    script_rel = step.get("script", "")
    timeout_seconds = int(step.get("timeout_seconds", 300))

    content = _read_artifact(paths, items_from)
    if content is None:
        atomic_write_text(log_path, f"[loop] items_from artifact not found: {items_from}\n")
        return 1, "loop-error"

    content = content.strip()
    try:
        items: list[Any] = json.loads(content)
        if not isinstance(items, list):
            items = [items]
    except json.JSONDecodeError:
        items = [line for line in content.splitlines() if line.strip()]

    script_path = resolve_safe_path(paths.workflow_dir, script_rel)
    results: list[dict[str, Any]] = []
    all_ok = True

    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"[loop] Processing {len(items)} item(s)...\n")
        for idx, item in enumerate(items):
            env = {
                **os.environ,
                "LOOP_ITEM": str(item),
                "LOOP_INDEX": str(idx),
                "LOOP_TOTAL": str(len(items)),
            }
            try:
                proc = subprocess.run(
                    ["bash", str(script_path)],
                    cwd=paths.workflow_dir,
                    env=env,
                    text=True,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=timeout_seconds,
                )
                results.append({"index": idx, "item": item, "returncode": proc.returncode})
                if proc.returncode != 0:
                    all_ok = False
                    if not step.get("continue_on_error", False):
                        log_handle.write(f"[loop] item #{idx} failed; stopping.\n")
                        break
            except subprocess.TimeoutExpired:
                all_ok = False
                results.append({"index": idx, "item": item, "returncode": -1, "error": "timeout"})
                log_handle.write(f"[loop] item #{idx} timed out.\n")
                break

    artifact_path = paths.workflow_dir / "artifacts" / f"{step['id']}.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, json.dumps(results, indent=2) + "\n")
    return 0 if all_ok else 1, "loop"


def run_wait_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Pause execution for `seconds` or until a condition script exits 0."""
    seconds = step.get("seconds")
    until_script = step.get("until", "")
    timeout_seconds = int(step.get("timeout_seconds", 3600))
    poll_interval = int(step.get("poll_seconds", 10))

    if until_script:
        script_path = resolve_safe_path(paths.workflow_dir, until_script)
        elapsed = 0
        while elapsed < timeout_seconds:
            proc = subprocess.run(
                ["bash", str(script_path)],
                capture_output=True,
                cwd=paths.workflow_dir,
                check=False,
            )
            if proc.returncode == 0:
                atomic_write_text(log_path, f"[wait] condition met after {elapsed}s\n")
                return 0, "wait"
            time.sleep(poll_interval)
            elapsed += poll_interval
        atomic_write_text(log_path, f"[wait] timed out after {timeout_seconds}s\n")
        return 1, "wait-timeout"

    duration = float(seconds) if seconds is not None else 0
    atomic_write_text(log_path, f"[wait] sleeping {duration}s\n")
    time.sleep(duration)
    return 0, "wait"


def run_merge_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Merge data from multiple artifact inputs into one output artifact."""
    inputs: list[str] = step.get("inputs", [])
    mode = step.get("mode", "concat")

    data: list[Any] = []
    for artifact_id in inputs:
        raw = _read_artifact(paths, artifact_id)
        if raw is not None:
            try:
                data.append(json.loads(raw.strip()))
            except json.JSONDecodeError:
                data.append(raw.strip())

    if mode == "concat":
        if all(isinstance(d, list) for d in data):
            result: Any = [item for sublist in data for item in sublist]
        elif all(isinstance(d, dict) for d in data):
            merged: dict[str, Any] = {}
            for d in data:
                merged.update(d)
            result = merged
        else:
            result = data
    elif mode == "zip":
        lists = [d if isinstance(d, list) else [d] for d in data]
        result = [list(row) for row in zip(*lists)]
    elif mode == "first":
        result = data[0] if data else None
    else:
        result = data

    artifact_path = paths.workflow_dir / "artifacts" / f"{step['id']}.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, json.dumps(result, indent=2) + "\n")
    atomic_write_text(log_path, f"[merge] merged {len(inputs)} input(s), mode={mode}\n")
    return 0, "merge"


def run_workflow_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Run another workflow.json as a sub-workflow, optionally passing/collecting artifacts."""
    sub_dir_raw = step.get("workflow_dir", "")
    sub_dir = (paths.workflow_dir / sub_dir_raw).resolve()
    pass_artifacts: list[str] = step.get("pass_artifacts", [])
    collect_artifacts: list[str] = step.get("collect_artifacts", [])
    timeout_seconds = int(step.get("timeout_seconds", 7200))

    for artifact_id in pass_artifacts:
        raw = _read_artifact(paths, artifact_id)
        if raw is not None:
            dst = sub_dir / "artifacts" / f"{artifact_id}.out"
            dst.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_text(dst, raw)

    runner = Path(__file__).resolve()
    try:
        proc = subprocess.run(
            [sys.executable, str(runner), str(sub_dir)],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        atomic_write_text(log_path, f"[workflow] sub-workflow timed out after {timeout_seconds}s\n")
        return 1, "timeout"

    atomic_write_text(
        log_path,
        f"[workflow] sub-workflow {sub_dir.name} exit={proc.returncode}\n"
        f"{proc.stdout}\n{proc.stderr}\n",
    )

    sub_paths = build_paths(sub_dir)
    for artifact_id in collect_artifacts:
        raw = _read_artifact(sub_paths, artifact_id)
        if raw is not None:
            dst = paths.workflow_dir / "artifacts" / f"{artifact_id}.out"
            dst.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_text(dst, raw)

    return proc.returncode, "workflow"


def run_skill_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Invoke a named Codex or Claude Code skill as a workflow step."""
    import importlib.util  # noqa: PLC0415

    skill_name: str = step.get("skill", "")
    instruction: str = step.get("instruction", "")
    timeout_seconds = int(step.get("timeout_seconds", 300))
    pass_artifacts: list[str] = step.get("pass_artifacts", [])

    discover_script = Path(__file__).resolve().parent / "discover_skills.py"
    spec = importlib.util.spec_from_file_location("discover_skills", discover_script)
    if spec is None or spec.loader is None:
        atomic_write_text(log_path, "[skill] discover_skills.py not found\n")
        return 1, "skill-error"
    ds = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ds)  # type: ignore[union-attr]

    skills = ds.discover()
    skill_entry = ds.find_skill(skill_name, skills)
    if skill_entry is None:
        atomic_write_text(
            log_path,
            f"[skill] Skill '{skill_name}' not found. Available: "
            + ", ".join(s["name"] for s in skills[:10])
            + "\n",
        )
        return 1, "skill-not-found"

    skill_md = ds.read_skill_md(skill_entry)

    artifact_context = ""
    for artifact_id in pass_artifacts:
        raw = _read_artifact(paths, artifact_id)
        if raw:
            artifact_context += f"\n\n--- artifact:{artifact_id} ---\n{raw.strip()}\n"

    prompt_parts = []
    if skill_md:
        prompt_parts.append(f"Follow the methodology in this skill:\n\n{skill_md}")
    if artifact_context:
        prompt_parts.append(f"Workflow artifacts for context:{artifact_context}")
    if instruction:
        try:
            instruction = expand_claude_template(instruction, paths)
        except ValueError:
            pass
        prompt_parts.append(f"Your specific task for this workflow step:\n{instruction}")
    if not prompt_parts:
        prompt_parts.append(f"Execute the {skill_name} skill.")

    full_prompt = "\n\n".join(prompt_parts)

    claude_bin = shutil.which("claude")
    if not claude_bin:
        atomic_write_text(log_path, "[skill] claude CLI not found — required for skill steps\n")
        return 1, "missing-dependency"

    try:
        result = subprocess.run(
            [claude_bin, "-p", full_prompt, "--model", step.get("model", "claude-sonnet-4-6")],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=paths.workflow_dir,
        )
    except subprocess.TimeoutExpired:
        atomic_write_text(log_path, f"[skill] timed out after {timeout_seconds}s\n")
        return 1, "timeout"

    response = result.stdout or ""
    artifact_path = paths.workflow_dir / "artifacts" / f"{step['id']}.out"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, response + "\n")
    atomic_write_text(
        log_path,
        f"[skill] {skill_name} ({skill_entry['source']}) exit={result.returncode}\n{response[:500]}\n",
    )

    return result.returncode, f"skill:{skill_name}"


def _run_claude_with_tools(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
    allowed_tools: str,
    log_prefix: str,
) -> tuple[int, str]:
    """Shared runner for browser and computer-use steps: call claude CLI with restricted tools."""
    claude_bin = shutil.which("claude")
    if not claude_bin:
        atomic_write_text(
            log_path, f"[{log_prefix}] claude CLI not found — required for {log_prefix} steps\n"
        )
        return 1, "missing-dependency"

    try:
        instruction = expand_claude_template(step.get("instruction", ""), paths)
    except ValueError as exc:
        atomic_write_text(log_path, f"[{log_prefix}] template expansion error: {exc}\n")
        return 1, "template-error"

    if not instruction:
        atomic_write_text(log_path, f"[{log_prefix}] 'instruction' field is required\n")
        return 1, "config-error"

    timeout_seconds = int(step.get("timeout_seconds", 300))
    model = step.get("model", "claude-sonnet-4-6")

    try:
        result = subprocess.run(
            [claude_bin, "-p", instruction, "--allowedTools", allowed_tools, "--model", model],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=paths.workflow_dir,
        )
    except subprocess.TimeoutExpired:
        atomic_write_text(log_path, f"[{log_prefix}] timed out after {timeout_seconds}s\n")
        return 1, "timeout"

    response = result.stdout or ""
    artifact_id = step.get("output_artifact", step["id"])
    artifact_path = paths.workflow_dir / "artifacts" / f"{artifact_id}.out"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(artifact_path, response + "\n")

    atomic_write_text(
        log_path,
        f"[{log_prefix}] exit={result.returncode}\n{response[:500]}\n",
    )

    if result.returncode != 0:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(result.stderr[:500] + "\n")

    return result.returncode, log_prefix


def run_browser_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Execute a browser automation step via Claude + Chrome MCP integration."""
    return _run_claude_with_tools(
        step,
        paths,
        log_path,
        allowed_tools="mcp__Claude_in_Chrome__navigate,mcp__Claude_in_Chrome__read_page,mcp__Claude_in_Chrome__get_page_text,mcp__Claude_in_Chrome__find,mcp__Claude_in_Chrome__form_input,mcp__Claude_in_Chrome__javascript_tool,mcp__Claude_in_Chrome__read_network_requests,mcp__Claude_in_Chrome__read_console_messages,mcp__Claude_in_Chrome__tabs_create_mcp,mcp__Claude_in_Chrome__tabs_close_mcp",
        log_prefix="browser",
    )


def run_computer_use_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    log_path: Path,
) -> tuple[int, str]:
    """Execute a desktop automation step via Claude + computer-use MCP."""
    return _run_claude_with_tools(
        step,
        paths,
        log_path,
        allowed_tools="mcp__computer-use__screenshot,mcp__computer-use__left_click,mcp__computer-use__type,mcp__computer-use__key,mcp__computer-use__scroll,mcp__computer-use__mouse_move,mcp__computer-use__double_click,mcp__computer-use__right_click,mcp__computer-use__open_application,mcp__computer-use__computer_batch",
        log_prefix="computer-use",
    )


def run_command_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    policy: dict[str, Any],
    log_path: Path,
) -> tuple[int, str]:
    if step["type"] == "mcp":
        return run_mcp_step(step, paths, manifest, policy, log_path)
    if step["type"] == "claude":
        return run_claude_step(step, paths, log_path)
    if step["type"] == "http":
        return run_http_step(step, paths, log_path)
    if step["type"] == "loop":
        return run_loop_step(step, paths, log_path)
    if step["type"] == "wait":
        return run_wait_step(step, paths, log_path)
    if step["type"] == "merge":
        return run_merge_step(step, paths, log_path)
    if step["type"] == "workflow":
        return run_workflow_step(step, paths, log_path)
    if step["type"] == "skill":
        return run_skill_step(step, paths, log_path)
    if step["type"] == "browser":
        return run_browser_step(step, paths, log_path)
    if step["type"] == "computer-use":
        return run_computer_use_step(step, paths, log_path)
    working_directory = step.get("working_directory", manifest.get("working_directory", "."))
    cwd = resolve_safe_path(paths.workflow_dir, working_directory)
    env = build_step_env(policy)
    timeout_seconds = int(step.get("timeout_seconds", 1800))
    if step["type"] in {"shell", "test", "transform", "publish", "sidecar-consume", "approval"}:
        script_path = resolve_safe_path(paths.workflow_dir, step["script"])
        try:
            with log_path.open("w", encoding="utf-8") as handle:
                result = subprocess.run(
                    ["bash", str(script_path)],
                    cwd=cwd,
                    env=env,
                    text=True,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=timeout_seconds,
                )
            return result.returncode, "command"
        except subprocess.TimeoutExpired:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"\n[runner] Step timed out after {timeout_seconds} seconds.\n")
            return 124, "timeout"
    if step["type"] == "manual-approval":
        atomic_write_text(
            log_path,
            "[runner] Manual approval step completed after approval record was supplied.\n",
        )
        return 0, "manual-approval"
    executor_config = step.get("executor_config", {})
    if step["type"] == "file-exists":
        target = resolve_safe_path(paths.workflow_dir, executor_config["path"])
        atomic_write_text(log_path, f"[runner] Checking file exists: {executor_config['path']}\n")
        return (0, "native") if target.exists() else (1, "native")
    if step["type"] == "json-validate":
        target = resolve_safe_path(paths.workflow_dir, executor_config["path"])
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
            required_keys = executor_config.get("required_keys", [])
            if required_keys and (
                not isinstance(payload, dict) or any(key not in payload for key in required_keys)
            ):
                atomic_write_text(log_path, "[runner] JSON validation failed.\n")
                return 1, "native"
            atomic_write_text(log_path, "[runner] JSON validation passed.\n")
            return 0, "native"
        except Exception as exc:
            atomic_write_text(log_path, f"[runner] JSON validation error: {exc}\n")
            return 1, "native"
    if step["type"] == "python":
        script_path = executor_config.get("script") or step.get("script")
        if not isinstance(script_path, str) or not script_path:
            atomic_write_text(log_path, "[runner] Missing python script path.\n")
            return 1, "native"
        target = resolve_safe_path(paths.workflow_dir, script_path)
        try:
            with log_path.open("w", encoding="utf-8") as handle:
                result = subprocess.run(
                    ["python3", str(target)],
                    cwd=cwd,
                    env=env,
                    text=True,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=timeout_seconds,
                )
            return result.returncode, "native"
        except subprocess.TimeoutExpired:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"\n[runner] Step timed out after {timeout_seconds} seconds.\n")
            return 124, "timeout"
    if step["type"] == "copy":
        source = resolve_safe_path(paths.workflow_dir, executor_config["source"])
        destination = resolve_safe_path(paths.workflow_dir, executor_config["destination"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        atomic_write_text(
            log_path,
            f"[runner] Copied {executor_config['source']} -> {executor_config['destination']}\n",
        )
        return 0, "native"
    if step["type"] == "http-check":
        try:
            request = urllib.request.Request(executor_config["url"], method="GET")
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                status = response.getcode()
            expected_status = int(executor_config.get("status_code", 200))
            atomic_write_text(log_path, f"[runner] HTTP status={status}\n")
            return (0, "native") if status == expected_status else (1, "native")
        except urllib.error.URLError as exc:
            atomic_write_text(log_path, f"[runner] HTTP check failed: {exc}\n")
            return 1, "native"
    if step["type"] == "git-diff-check":
        pathspec = executor_config.get("pathspec", ".")
        require_clean = bool(executor_config.get("require_clean", True))
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", pathspec],
            cwd=paths.workflow_dir,
            text=True,
            capture_output=True,
            check=False,
        )
        atomic_write_text(log_path, redact_text(result.stdout))
        if require_clean:
            return (0, "native") if not result.stdout.strip() else (1, "native")
        return 0, "native"
    atomic_write_text(log_path, f"[runner] Unsupported step type: {step['type']}\n")
    return 1, "native"


def run_rollback(
    step: dict[str, Any],
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    policy: dict[str, Any],
    run_context: RunContext,
) -> None:
    rollback = step.get("rollback")
    if not isinstance(rollback, dict):
        return
    log_path = paths.log_dir / f"{step['id']}.rollback.log"
    script_path = resolve_safe_path(paths.workflow_dir, rollback["script"])
    for precondition in rollback.get("preconditions", []):
        if isinstance(precondition, str):
            target = resolve_safe_path(paths.workflow_dir, precondition)
            if not target.exists():
                return
    cwd = resolve_safe_path(
        paths.workflow_dir, step.get("working_directory", manifest.get("working_directory", "."))
    )
    with log_path.open("w", encoding="utf-8") as handle:
        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=cwd,
            env=build_step_env(policy),
            text=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=int(step.get("timeout_seconds", 1800)),
        )
    if run_context.run_dir is not None:
        shutil.copyfile(log_path, run_context.run_dir / "logs" / f"{step['id']}.rollback.log")
        record_event(
            run_context,
            {"event": "rollback", "step_id": step["id"], "returncode": result.returncode},
        )


def mark_step_status(paths: WorkflowPaths, step_id: str, status: str) -> None:
    with STATE_IO_LOCK:
        step_state = read_tsv_state(paths.step_state_path)
        step_state[step_id] = status
        write_tsv_state(paths.step_state_path, step_state)


def mark_approval_status(paths: WorkflowPaths, step_id: str, status: str) -> None:
    with STATE_IO_LOCK:
        approval_state = read_tsv_state(paths.approval_state_path)
        approval_state[step_id] = status
        write_tsv_state(paths.approval_state_path, approval_state)


def execute_single_step(
    manifest: dict[str, Any],
    step: dict[str, Any],
    paths: WorkflowPaths,
    policy: dict[str, Any],
    run_context: RunContext,
) -> StepResult:
    step_id = step["id"]
    log_path = paths.log_dir / f"{step_id}.log"
    retries = int(step.get("retry_limit", policy.get("failure_policy", {}).get("max_retries", 0)))

    ok, consume_errors = verify_consumes(step, paths)
    if not ok:
        atomic_write_text(log_path, "\n".join(consume_errors) + "\n")
        mark_step_status(paths, step_id, "failed")
        update_runtime_step(
            paths,
            step_id,
            {
                "status": "failed",
                "ended_at": utc_now(),
                "last_error": consume_errors[0],
                "category": "input-contract",
            },
        )
        return StepResult(
            step_id=step_id,
            returncode=1,
            category="input-contract",
            message=consume_errors[0],
            duration_seconds=0.0,
        )

    ok, message = enforce_security_policy(step, paths, manifest, policy)
    if not ok:
        atomic_write_text(log_path, message + "\n")
        mark_step_status(paths, step_id, "failed")
        update_runtime_step(
            paths,
            step_id,
            {
                "status": "failed",
                "ended_at": utc_now(),
                "last_error": message,
                "category": "security",
            },
        )
        return StepResult(
            step_id=step_id,
            returncode=1,
            category="security",
            message=message,
            duration_seconds=0.0,
        )

    mark_step_status(paths, step_id, "running")
    record_sidecars(paths, manifest, step_id)
    # Run "before" sidecar scripts
    for sidecar in manifest.get("sidecars", []):
        if sidecar.get("consumer_step") == step_id and sidecar.get("when") == "before":
            run_sidecar_script(sidecar, paths, manifest, run_context)
    base_attempt = int(step.get("_attempt", 0))
    last_result = StepResult(
        step_id=step_id, returncode=1, category="failed", message="failed", duration_seconds=0.0
    )
    for attempt in range(base_attempt, base_attempt + retries + 1):
        started_at = time.monotonic()
        update_runtime_step(
            paths,
            step_id,
            {
                "status": "running",
                "started_at": utc_now(),
                "last_attempt": attempt,
                "pid": os.getpid(),
                "run_id": run_context.run_id,
            },
        )
        record_event(
            run_context,
            {"event": "step_started", "step_id": step_id, "type": step["type"], "attempt": attempt},
        )
        if run_context.run_dir is not None:
            append_text(
                run_context.run_dir / "commands.log",
                f"BEGIN\t{step_id}\tattempt={attempt}\t{step.get('script', '-')}",
            )

        if step["type"] == "branch":
            returncode, category, skipped_ids = run_branch_step(step, paths, log_path)
            if returncode == 0:
                for sid in skipped_ids:
                    mark_step_status(paths, sid, "skipped")
                    print(f"  ↷ skipped  {sid}  (branch not taken)", flush=True)
        elif step["type"] == "switch":
            returncode, category, skipped_ids = run_switch_step(step, paths, log_path)
            if returncode == 0:
                for sid in skipped_ids:
                    mark_step_status(paths, sid, "skipped")
                    print(f"  ↷ skipped  {sid}  (switch not matched)", flush=True)
        else:
            returncode, category = run_command_step(step, paths, manifest, policy, log_path)
        duration_seconds = round(time.monotonic() - started_at, 3)
        if run_context.run_dir is not None and log_path.exists():
            shutil.copyfile(log_path, run_context.run_dir / "logs" / f"{step_id}.log")

        if returncode == 0:
            ok, errors = verify_step_contracts(step, paths, log_path)
            if not ok:
                returncode = 1
                category = "contract"
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write("\n".join(errors) + "\n")

        if returncode == 0:
            # Run "after" sidecar scripts
            for sidecar in manifest.get("sidecars", []):
                if sidecar.get("consumer_step") == step_id and sidecar.get("when") == "after":
                    run_sidecar_script(sidecar, paths, manifest, run_context)
            mark_step_status(paths, step_id, "complete")
            if should_require_approval(step, policy):
                attach_approval_to_run(paths, run_context, step_id)
                if policy.get("approval", {}).get("auto_use_once", True):
                    mark_approval_status(paths, step_id, "used")
            update_runtime_step(
                paths,
                step_id,
                {
                    "status": "complete",
                    "ended_at": utc_now(),
                    "duration_seconds": duration_seconds,
                    "category": category,
                },
            )
            record_event(
                run_context,
                {
                    "event": "step_completed",
                    "step_id": step_id,
                    "duration_seconds": duration_seconds,
                    "attempt": attempt,
                },
            )
            if run_context.run_dir is not None:
                append_text(
                    run_context.run_dir / "commands.log",
                    f"COMPLETE\t{step_id}\tattempt={attempt}\tduration={duration_seconds}",
                )
            return StepResult(
                step_id=step_id,
                returncode=0,
                category=category,
                message="ok",
                duration_seconds=duration_seconds,
            )

        if category == "timeout":
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    f"[runner] Timeout enforced after {step.get('timeout_seconds', 1800)} seconds.\n"
                )
        last_result = StepResult(
            step_id=step_id,
            returncode=1,
            category=category,
            message="failed",
            duration_seconds=duration_seconds,
        )
        if attempt < base_attempt + retries:
            record_event(
                run_context,
                {
                    "event": "step_retry",
                    "step_id": step_id,
                    "attempt": attempt,
                    "category": category,
                },
            )
            if run_context.run_dir is not None:
                append_text(
                    run_context.run_dir / "commands.log",
                    f"RETRY\t{step_id}\tattempt={attempt}\tcategory={category}",
                )
            continue

    rollback = step.get("rollback")
    if isinstance(rollback, dict) and rollback.get("when") == "on_failure":
        run_rollback(step, paths, manifest, policy, run_context)
    mark_step_status(paths, step_id, "failed")
    auto_heal_step(step, last_result, paths, manifest, run_context)
    update_runtime_step(
        paths,
        step_id,
        {
            "status": "failed",
            "ended_at": utc_now(),
            "duration_seconds": last_result.duration_seconds,
            "category": last_result.category,
        },
    )
    record_event(
        run_context,
        {
            "event": "step_failed",
            "step_id": step_id,
            "category": last_result.category,
            "duration_seconds": last_result.duration_seconds,
        },
    )
    if run_context.run_dir is not None:
        append_text(
            run_context.run_dir / "commands.log",
            f"FAILED\t{step_id}\tcategory={last_result.category}\tduration={last_result.duration_seconds}",
        )
    return last_result


def record_metrics(paths: WorkflowPaths, run_context: RunContext, result: StepResult) -> None:
    def mutate(payload: dict[str, Any]) -> None:
        payload.setdefault("steps", {})
        metrics = payload["steps"].setdefault(
            result.step_id,
            {
                "runs": 0,
                "failures": 0,
                "timeouts": 0,
                "last_duration_seconds": 0.0,
                "last_failure_category": "",
            },
        )
        metrics["runs"] += 1
        metrics["last_duration_seconds"] = result.duration_seconds
        if result.returncode != 0:
            metrics["failures"] += 1
            metrics["last_failure_category"] = result.category
        if result.category == "timeout":
            metrics["timeouts"] += 1

    update_metrics(paths, mutate)
    run_context.metrics.setdefault("steps", {})
    run_context.metrics["steps"][result.step_id] = {
        "duration_seconds": result.duration_seconds,
        "returncode": result.returncode,
        "category": result.category,
    }


def reconcile_interrupted_steps(manifest: dict[str, Any], paths: WorkflowPaths) -> list[str]:
    ensure_state(paths, manifest)
    step_state = read_tsv_state(paths.step_state_path)
    repaired: list[str] = []
    for step_id, status in list(step_state.items()):
        if status == "running":
            step_state[step_id] = "interrupted"
            repaired.append(step_id)
            update_runtime_step(
                paths,
                step_id,
                {
                    "status": "interrupted",
                    "ended_at": utc_now(),
                    "last_error": "Runner exited before step completion.",
                },
            )
    if repaired:
        write_tsv_state(paths.step_state_path, step_state)
    return repaired


def ordered_subset(manifest: dict[str, Any], start_step: str | None = None) -> list[str]:
    order = simulate_step_order(manifest)
    if start_step is None:
        return order
    if start_step not in order:
        raise KeyError(start_step)
    return order[order.index(start_step) :]


def executable_order_index(manifest: dict[str, Any]) -> dict[str, int]:
    return {step_id: index for index, step_id in enumerate(simulate_step_order(manifest))}


def run_many(
    manifest: dict[str, Any],
    paths: WorkflowPaths,
    policy: dict[str, Any],
    *,
    start_step: str | None = None,
    dry_run: bool = False,
) -> int:
    order = ordered_subset(manifest, start_step)
    step_map = get_steps_by_id(manifest)
    step_state = read_tsv_state(paths.step_state_path)
    approval_state = read_tsv_state(paths.approval_state_path)
    # Policy takes precedence over manifest; manifest graph takes precedence over the default of 1.
    graph_parallel = manifest.get("graph", {}).get("max_parallel", 1)
    policy_parallel = policy.get("execution", {}).get("max_parallel", graph_parallel)
    max_parallel = max(1, int(policy_parallel))

    if dry_run:
        for step_id in order:
            step = step_map[step_id]
            if should_require_approval(step, policy) and approval_state.get(step_id) != "approved":
                print(f"WOULD WAIT APPROVAL\t{step_id}\t{step['type']}\t{step.get('script', '-')}")
            else:
                print(f"WOULD RUN\t{step_id}\t{step['type']}\t{step.get('script', '-')}")
        return 0

    run_context = (
        setup_run_audit(paths, manifest, policy)
        if audit_enabled(manifest, policy)
        else RunContext(run_id=None, run_dir=None, dry_run=False, metrics={"steps": {}})
    )
    done_statuses = {"complete", "skipped"}
    pending = [step_id for step_id in order if step_state.get(step_id) not in done_statuses]
    completed = {step_id for step_id, status in step_state.items() if status in done_statuses}
    running: dict[concurrent.futures.Future[StepResult], str] = {}
    stop_launching = False
    failure_code = 0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel)
    order_index = executable_order_index(manifest)

    try:
        while pending or running:
            launchable: list[str] = []
            if not stop_launching:
                for step_id in list(pending):
                    step = step_map[step_id]
                    deps = step.get("depends_on", [])
                    if any(dep not in completed for dep in deps):
                        continue
                    if (
                        should_require_approval(step, policy)
                        and read_tsv_state(paths.approval_state_path).get(step_id) != "approved"
                    ):
                        if read_tsv_state(paths.step_state_path).get(step_id) != "waiting-approval":
                            mark_step_status(paths, step_id, "waiting-approval")
                        continue
                    launchable.append(step_id)
                launchable.sort(key=lambda item: order_index[item])
                while launchable and len(running) < max_parallel:
                    step_id = launchable.pop(0)
                    pending.remove(step_id)
                    print(f"  → running  {step_id}", flush=True)
                    step = dict(step_map[step_id])
                    attempts = (
                        read_json_file(paths.runtime_state_path, {"steps": {}})
                        .get("steps", {})
                        .get(step_id, {})
                        .get("last_attempt", -1)
                    )
                    step["_attempt"] = int(attempts) + 1
                    future = executor.submit(
                        execute_single_step, manifest, step, paths, policy, run_context
                    )
                    running[future] = step_id
            if not running:
                if pending:
                    failure_code = (
                        3
                        if any(
                            read_tsv_state(paths.step_state_path).get(step_id) == "waiting-approval"
                            for step_id in pending
                        )
                        else 2
                    )
                    waiting = [
                        sid
                        for sid in pending
                        if read_tsv_state(paths.step_state_path).get(sid) == "waiting-approval"
                    ]
                    for sid in waiting:
                        print(f"  ⏸ approval {sid}  (run --approve {sid} to unblock)", flush=True)
                break

            done, _ = concurrent.futures.wait(
                list(running.keys()), return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                step_id = running.pop(future)
                result = future.result()
                record_metrics(paths, run_context, result)
                # Refresh skipped set in case a branch step just wrote new skipped entries
                fresh_state = read_tsv_state(paths.step_state_path)
                newly_skipped = {
                    sid
                    for sid, st in fresh_state.items()
                    if st == "skipped" and sid not in completed
                }
                for sid in newly_skipped:
                    completed.add(sid)
                    if sid in pending:
                        pending.remove(sid)
                if result.returncode == 0:
                    dur = f"  ({result.duration_seconds:.1f}s)" if result.duration_seconds else ""
                    print(f"  ✓ complete {step_id}{dur}", flush=True)
                    completed.add(step_id)
                else:
                    print(f"  ✗ failed   {step_id}  [{result.category}]", flush=True)
                    if policy.get("failure_policy", {}).get("on_error") != "continue":
                        stop_launching = True
                        failure_code = result.returncode
                    elif result.returncode != 0:
                        failure_code = result.returncode
        if failure_code == 3:
            print(
                "\nWorkflow paused — waiting for approval. Run --list to see which steps are blocked.",
                flush=True,
            )
        elif failure_code not in (0, 2):
            print(f"\nWorkflow stopped with errors (exit {failure_code}).", flush=True)
        return failure_code
    finally:
        executor.shutdown(wait=True)
        finalize_run_audit(paths, manifest, run_context)


def first_incomplete_step(manifest: dict[str, Any], paths: WorkflowPaths) -> str | None:
    step_state = read_tsv_state(paths.step_state_path)
    done_statuses = {"complete", "skipped"}
    for step_id in simulate_step_order(manifest):
        if step_state.get(step_id) not in done_statuses:
            return step_id
    return None


def approve_step(
    manifest: dict[str, Any],
    paths: WorkflowPaths,
    policy: dict[str, Any],
    step_id: str,
    *,
    approver: str,
    reason: str | None,
    change_ref: str | None,
) -> int:
    steps_by_id = get_steps_by_id(manifest)
    if step_id not in steps_by_id:
        print(f"Unknown step: {step_id}", file=sys.stderr)
        return 1
    if not should_require_approval(steps_by_id[step_id], policy):
        print(f"Step {step_id} does not require approval.")
        return 0
    require_reason = bool(policy.get("approval", {}).get("require_reason", False))
    if require_reason and not reason:
        print("Approval reason is required by policy. Use --approval-reason.", file=sys.stderr)
        return 1

    mark_approval_status(paths, step_id, "approved")
    run_id, run_dir = detect_run_dir(paths)
    record = {
        "timestamp": utc_now(),
        "step_id": step_id,
        "approver": approver,
        "reason": reason or "",
        "change_ref": change_ref or "",
        "run_id": run_id or "",
    }
    append_jsonl(paths.approval_records_path, record)
    if run_dir is not None:
        append_text(
            run_dir / "approvals.log",
            f"APPROVED\t{step_id}\tapprover={approver}\treason={reason or ''}\tchange_ref={change_ref or ''}",
        )
        append_jsonl(
            run_dir / "events.jsonl", {"timestamp": utc_now(), "event": "approval", **record}
        )
    print(f"Approved {step_id}")
    return 0


def rollback_step(
    manifest: dict[str, Any], paths: WorkflowPaths, policy: dict[str, Any], step_id: str
) -> int:
    step = get_steps_by_id(manifest).get(step_id)
    if step is None:
        print(f"Unknown step: {step_id}", file=sys.stderr)
        return 1
    if "rollback" not in step:
        print(f"Step {step_id} does not define rollback.")
        return 0
    run_context = (
        setup_run_audit(paths, manifest, policy)
        if audit_enabled(manifest, policy)
        else RunContext(run_id=None, run_dir=None, dry_run=False, metrics={"steps": {}})
    )
    try:
        run_rollback(step, paths, manifest, policy, run_context)
        mark_step_status(paths, step_id, "rolled-back")
        print(f"Rolled back {step_id}")
        return 0
    finally:
        finalize_run_audit(paths, manifest, run_context)


def repair_state(manifest: dict[str, Any], paths: WorkflowPaths) -> int:
    repaired = reconcile_interrupted_steps(manifest, paths)
    step_state, step_errors = read_tsv_state_with_errors(paths.step_state_path)
    approval_state, approval_errors = read_tsv_state_with_errors(paths.approval_state_path)
    _, runtime_errors = read_json_file_with_errors(
        paths.runtime_state_path, {"active_run_id": None, "steps": {}, "updated_at": utc_now()}
    )
    _, metrics_errors = read_json_file_with_errors(
        paths.metrics_path, {"workflow_runs": 0, "steps": {}}
    )
    if step_errors:
        step_state = {step["id"]: "pending" for step in manifest["steps"]}
    if approval_errors:
        approval_state = {
            step["id"]: ("pending" if step.get("requires_approval") else "not-required")
            for step in manifest["steps"]
        }
    if runtime_errors:
        write_runtime_state(paths, {"active_run_id": None, "steps": {}, "updated_at": utc_now()})
    if metrics_errors:
        atomic_write_json(paths.metrics_path, {"workflow_runs": 0, "steps": {}})
    for step_id, status in list(step_state.items()):
        if status == "interrupted":
            step_state[step_id] = "pending"
    write_tsv_state(paths.step_state_path, step_state)
    write_tsv_state(paths.approval_state_path, approval_state)
    repaired_count = (
        len(repaired)
        + len(step_errors)
        + len(approval_errors)
        + len(runtime_errors)
        + len(metrics_errors)
    )
    print(f"Repaired {repaired_count} state issue(s).")
    return 0


def doctor(manifest: dict[str, Any], paths: WorkflowPaths, policy: dict[str, Any]) -> int:
    issues: list[list[str]] = []
    step_state, step_errors = read_tsv_state_with_errors(paths.step_state_path)
    approval_state, approval_errors = read_tsv_state_with_errors(paths.approval_state_path)
    _, runtime_errors = read_json_file_with_errors(
        paths.runtime_state_path, {"active_run_id": None, "steps": {}, "updated_at": utc_now()}
    )
    _, metrics_errors = read_json_file_with_errors(
        paths.metrics_path, {"workflow_runs": 0, "steps": {}}
    )
    for error in step_errors:
        issues.append(["state/step-status.tsv", "corrupt-state", error])
    for error in approval_errors:
        issues.append(["state/approval-status.tsv", "corrupt-state", error])
    for error in runtime_errors:
        issues.append(["state/runtime-state.json", "corrupt-state", error])
    for error in metrics_errors:
        issues.append(["state/metrics.json", "corrupt-state", error])
    for step in manifest["steps"]:
        step_id = step["id"]
        status = step_state.get(step_id, "missing")
        if status == "interrupted":
            issues.append(
                [
                    step_id,
                    "interrupted",
                    "Runner exited before step completion; use --repair and then --resume.",
                ]
            )
        if status == "complete":
            ok, errors = verify_step_contracts(step, paths, paths.log_dir / f"{step_id}.log")
            if not ok:
                issues.append([step_id, "contract-drift", "; ".join(errors)])
        if should_require_approval(step, policy) and approval_state.get(step_id) == "pending":
            issues.append(
                [step_id, "approval-pending", "Approval required before this step can run."]
            )
    if not issues:
        print("Workflow state looks healthy.")
        return 0
    print_table(["step", "issue", "details"], issues)
    return 0


def reset_state(manifest: dict[str, Any], paths: WorkflowPaths) -> int:
    if paths.state_dir.exists():
        shutil.rmtree(paths.state_dir)
    ensure_state(paths, manifest)
    return list_steps(manifest, paths)


def install_triggers(manifest: dict[str, Any], paths: WorkflowPaths) -> int:
    """Delegate to schedule_workflow.py."""
    import importlib.util  # noqa: PLC0415

    schedule_script = Path(__file__).resolve().parent / "schedule_workflow.py"
    spec = importlib.util.spec_from_file_location("schedule_workflow", schedule_script)
    if spec is None or spec.loader is None:
        print("[triggers] Could not load schedule_workflow.py", file=sys.stderr)
        return 1
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.install_triggers(manifest, paths.workflow_dir)


def generate_dashboard(paths: WorkflowPaths) -> int:
    """Delegate to dashboard.py."""
    import importlib.util  # noqa: PLC0415

    dashboard_script = Path(__file__).resolve().parent / "dashboard.py"
    if not dashboard_script.exists():
        print("[dashboard] dashboard.py not found.", file=sys.stderr)
        return 1
    spec = importlib.util.spec_from_file_location("dashboard", dashboard_script)
    if spec is None or spec.loader is None:
        print("[dashboard] Could not load dashboard.py", file=sys.stderr)
        return 1
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.run_dashboard(paths.workflow_dir)


_GENERATE_SYSTEM_PROMPT = """\
You are a workflow architect for the deterministic-workflow-builder skill.
Generate a valid workflow.json manifest (schema_version 4) for the description below.

Required top-level fields:
  schema_version (4), workflow_name (string), version (1), goal (string),
  policy_pack ("strict-prod"), graph ({"execution_model": "dag"}), steps (list).

Required per-step fields:
  id (kebab-case), name (string), type ("shell"), script (string path under steps/),
  success_gate (""), gate_type ("artifact"), requires_approval (false),
  retry_limit (0), timeout_seconds (300), depends_on (list of step ids).

Rules:
- First step has depends_on: []
- Each subsequent step depends_on the previous step's id (or whichever makes logical sense)
- script paths follow the pattern "steps/{id}.sh"
- Use only the shell step type unless a step needs human review (use requires_approval: true)
- Return ONLY a JSON code block, no explanation.
"""


def extract_json_from_claude_output(output: str) -> dict[str, Any]:
    """Extract first ```json...``` block or first bare JSON object from Claude output."""
    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", output)
    if fenced:
        return json.loads(fenced.group(1))
    first_brace = output.find("{")
    last_brace = output.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        return json.loads(output[first_brace : last_brace + 1])
    raise ValueError("No JSON object found in Claude output")


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60]


def _load_skill_registry() -> tuple[list[dict[str, Any]], str]:
    """Load available skills and return (skills_list, formatted_prompt_text)."""
    import importlib.util  # noqa: PLC0415

    discover_script = Path(__file__).resolve().parent / "discover_skills.py"
    if not discover_script.exists():
        return [], ""
    spec = importlib.util.spec_from_file_location("discover_skills", discover_script)
    if spec is None or spec.loader is None:
        return [], ""
    ds = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ds)  # type: ignore[union-attr]
    skills = ds.discover()
    return skills, ds.format_for_prompt(skills)


def discover_skills_command() -> int:
    """Print all available Codex and Claude Code skills."""
    skills, formatted = _load_skill_registry()
    if not skills:
        print("No skills found. Install Codex skills to ~/.codex/skills/ or Claude Code plugins.")
        return 0
    print(f"Available skills ({len(skills)} found):\n")
    for s in skills:
        marker = "✓" if s["has_skill_md"] else "·"
        source = f"[{s['source']}]"
        desc = s["description"][:70] if s["description"] else ""
        print(f"  {marker} {s['name']:45s} {source:8s}  {desc}")
    return 0


def generate_workflow(description: str, output_dir: Path) -> int:
    """Generate a workflow.json from a natural language description using Claude."""
    claude_bin = shutil.which("claude")
    if not claude_bin:
        print("[generate] Error: 'claude' CLI not found. Install Claude Code to use --generate.")
        return 1

    skills, skill_section = _load_skill_registry()
    skill_block = ""
    if skills:
        skill_block = (
            '\n\nAvailable skills (prefer type:"skill" with skill:<name> when a skill covers the task):\n'
            + skill_section
            + "\n\nFor a skill step use:\n"
            '  {"id":"...", "type":"skill", "skill":"<name>", "instruction":"<task>", ...}\n'
        )

    full_prompt = f"{_GENERATE_SYSTEM_PROMPT}{skill_block}\nDescription: {description}"
    print(f"[generate] Calling Claude to design workflow: {description!r}")
    if skills:
        print(f"[generate] {len(skills)} skill(s) available for use as steps")
    try:
        result = subprocess.run(
            [claude_bin, "-p", full_prompt, "--model", "claude-sonnet-4-6"],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        print("[generate] Error: Claude timed out after 120s.")
        return 1

    if result.returncode != 0:
        print(f"[generate] Claude exited {result.returncode}: {result.stderr[:200]}")
        return 1

    try:
        manifest = extract_json_from_claude_output(result.stdout)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"[generate] Failed to parse JSON from Claude output: {exc}")
        print("Claude output was:")
        print(result.stdout[:500])
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "workflow.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    issues = validate_manifest(manifest, manifest_path, output_dir)
    errors = [i for i in issues if i.severity == "error"]
    if errors:
        print(f"[generate] Warning: generated manifest has {len(errors)} validation issue(s):")
        for issue in errors[:5]:
            print(f"  - {issue.message}")

    for subdir in ("steps", "artifacts", "state", "logs", "audit/runs"):
        (output_dir / subdir).mkdir(parents=True, exist_ok=True)

    run_sh = output_dir / "run_workflow.sh"
    runner_path = Path(__file__).resolve()
    run_sh.write_text(
        f'#!/usr/bin/env bash\nexec python3 {runner_path} "$(dirname "$0")" "$@"\n',
        encoding="utf-8",
    )
    run_sh.chmod(0o755)

    steps = manifest.get("steps", [])
    for step in steps:
        script_rel = step.get("script", "")
        if script_rel:
            script_path = output_dir / script_rel
            script_path.parent.mkdir(parents=True, exist_ok=True)
            if not script_path.exists():
                script_path.write_text(
                    f"#!/usr/bin/env bash\n# TODO: implement {step.get('id', 'step')}\necho 'done'\n",
                    encoding="utf-8",
                )
                script_path.chmod(0o755)

    print(f"[generate] Created workflow at {output_dir}/")
    print(f"  workflow.json  ({len(steps)} steps)")
    print("  run_workflow.sh")
    return 0


def _import_n8n_command(n8n_path: Path, output_dir: Path | None) -> int:
    """Delegate to import_n8n.py."""
    import importlib.util  # noqa: PLC0415

    import_script = Path(__file__).resolve().parent / "import_n8n.py"
    spec = importlib.util.spec_from_file_location("import_n8n", import_script)
    if spec is None or spec.loader is None:
        print("[import-n8n] Could not load import_n8n.py", file=sys.stderr)
        return 1
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]

    try:
        n8n_export = json.loads(n8n_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[import-n8n] Failed to read {n8n_path}: {exc}", file=sys.stderr)
        return 1

    manifest, proposals = mod.convert(n8n_export)
    if output_dir is None:
        output_dir = Path(manifest["workflow_name"])

    mod.scaffold(manifest, proposals, output_dir)

    runner_path = Path(__file__).resolve()
    run_sh = output_dir / "run_workflow.sh"
    run_sh.write_text(
        f'#!/usr/bin/env bash\nexec python3 {runner_path} "$(dirname "$0")" "$@"\n',
        encoding="utf-8",
    )
    run_sh.chmod(0o755)

    n_steps = len(manifest["steps"])
    n_triggers = len(manifest.get("triggers", []))
    print(f"[import-n8n] '{n8n_export.get('name')}' → {output_dir}/")
    print(f"  {n_steps} step(s), {n_triggers} trigger(s), {len(proposals)} improvement proposal(s)")
    if proposals:
        print("  Review improvements: ./run_workflow.sh --list-mutations")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Python execution engine for deterministic workflows."
    )
    parser.add_argument(
        "workflow_dir",
        nargs="?",
        default=None,
        help="Workflow directory (positional or use --workflow-dir).",
    )
    parser.add_argument(
        "--workflow-dir",
        default=None,
        dest="workflow_dir_flag",
        help="Workflow directory to operate on.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview execution without running steps."
    )
    parser.add_argument("--list", action="store_true", help="List step status.")
    parser.add_argument("--sidecars", action="store_true", help="List configured sidecars.")
    parser.add_argument("--list-runs", action="store_true", help="List recorded audit runs.")
    parser.add_argument("--replay", default=None, help="Replay a recorded run by id.")
    parser.add_argument(
        "--simulate-run", default=None, help="Replay a run and print the expected step order."
    )
    parser.add_argument("--reset", action="store_true", help="Reset workflow state.")
    parser.add_argument(
        "--resume", action="store_true", help="Resume from the first incomplete step."
    )
    parser.add_argument("--from-step", default=None, help="Run from the specified step onward.")
    parser.add_argument("--step", default=None, help="Run a single step.")
    parser.add_argument("--approve", default=None, help="Approve a waiting step.")
    parser.add_argument(
        "--approver",
        default=os.environ.get("USER", "unknown"),
        help="Approver identity for structured approvals.",
    )
    parser.add_argument("--approval-reason", default=None, help="Structured approval reason.")
    parser.add_argument(
        "--change-ref", default=None, help="Ticket, change request, or rollout reference."
    )
    parser.add_argument("--rollback", default=None, help="Run rollback for the specified step.")
    parser.add_argument("--doctor", action="store_true", help="Diagnose workflow state problems.")
    parser.add_argument(
        "--repair", action="store_true", help="Repair interrupted state so the workflow can resume."
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate workflow-graph.html visualization and exit.",
    )
    parser.add_argument(
        "--list-mutations", action="store_true", help="List pending mutation proposals."
    )
    parser.add_argument("--approve-mutation", default=None, help="Apply a pending mutation by id.")
    parser.add_argument("--reject-mutation", default=None, help="Reject a pending mutation by id.")
    parser.add_argument(
        "--discover-skills",
        action="store_true",
        help="List all available Codex and Claude Code skills.",
    )
    parser.add_argument(
        "--import-n8n",
        default=None,
        metavar="N8N_EXPORT",
        dest="import_n8n",
        help="Import an n8n workflow export JSON and convert it to workflow.json.",
    )
    parser.add_argument(
        "--generate",
        default=None,
        metavar="DESCRIPTION",
        help="Generate a workflow.json from a natural language description.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        dest="output_dir",
        help="Output directory for --generate (defaults to slugified description).",
    )
    parser.add_argument(
        "--install-triggers",
        action="store_true",
        help="Install schedule/webhook triggers defined in workflow.json.",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Generate and open the run history dashboard.",
    )
    parser.add_argument(
        "--improve",
        action="store_true",
        help="Run the autonomous improvement cycle: score and auto-approve low-risk mutations.",
    )
    parser.add_argument(
        "--improve-max-risk",
        default=None,
        choices=["low", "medium", "high"],
        help="Maximum mutation risk level to auto-approve (default: from auto_improve config or 'low').",
    )
    parser.add_argument(
        "--live",
        metavar="PORT",
        type=int,
        nargs="?",
        const=7474,
        default=None,
        help="Start the live dashboard server on PORT (default 7474) while running the workflow.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if args.discover_skills:
        return discover_skills_command()

    if args.import_n8n:
        return _import_n8n_command(
            Path(args.import_n8n), Path(args.output_dir) if args.output_dir else None
        )

    if args.generate:
        out = Path(args.output_dir) if args.output_dir else Path(_slugify(args.generate))
        return generate_workflow(args.generate, out)

    workflow_dir_raw = args.workflow_dir or args.workflow_dir_flag or "."
    workflow_dir = resolve_workflow_dir(Path(workflow_dir_raw))
    paths = build_paths(workflow_dir)
    manifest = load_manifest(paths.manifest_path)
    verify_manifest_or_die(manifest, paths)
    ensure_state(paths, manifest)

    skill_dir = Path(__file__).resolve().parents[1]
    policy_name = manifest.get("policy_pack", "strict-prod")
    policy = deep_merge(load_policy(skill_dir, policy_name), manifest.get("policy", {}))

    with WorkflowLock(paths.lock_path):
        ensure_state(paths, manifest)
        reconcile_interrupted_steps(manifest, paths)
        if args.visualize:
            _write_workflow_viz(paths)
            print(f"[visualize] Wrote {paths.workflow_dir / 'workflow-graph.html'}")
            return 0
        if args.list:
            return list_steps(manifest, paths)
        if args.sidecars:
            return list_sidecars(manifest)
        if args.list_runs:
            return list_runs(paths)
        if args.replay is not None:
            return replay_run(paths, args.replay, simulate=False)
        if args.simulate_run is not None:
            return replay_run(paths, args.simulate_run, simulate=True)
        if args.reset:
            return reset_state(manifest, paths)
        if args.doctor:
            return doctor(manifest, paths, policy)
        if args.repair:
            return repair_state(manifest, paths)
        if args.approve is not None:
            return approve_step(
                manifest,
                paths,
                policy,
                args.approve,
                approver=args.approver,
                reason=args.approval_reason,
                change_ref=args.change_ref,
            )
        if args.rollback is not None:
            return rollback_step(manifest, paths, policy, args.rollback)
        if args.list_mutations:
            return list_mutations(paths)
        if args.approve_mutation is not None:
            return approve_mutation(paths, args.approve_mutation)
        if args.reject_mutation is not None:
            return reject_mutation(paths, args.reject_mutation)
        if args.install_triggers:
            return install_triggers(manifest, paths)
        if args.dashboard:
            return generate_dashboard(paths)
        if args.improve:
            max_risk = args.improve_max_risk or manifest.get("auto_improve", {}).get(
                "max_risk", "low"
            )
            return (
                0 if run_improvement_cycle(paths, manifest, policy, max_risk=max_risk) >= 0 else 1
            )
        if args.step is not None:
            step = get_steps_by_id(manifest).get(args.step)
            if step is None:
                print(f"Unknown step: {args.step}", file=sys.stderr)
                return 1
            current_status = read_tsv_state(paths.step_state_path).get(step["id"])
            if current_status == "complete" and not args.dry_run:
                print(
                    f"Step {step['id']} is already complete. Use --reset first to re-run.",
                    file=sys.stderr,
                )
                return 0
            run_context = (
                setup_run_audit(paths, manifest, policy)
                if audit_enabled(manifest, policy) and not args.dry_run
                else RunContext(
                    run_id=None, run_dir=None, dry_run=args.dry_run, metrics={"steps": {}}
                )
            )
            try:
                if args.dry_run:
                    print(f"WOULD RUN\t{step['id']}\t{step['type']}\t{step.get('script', '-')}")
                    return 0
                if (
                    should_require_approval(step, policy)
                    and read_tsv_state(paths.approval_state_path).get(step["id"]) != "approved"
                ):
                    mark_step_status(paths, step["id"], "waiting-approval")
                    print(
                        f"Approval required before {step['id']}. Run ./run_workflow.sh --approve {step['id']}",
                        file=sys.stderr,
                    )
                    return 3
                result = execute_single_step(manifest, step, paths, policy, run_context)
                record_metrics(paths, run_context, result)
                return result.returncode
            finally:
                finalize_run_audit(paths, manifest, run_context)
        if args.from_step is not None:
            return run_many(
                manifest, paths, policy, start_step=args.from_step, dry_run=args.dry_run
            )
        # --live: start dashboard server in a daemon thread before running.
        if args.live is not None and not args.dry_run:
            import importlib.util as _ilu  # noqa: PLC0415
            import threading as _threading  # noqa: PLC0415

            _spec = _ilu.spec_from_file_location(
                "live_dashboard", Path(__file__).parent / "live_dashboard.py"
            )
            if _spec and _spec.loader:
                _ld = _ilu.module_from_spec(_spec)
                _spec.loader.exec_module(_ld)  # type: ignore[union-attr]
                _t = _threading.Thread(
                    target=_ld.serve_live, args=(workflow_dir, args.live), daemon=True
                )
                _t.start()

        def _run_and_improve(start_step=None, dry_run_flag=False):
            rc = run_many(manifest, paths, policy, start_step=start_step, dry_run=dry_run_flag)
            # Post-run autonomous improvement cycle.
            ai_cfg = manifest.get("auto_improve", {})
            if ai_cfg.get("enabled") and not dry_run_flag:
                max_risk = args.improve_max_risk or ai_cfg.get("max_risk", "low")
                run_improvement_cycle(paths, manifest, policy, max_risk=max_risk)
            return rc

        if args.resume:
            start_step = first_incomplete_step(manifest, paths)
            if start_step is None:
                print("All steps are already complete.")
                return 0
            return _run_and_improve(start_step=start_step, dry_run_flag=args.dry_run)
        if args.dry_run:
            return run_many(manifest, paths, policy, dry_run=True)
        return _run_and_improve()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

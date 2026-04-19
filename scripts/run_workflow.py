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


def run_command_step(
    step: dict[str, Any],
    paths: WorkflowPaths,
    manifest: dict[str, Any],
    policy: dict[str, Any],
    log_path: Path,
) -> tuple[int, str]:
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
    max_parallel = max(1, int(policy.get("execution", {}).get("max_parallel", 1)))

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
    pending = [step_id for step_id in order if step_state.get(step_id) != "complete"]
    completed = {step_id for step_id, status in step_state.items() if status == "complete"}
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
    for step_id in simulate_step_order(manifest):
        if step_state.get(step_id) != "complete":
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
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
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
        if args.resume:
            start_step = first_incomplete_step(manifest, paths)
            if start_step is None:
                print("All steps are already complete.")
                return 0
            return run_many(manifest, paths, policy, start_step=start_step, dry_run=args.dry_run)
        if args.dry_run:
            return run_many(manifest, paths, policy, dry_run=True)
        return run_many(manifest, paths, policy)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

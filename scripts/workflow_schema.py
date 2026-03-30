#!/usr/bin/env python3
"""Shared schema and validation helpers for deterministic workflows."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 4
SUPPORTED_SCHEMA_VERSIONS = {2, 3, 4}
STEP_REQUIRED_FIELDS = (
    "id",
    "name",
    "type",
    "success_gate",
    "requires_approval",
    "retry_limit",
)
SIDECAR_REQUIRED_FIELDS = (
    "id",
    "name",
    "purpose",
    "when",
    "kind",
    "containment",
    "consumer_step",
)
VALID_STEP_TYPES = {
    "shell",
    "test",
    "approval",
    "transform",
    "publish",
    "sidecar-consume",
    "json-validate",
    "file-exists",
    "python",
    "copy",
    "http-check",
    "git-diff-check",
    "manual-approval",
}
VALID_FAILURE_POLICIES = {"stop", "continue"}
VALID_SIDECAR_KINDS = {"prompt", "skill"}
VALID_GRAPH_EXECUTION_MODELS = {"sequence", "dag"}
VALID_GATE_TYPES = {"artifact", "approval", "review", "test", "http", "json", "git"}
SCRIPTED_STEP_TYPES = {"shell", "test", "transform", "publish", "sidecar-consume", "approval"}
VALID_CONTRACT_TYPES = {"file", "json", "report"}
VALID_VALIDATION_TYPES = {"file_exists", "path_absent", "json_required_keys", "log_contains", "command"}
DEFAULT_ALLOWLISTED_COMMANDS = [
    "bash", "cat", "cp", "echo", "find", "git", "grep", "jq",
    "mkdir", "mv", "python3", "rm", "sed", "sleep", "sort", "touch", "xargs",
]


@dataclass
class Issue:
    severity: str
    message: str
    path: str
    line: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_workflow_dir(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.is_dir():
        return resolved
    return resolved.parent


def _add_issue(issues: list[Issue], severity: str, path: Path, message: str, line: int | None = None) -> None:
    issues.append(Issue(severity=severity, message=message, path=str(path), line=line))


def _expect_type(
    issues: list[Issue],
    path: Path,
    container: dict[str, Any],
    field: str,
    expected: type,
    *,
    allow_empty: bool = True,
) -> Any:
    value = container.get(field)
    if not isinstance(value, expected) or (expected is int and isinstance(value, bool)):
        _add_issue(issues, "error", path, f"`{field}` must be of type {expected.__name__}.")
        return None
    if not allow_empty and not value:
        _add_issue(issues, "error", path, f"`{field}` must not be empty.")
    return value


def normalize_contract(entry: Any) -> dict[str, Any] | None:
    if isinstance(entry, str):
        return {"type": "file", "path": entry, "required": True}
    if isinstance(entry, dict):
        normalized = dict(entry)
        normalized.setdefault("type", "file")
        normalized.setdefault("required", True)
        return normalized
    return None


def _validate_contract_entry(
    issues: list[Issue],
    manifest_path: Path,
    workflow_dir: Path | None,
    step_id: str,
    field_name: str,
    entry: Any,
) -> None:
    contract = normalize_contract(entry)
    if contract is None:
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `{field_name}` must contain strings or objects.")
        return
    contract_type = contract.get("type")
    if contract_type not in VALID_CONTRACT_TYPES:
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` has invalid contract type `{contract_type}` in `{field_name}`.")
    path_value = contract.get("path")
    if not isinstance(path_value, str) or not path_value:
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract in `{field_name}` must define non-empty `path`.")
        return
    contract_path = Path(path_value)
    if contract_path.is_absolute():
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` must be relative.")
    if ".." in contract_path.parts:
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` must not contain `..`.")
    if workflow_dir is not None and contract.get("required", True):
        resolved = (workflow_dir / path_value).resolve()
        if workflow_dir.resolve() not in (resolved, *resolved.parents):
            _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` escapes the workflow directory.")
    if "sha256" in contract and (not isinstance(contract["sha256"], str) or len(contract["sha256"]) != 64):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` has invalid `sha256`.")
    for size_field in ("min_size_bytes", "max_size_bytes"):
        if size_field in contract and (not isinstance(contract[size_field], int) or isinstance(contract[size_field], bool) or contract[size_field] < 0):
            _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` has invalid `{size_field}`.")
    if "retention" in contract and not isinstance(contract["retention"], dict):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` contract path `{path_value}` must use object `retention` metadata.")
    if contract_type == "json" and "schema" in contract and not isinstance(contract["schema"], dict):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` JSON contract `{path_value}` must use object `schema`.")


def _validate_validation_check(
    issues: list[Issue],
    manifest_path: Path,
    step_id: str,
    entry: Any,
) -> None:
    if isinstance(entry, str):
        return
    if not isinstance(entry, dict):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` validation checks must be strings or objects.")
        return
    check_type = entry.get("type")
    if check_type not in VALID_VALIDATION_TYPES:
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` has invalid validation check type `{check_type}`.")
        return
    if check_type in {"file_exists", "path_absent", "json_required_keys"}:
        if not isinstance(entry.get("path"), str) or not entry["path"]:
            _add_issue(issues, "error", manifest_path, f"Step `{step_id}` validation check `{check_type}` must define `path`.")
    if check_type == "json_required_keys":
        keys = entry.get("required_keys")
        if not isinstance(keys, list) or not all(isinstance(item, str) and item for item in keys):
            _add_issue(issues, "error", manifest_path, f"Step `{step_id}` json_required_keys must define string `required_keys`.")
    if check_type == "log_contains" and (not isinstance(entry.get("value"), str) or not entry["value"]):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` log_contains check must define non-empty `value`.")
    if check_type == "command" and (not isinstance(entry.get("command"), str) or not entry["command"]):
        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` command validation must define non-empty `command`.")


def validate_manifest(manifest: dict[str, Any], manifest_path: Path, workflow_dir: Path | None = None) -> list[Issue]:
    issues: list[Issue] = []
    if not isinstance(manifest, dict):
        _add_issue(issues, "error", manifest_path, "Manifest root must be an object.")
        return issues

    version = _expect_type(issues, manifest_path, manifest, "schema_version", int)
    if isinstance(version, int) and version not in SUPPORTED_SCHEMA_VERSIONS:
        _add_issue(
            issues,
            "error",
            manifest_path,
            f"`schema_version` must be one of {sorted(SUPPORTED_SCHEMA_VERSIONS)}, got {version}.",
        )

    _expect_type(issues, manifest_path, manifest, "workflow_name", str, allow_empty=False)
    _expect_type(issues, manifest_path, manifest, "version", int)
    _expect_type(issues, manifest_path, manifest, "working_directory", str, allow_empty=False)
    policy_override = manifest.get("policy", {})
    if policy_override is not None and not isinstance(policy_override, dict):
        _add_issue(issues, "error", manifest_path, "`policy` must be an object when provided.")

    if version in {3, 4}:
        _expect_type(issues, manifest_path, manifest, "goal", str, allow_empty=False)
        _expect_type(issues, manifest_path, manifest, "policy_pack", str, allow_empty=False)
        _expect_type(issues, manifest_path, manifest, "inputs", list)
        _expect_type(issues, manifest_path, manifest, "outputs", list)
        graph = _expect_type(issues, manifest_path, manifest, "graph", dict)
        if isinstance(graph, dict):
            execution_model = graph.get("execution_model")
            if execution_model not in VALID_GRAPH_EXECUTION_MODELS:
                _add_issue(
                    issues,
                    "error",
                    manifest_path,
                    f"`graph.execution_model` must be one of {sorted(VALID_GRAPH_EXECUTION_MODELS)}.",
                )
        if version == 4:
            environment = _expect_type(issues, manifest_path, manifest, "environment", dict)
            if isinstance(environment, dict):
                network_mode = environment.get("network_mode")
                if not isinstance(network_mode, str) or not network_mode:
                    _add_issue(issues, "error", manifest_path, "`environment.network_mode` must be a non-empty string.")
            tooling = _expect_type(issues, manifest_path, manifest, "tooling", dict)
            if isinstance(tooling, dict):
                if "allowlisted_commands" in tooling and not isinstance(tooling["allowlisted_commands"], list):
                    _add_issue(issues, "error", manifest_path, "`tooling.allowlisted_commands` must be a list.")
            migrations = _expect_type(issues, manifest_path, manifest, "migrations", dict)
            if isinstance(migrations, dict):
                current_from = migrations.get("current_from")
                if current_from is not None and (not isinstance(current_from, int) or isinstance(current_from, bool)):
                    _add_issue(issues, "error", manifest_path, "`migrations.current_from` must be an integer when provided.")

    failure_policy = _expect_type(issues, manifest_path, manifest, "failure_policy", dict)
    if isinstance(failure_policy, dict):
        on_error = failure_policy.get("on_error")
        if on_error not in VALID_FAILURE_POLICIES:
            _add_issue(
                issues,
                "error",
                manifest_path,
                f"`failure_policy.on_error` must be one of {sorted(VALID_FAILURE_POLICIES)}.",
            )
        retries = failure_policy.get("max_retries")
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            _add_issue(issues, "error", manifest_path, "`failure_policy.max_retries` must be a non-negative integer.")

    audit = _expect_type(issues, manifest_path, manifest, "audit", dict)
    if isinstance(audit, dict):
        enabled = audit.get("enabled")
        if not isinstance(enabled, bool):
            _add_issue(issues, "error", manifest_path, "`audit.enabled` must be a boolean.")
        directory = audit.get("directory")
        if not isinstance(directory, str) or not directory:
            _add_issue(issues, "error", manifest_path, "`audit.directory` must be a non-empty string.")

    residual = _expect_type(issues, manifest_path, manifest, "residual_nondeterminism", list, allow_empty=False)
    if isinstance(residual, list):
        for item in residual:
            if not isinstance(item, str) or not item.strip():
                _add_issue(issues, "error", manifest_path, "`residual_nondeterminism` entries must be non-empty strings.")

    steps = _expect_type(issues, manifest_path, manifest, "steps", list, allow_empty=False)
    sidecars = manifest.get("sidecars", [])
    if sidecars is None:
        sidecars = []
    if not isinstance(sidecars, list):
        _add_issue(issues, "error", manifest_path, "`sidecars` must be a list.")
        sidecars = []

    step_ids: set[str] = set()
    if isinstance(steps, list):
        for index, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                _add_issue(issues, "error", manifest_path, f"Step #{index} must be an object.")
                continue
            for field in STEP_REQUIRED_FIELDS:
                if field not in step:
                    _add_issue(issues, "error", manifest_path, f"Step #{index} is missing `{field}`.")
            step_id = step.get("id")
            if isinstance(step_id, str):
                if step_id in step_ids:
                    _add_issue(issues, "error", manifest_path, f"Duplicate step id `{step_id}`.")
                step_ids.add(step_id)
            else:
                _add_issue(issues, "error", manifest_path, f"Step #{index} must have a string `id`.")
            step_type = step.get("type")
            if step_type not in VALID_STEP_TYPES:
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` has invalid `type` `{step_type}`.")
            if not isinstance(step.get("requires_approval"), bool):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` must have boolean `requires_approval`.")
            retry_limit = step.get("retry_limit")
            if not isinstance(retry_limit, int) or isinstance(retry_limit, bool) or retry_limit < 0:
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` must have non-negative integer `retry_limit`.")
            gate_type = step.get("gate_type")
            if gate_type not in VALID_GATE_TYPES:
                _add_issue(
                    issues,
                    "error",
                    manifest_path,
                    f"Step `{step_id}` has invalid `gate_type` `{gate_type}`.",
                )
            timeout_seconds = step.get("timeout_seconds")
            if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
                _add_issue(
                    issues,
                    "error",
                    manifest_path,
                    f"Step `{step_id}` must define positive integer `timeout_seconds`.",
                )
            depends_on = step.get("depends_on", [])
            if not isinstance(depends_on, list):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `depends_on` must be a list.")
            if step_type in SCRIPTED_STEP_TYPES:
                script = step.get("script")
                if not isinstance(script, str) or not script:
                    _add_issue(issues, "error", manifest_path, f"Step `{step_id}` must define non-empty `script`.")
                elif workflow_dir is not None:
                    script_path = workflow_dir / script
                    if not script_path.exists():
                        _add_issue(issues, "error", script_path, f"Step script referenced by `{step_id}` does not exist.")
            elif "script" in step and step.get("script") not in ("", None):
                script = step.get("script")
                if not isinstance(script, str):
                    _add_issue(issues, "error", manifest_path, f"Step `{step_id}` script must be a string when provided.")

            produces = step.get("produces", [])
            consumes = step.get("consumes", [])
            commands = step.get("commands", [])
            validation_checks = step.get("validation_checks", [])
            if not isinstance(produces, list):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `produces` must be a list.")
            else:
                for entry in produces:
                    _validate_contract_entry(issues, manifest_path, workflow_dir, step_id, "produces", entry)
            if not isinstance(consumes, list):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `consumes` must be a list.")
            else:
                for entry in consumes:
                    _validate_contract_entry(issues, manifest_path, workflow_dir, step_id, "consumes", entry)
            if not isinstance(commands, list):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `commands` must be a list.")
            if not isinstance(validation_checks, list):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` field `validation_checks` must be a list.")
            else:
                for entry in validation_checks:
                    _validate_validation_check(issues, manifest_path, step_id, entry)
            if "executor_config" in step and not isinstance(step["executor_config"], dict):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` must use object `executor_config`.")
            if "rollback" in step:
                rollback = step["rollback"]
                if not isinstance(rollback, dict):
                    _add_issue(issues, "error", manifest_path, f"Step `{step_id}` rollback must be an object.")
                else:
                    script = rollback.get("script")
                    if not isinstance(script, str) or not script:
                        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` rollback must define non-empty `script`.")
                    elif workflow_dir is not None and not (workflow_dir / script).exists():
                        _add_issue(issues, "error", workflow_dir / script, f"Rollback script for `{step_id}` does not exist.")
                    when = rollback.get("when", "manual")
                    if when not in {"manual", "on_failure"}:
                        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` rollback.when must be `manual` or `on_failure`.")
                    preconditions = rollback.get("preconditions", [])
                    if not isinstance(preconditions, list):
                        _add_issue(issues, "error", manifest_path, f"Step `{step_id}` rollback.preconditions must be a list.")
            if "working_directory" in step and not isinstance(step["working_directory"], str):
                _add_issue(issues, "error", manifest_path, f"Step `{step_id}` working_directory must be a string.")

    if version in {3, 4} and isinstance(steps, list):
        step_map = {step["id"]: step for step in steps if isinstance(step, dict) and isinstance(step.get("id"), str)}
        for step_id, step in step_map.items():
            for dependency in step.get("depends_on", []):
                if dependency not in step_map:
                    _add_issue(issues, "error", manifest_path, f"Step `{step_id}` depends on unknown step `{dependency}`.")
        graph = manifest.get("graph", {})
        if isinstance(graph, dict) and graph.get("execution_model") == "dag":
            ordered = _topological_step_order(manifest)
            if len(ordered) != len(step_map):
                _add_issue(issues, "error", manifest_path, "Workflow DAG contains a cycle or unreachable dependency chain.")

        all_produced: dict[str, str] = {}
        for step_id, step in step_map.items():
            for entry in step.get("produces", []):
                contract = normalize_contract(entry)
                if contract and isinstance(contract.get("path"), str):
                    all_produced[contract["path"]] = step_id
        for step_id, step in step_map.items():
            for entry in step.get("consumes", []):
                contract = normalize_contract(entry)
                if contract and isinstance(contract.get("path"), str) and contract.get("required", True):
                    consumed_path = contract["path"]
                    if consumed_path not in all_produced:
                        _add_issue(issues, "warning", manifest_path, f"Step `{step_id}` consumes `{consumed_path}` which is not produced by any step.")

    sidecar_ids: set[str] = set()
    for index, sidecar in enumerate(sidecars, start=1):
        if not isinstance(sidecar, dict):
            _add_issue(issues, "error", manifest_path, f"Sidecar #{index} must be an object.")
            continue
        for field in SIDECAR_REQUIRED_FIELDS:
            if field not in sidecar:
                _add_issue(issues, "error", manifest_path, f"Sidecar #{index} is missing `{field}`.")
        sidecar_id = sidecar.get("id")
        if isinstance(sidecar_id, str):
            if sidecar_id in sidecar_ids:
                _add_issue(issues, "error", manifest_path, f"Duplicate sidecar id `{sidecar_id}`.")
            sidecar_ids.add(sidecar_id)
        else:
            _add_issue(issues, "error", manifest_path, f"Sidecar #{index} must have a string `id`.")
        kind = sidecar.get("kind")
        if kind not in VALID_SIDECAR_KINDS:
            _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` has invalid `kind` `{kind}`.")
        consumer_step = sidecar.get("consumer_step")
        if isinstance(consumer_step, str) and consumer_step not in step_ids:
            _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` references unknown `consumer_step` `{consumer_step}`.")
        containment = sidecar.get("containment")
        if not isinstance(containment, dict):
            _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` must contain a `containment` object.")
        else:
            for field in ("mode", "enforced_by", "notes"):
                if field not in containment or not isinstance(containment.get(field), str) or not containment.get(field):
                    _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` containment must include non-empty `{field}`.")
        if kind == "prompt":
            prompt_asset = sidecar.get("prompt_asset")
            if not isinstance(prompt_asset, str) or not prompt_asset:
                _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` must define `prompt_asset`.")
            elif workflow_dir is not None and not (workflow_dir / prompt_asset).exists():
                _add_issue(issues, "error", workflow_dir / prompt_asset, f"Prompt asset for sidecar `{sidecar_id}` does not exist.")
            prompt_sha256 = sidecar.get("prompt_sha256")
            if prompt_sha256 is not None and (not isinstance(prompt_sha256, str) or len(prompt_sha256) != 64):
                _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` has invalid `prompt_sha256`.")
            elif workflow_dir is not None and isinstance(prompt_asset, str) and isinstance(prompt_sha256, str):
                actual_sha256 = __import__("hashlib").sha256((workflow_dir / prompt_asset).read_bytes()).hexdigest()
                if actual_sha256 != prompt_sha256:
                    _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` prompt digest does not match `prompt_sha256`.")
        if kind == "skill":
            skill_path = sidecar.get("skill_path")
            if not isinstance(skill_path, str) or not skill_path:
                _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` must define `skill_path`.")
        output_schema = sidecar.get("output_schema")
        if output_schema is None or not isinstance(output_schema, dict):
            _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` must define object `output_schema`.")
        validator = sidecar.get("validator")
        if not isinstance(validator, str) or not validator:
            _add_issue(issues, "error", manifest_path, f"Sidecar `{sidecar_id}` must define non-empty `validator`.")

    return issues


def _topological_step_order(manifest: dict[str, Any]) -> list[str]:
    steps = [step for step in manifest.get("steps", []) if isinstance(step, dict) and "id" in step]
    if manifest.get("schema_version", 2) < 3:
        return [step["id"] for step in steps]

    graph = manifest.get("graph", {})
    if graph.get("execution_model") != "dag":
        return [step["id"] for step in steps]

    indegree: dict[str, int] = {}
    edges: dict[str, list[str]] = {}
    order_hint = {step["id"]: index for index, step in enumerate(steps)}
    for step in steps:
        step_id = step["id"]
        depends_on = [dep for dep in step.get("depends_on", []) if isinstance(dep, str)]
        indegree[step_id] = len(depends_on)
        for dependency in depends_on:
            edges.setdefault(dependency, []).append(step_id)

    ready = sorted([step_id for step_id, degree in indegree.items() if degree == 0], key=lambda item: order_hint[item])
    result: list[str] = []
    queue = deque(ready)
    while queue:
        current = queue.popleft()
        result.append(current)
        for neighbour in sorted(edges.get(current, []), key=lambda item: order_hint[item]):
            indegree[neighbour] -= 1
            if indegree[neighbour] == 0:
                queue.append(neighbour)
    return result


def simulate_step_order(manifest: dict[str, Any]) -> list[str]:
    steps = [step for step in manifest.get("steps", []) if isinstance(step, dict) and "id" in step]
    ordered = _topological_step_order(manifest)
    if len(ordered) != len(steps):
        return [step["id"] for step in steps]
    return ordered


def summarize_sidecars(manifest: dict[str, Any]) -> list[dict[str, str]]:
    summary: list[dict[str, str]] = []
    for sidecar in manifest.get("sidecars", []):
        if not isinstance(sidecar, dict):
            continue
        summary.append(
            {
                "id": str(sidecar.get("id", "")),
                "when": str(sidecar.get("when", "")),
                "consumer_step": str(sidecar.get("consumer_step", "")),
                "kind": str(sidecar.get("kind", "")),
                "validator": str(sidecar.get("validator", "")),
            }
        )
    return summary

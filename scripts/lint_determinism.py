#!/usr/bin/env python3
"""Lint deterministic workflow packages for structural and heuristic issues."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from workflow_schema import load_manifest as load_manifest_data
from workflow_schema import validate_manifest

SUBJECTIVE_PATTERNS = [
    re.compile(r"\binspect and decide\b", re.IGNORECASE),
    re.compile(r"\blooks? good\b", re.IGNORECASE),
    re.compile(r"\bprobably\b", re.IGNORECASE),
    re.compile(r"\bintuition\b", re.IGNORECASE),
    re.compile(r"\bjudge\b", re.IGNORECASE),
    re.compile(r"\beyeball\b", re.IGNORECASE),
]

NONDETERMINISTIC_PATTERNS = [
    re.compile(r"\bclaude\b", re.IGNORECASE),
    re.compile(r"\bgpt\b", re.IGNORECASE),
    re.compile(r"\bopenai\b", re.IGNORECASE),
    re.compile(r"\bllm\b", re.IGNORECASE),
    re.compile(r"\bshuf\b"),
    re.compile(r"\buuidgen\b"),
    re.compile(r"\$RANDOM\b"),
    re.compile(r"\bdate\b"),
]

UNSORTED_TRAVERSAL_PATTERNS = [
    re.compile(r"\bfind\b"),
    re.compile(r"\bls\b"),
]


@dataclass
class Finding:
    severity: str
    file: str
    message: str
    line: int | None = None


def add_finding(
    findings: list[Finding], severity: str, path: Path, message: str, line: int | None = None
) -> None:
    findings.append(Finding(severity=severity, file=str(path), message=message, line=line))


def resolve_workflow_dir(input_path: Path) -> Path:
    path = input_path.resolve()
    if path.is_dir():
        return path
    return path.parent


def load_workflow_manifest(workflow_dir: Path, findings: list[Finding]) -> dict | None:
    manifest_path = workflow_dir / "workflow.json"
    if not manifest_path.exists():
        add_finding(findings, "error", manifest_path, "Missing workflow.json manifest.")
        return None

    try:
        data = load_manifest_data(manifest_path)
    except json.JSONDecodeError as exc:
        add_finding(findings, "error", manifest_path, f"Invalid JSON: {exc}")
        return None

    return data


def scan_todos(path: Path, findings: list[Finding]) -> None:
    if not path.exists():
        add_finding(findings, "error", path, "Required file is missing.")
        return

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if "TODO" in line:
            add_finding(findings, "error", path, "Unresolved TODO marker.", line_number)


def scan_step_script(path: Path, findings: list[Finding]) -> None:
    if not path.exists():
        add_finding(findings, "error", path, "Step script is missing.")
        return

    lines = path.read_text(encoding="utf-8").splitlines()
    text = "\n".join(lines)

    if "set -euo pipefail" not in text:
        add_finding(findings, "error", path, "Shell step is missing `set -euo pipefail`.")

    for line_number, line in enumerate(lines, start=1):
        for pattern in SUBJECTIVE_PATTERNS:
            if pattern.search(line):
                add_finding(
                    findings,
                    "error",
                    path,
                    "Subjective runtime wording is not deterministic.",
                    line_number,
                )
        for pattern in NONDETERMINISTIC_PATTERNS:
            if pattern.search(line):
                add_finding(
                    findings,
                    "warning",
                    path,
                    "Potential nondeterministic runtime dependency detected.",
                    line_number,
                )
        if (
            any(pattern.search(line) for pattern in UNSORTED_TRAVERSAL_PATTERNS)
            and "sort" not in line
        ):
            add_finding(
                findings, "warning", path, "Filesystem traversal appears unsorted.", line_number
            )


def lint_workflow(workflow_dir: Path) -> list[Finding]:
    findings: list[Finding] = []
    spec_path = workflow_dir / "WORKFLOW_SPEC.md"
    manifest_path = workflow_dir / "workflow.json"
    runner_path = workflow_dir / "run_workflow.sh"

    scan_todos(spec_path, findings)
    scan_todos(manifest_path, findings)

    if not runner_path.exists():
        add_finding(findings, "error", runner_path, "Missing run_workflow.sh runner.")

    manifest = load_workflow_manifest(workflow_dir, findings)
    if manifest is None:
        return findings

    for issue in validate_manifest(manifest, manifest_path, workflow_dir=workflow_dir):
        add_finding(findings, issue.severity, Path(issue.path), issue.message, issue.line)

    residual = manifest.get("residual_nondeterminism")
    if not isinstance(residual, list) or not residual:
        add_finding(
            findings,
            "error",
            manifest_path,
            "Manifest must define residual_nondeterminism as a non-empty list.",
        )

    for step in manifest["steps"]:
        step_path = workflow_dir / step["script"]
        if step.get("success_gate") == "TODO":
            add_finding(
                findings,
                "error",
                manifest_path,
                f"Step `{step['id']}` still has a TODO success gate.",
            )
        scan_step_script(step_path, findings)

    for sidecar in manifest.get("sidecars", []):
        if not isinstance(sidecar, dict):
            continue
        containment = sidecar.get("containment", {})
        notes = containment.get("notes", "") if isinstance(containment, dict) else ""
        if isinstance(sidecar.get("when"), str) and not sidecar["when"].strip():
            add_finding(
                findings,
                "error",
                manifest_path,
                f"Sidecar `{sidecar.get('id', '?')}` must describe `when`.",
            )
        if (
            "pass" not in notes.lower()
            and "approval" not in notes.lower()
            and "proposal" not in notes.lower()
        ):
            add_finding(
                findings,
                "warning",
                manifest_path,
                f"Sidecar `{sidecar.get('id', '?')}` containment notes should explain how outputs stay advisory.",
            )

    return findings


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lint a deterministic workflow directory.")
    parser.add_argument("path", help="Workflow directory or a file inside the workflow.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero when warnings are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workflow_dir = resolve_workflow_dir(Path(args.path))
    findings = lint_workflow(workflow_dir)

    errors = [finding for finding in findings if finding.severity == "error"]
    warnings = [finding for finding in findings if finding.severity == "warning"]

    if args.json:
        print(json.dumps([asdict(finding) for finding in findings], indent=2))
    else:
        if not findings:
            print(f"[OK] {workflow_dir} passed deterministic lint.")
        else:
            for finding in findings:
                location = f"{finding.file}:{finding.line}" if finding.line else finding.file
                print(f"[{finding.severity.upper()}] {location} - {finding.message}")
            print(f"[SUMMARY] errors={len(errors)} warnings={len(warnings)}")

    if errors:
        return 1
    if args.strict and warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Security-oriented audit for deterministic workflow packages."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from workflow_schema import Issue, load_manifest, resolve_workflow_dir, validate_manifest


DANGEROUS_PATTERNS = (
    (re.compile(r"curl\b.*\|\s*(bash|sh)"), "Remote script execution via curl pipe."),
    (re.compile(r"wget\b.*\|\s*(bash|sh)"), "Remote script execution via wget pipe."),
    (re.compile(r"\beval\b"), "Use of eval."),
    (re.compile(r"rm\s+-rf\s+/"), "Dangerous root delete."),
    (re.compile(r"chmod\s+777\b"), "Overly permissive chmod."),
)


def collect_script_findings(workflow_dir: Path) -> list[Issue]:
    issues: list[Issue] = []
    for script_path in sorted((workflow_dir / "steps").glob("*.sh")):
        text = script_path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            for pattern, message in DANGEROUS_PATTERNS:
                if pattern.search(line):
                    issues.append(Issue(severity="warning", message=message, path=str(script_path), line=line_no))
    return issues


def collect_manifest_findings(manifest: dict[str, object], workflow_dir: Path) -> list[Issue]:
    issues: list[Issue] = []
    environment = manifest.get("environment", {})
    if isinstance(environment, dict):
        allowed_env = environment.get("allowed_env", [])
        if isinstance(allowed_env, list) and "*" in allowed_env:
            issues.append(Issue(severity="warning", message="Wildcard allowed_env entry `*` weakens isolation.", path=str(workflow_dir / "workflow.json")))
    tooling = manifest.get("tooling", {})
    if isinstance(tooling, dict):
        allowlisted_commands = tooling.get("allowlisted_commands", [])
        if isinstance(allowlisted_commands, list) and not allowlisted_commands:
            issues.append(Issue(severity="warning", message="Empty command allowlist means shell steps are not command-restricted.", path=str(workflow_dir / "workflow.json")))
    return issues


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Security audit a deterministic workflow package.")
    parser.add_argument("path", help="Workflow directory or a file inside it.")
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workflow_dir = resolve_workflow_dir(Path(args.path))
    manifest_path = workflow_dir / "workflow.json"
    manifest = load_manifest(manifest_path)
    issues = validate_manifest(manifest, manifest_path, workflow_dir=workflow_dir)
    issues.extend(collect_manifest_findings(manifest, workflow_dir))
    issues.extend(collect_script_findings(workflow_dir))

    if args.json:
        print(json.dumps([issue.to_dict() for issue in issues], indent=2))
    else:
        if not issues:
            print(f"[OK] {workflow_dir} passed security audit.")
        for issue in issues:
            location = f"{issue.path}:{issue.line}" if issue.line else issue.path
            print(f"[{issue.severity.upper()}] {location} - {issue.message}")
    return 1 if any(issue.severity == "error" for issue in issues) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

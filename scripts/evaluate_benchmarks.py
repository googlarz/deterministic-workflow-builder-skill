#!/usr/bin/env python3
"""Run the golden benchmark harness for deterministic workflow compilation."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from compile_workflow import compile_workflow
from workflow_schema import load_manifest, validate_manifest


SKILL_DIR = Path(__file__).resolve().parents[1]
BENCHMARK_DIR = SKILL_DIR / "benchmarks"


def score_benchmark(benchmark_path: Path) -> dict[str, object]:
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as temp_dir:
        workflow_dir, kind = compile_workflow(benchmark["request"], Path(temp_dir))
        manifest = load_manifest(workflow_dir / "workflow.json")
        issues = validate_manifest(manifest, workflow_dir / "workflow.json", workflow_dir=workflow_dir)
        step_ids = [step["id"] for step in manifest["steps"]]
        sidecar_ids = [sidecar["id"] for sidecar in manifest["sidecars"]]
        errors = [issue for issue in issues if issue.severity == "error"]
        warnings = [issue for issue in issues if issue.severity == "warning"]
        score = 0
        score += 20 if kind == benchmark["expected_kind"] else 0
        score += 20 if step_ids == benchmark["expected_steps"] else 0
        score += 20 if sidecar_ids == benchmark["expected_sidecars"] else 0
        score += 20 if manifest.get("policy_pack") == benchmark.get("expected_policy_pack") else 0
        score += max(0, 20 - len(errors) * 8 - len(warnings) * 2)
        return {
            "benchmark": benchmark_path.name,
            "score": score,
            "kind": kind,
            "expected_kind": benchmark["expected_kind"],
            "policy_pack": manifest.get("policy_pack"),
            "expected_policy_pack": benchmark.get("expected_policy_pack"),
            "step_ids": step_ids,
            "expected_steps": benchmark["expected_steps"],
            "sidecar_ids": sidecar_ids,
            "expected_sidecars": benchmark["expected_sidecars"],
            "errors": len(errors),
            "warnings": len(warnings),
        }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate compiler performance on golden benchmarks.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    results = [score_benchmark(path) for path in sorted(BENCHMARK_DIR.glob("*.json"))]
    total = sum(result["score"] for result in results)
    if args.json:
        print(json.dumps({"results": results, "total_score": total}, indent=2))
    else:
        for result in results:
            print(
                f"{result['benchmark']}: score={result['score']} "
                f"kind={result['kind']} errors={result['errors']} warnings={result['warnings']}"
            )
        print(f"TOTAL {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

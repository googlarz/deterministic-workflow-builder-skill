from __future__ import annotations

import json
import os
import random
import subprocess
import tempfile
import time
import unittest
import zipfile
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
INIT_SCRIPT = SKILL_DIR / "scripts" / "init_deterministic_workflow.py"
LINT_SCRIPT = SKILL_DIR / "scripts" / "lint_determinism.py"
VERIFY_SCRIPT = SKILL_DIR / "scripts" / "verify_workflow.py"
COMPILE_SCRIPT = SKILL_DIR / "scripts" / "compile_workflow.py"
HARDEN_SCRIPT = SKILL_DIR / "scripts" / "auto_harden_workflow.py"
DIFF_SCRIPT = SKILL_DIR / "scripts" / "diff_workflows.py"
EVAL_SCRIPT = SKILL_DIR / "scripts" / "evaluate_benchmarks.py"
MIGRATE_SCRIPT = SKILL_DIR / "scripts" / "migrate_workflow.py"
SECURITY_SCRIPT = SKILL_DIR / "scripts" / "security_audit.py"
PACKAGE_SCRIPT = SKILL_DIR / "scripts" / "package_skill.py"
BENCHMARK_DIR = SKILL_DIR / "benchmarks"


def run_command(
    *args: str, cwd: Path | None = None, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


class DeterministicWorkflowTests(unittest.TestCase):
    def test_scaffold_creates_manifest_and_runner(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = run_command(
                "python3",
                str(INIT_SCRIPT),
                "release-check",
                "--path",
                str(root),
                "--steps",
                "fetch,validate,test",
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            workflow_dir = root / "release-check"
            self.assertTrue((workflow_dir / "WORKFLOW_SPEC.md").exists())
            self.assertTrue((workflow_dir / "workflow.json").exists())
            self.assertTrue((workflow_dir / "run_workflow.sh").exists())
            self.assertTrue((workflow_dir / "state" / "approval-status.tsv").exists())
            self.assertTrue((workflow_dir / "state" / "run-counter.txt").exists())
            self.assertTrue((workflow_dir / "audit" / "runs").exists())

            listed = run_command(str(workflow_dir / "run_workflow.sh"), "--list")
            self.assertEqual(listed.returncode, 0, listed.stderr)
            self.assertIn("01-fetch", listed.stdout)
            self.assertIn("approval", listed.stdout)

            verified = run_command("python3", str(VERIFY_SCRIPT), str(workflow_dir), "--simulate")
            self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
            self.assertIn("[SIMULATION] step_order=", verified.stdout)

    def test_runner_uses_local_skill_fallback_when_global_skill_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = run_command(
                "python3",
                str(INIT_SCRIPT),
                "fallback-check",
                "--path",
                str(root),
                "--steps",
                "fetch",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            workflow_dir = root / "fallback-check"
            isolated_home = root / "isolated-home"
            isolated_home.mkdir()
            env = dict(os.environ)
            env["HOME"] = str(isolated_home)
            env["CODEX_HOME"] = str(isolated_home / ".codex-missing")
            listed = run_command(str(workflow_dir / "run_workflow.sh"), "--list", env=env)
            self.assertEqual(listed.returncode, 0, listed.stderr)
            self.assertIn("01-fetch", listed.stdout)

    def test_failed_step_marks_failed_and_writes_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "demo-flow",
                "--path",
                str(root),
                "--steps",
                "fetch,validate",
            )
            workflow_dir = root / "demo-flow"

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-fetch")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

            state = (workflow_dir / "state" / "step-status.tsv").read_text(encoding="utf-8")
            log_text = (workflow_dir / "logs" / "01-fetch.log").read_text(encoding="utf-8")
            self.assertIn("01-fetch\tfailed", state)
            self.assertIn("not implemented yet", log_text)

            runs = run_command(str(workflow_dir / "run_workflow.sh"), "--list-runs")
            self.assertEqual(runs.returncode, 0)
            self.assertIn("run-0001", runs.stdout)

    def test_approval_gate_blocks_until_approved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "approved-flow",
                "--path",
                str(root),
                "--steps",
                "review,apply",
            )
            workflow_dir = root / "approved-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["goal"] = "Approve deterministic review workflow"
            manifest["steps"][0]["requires_approval"] = True
            manifest["steps"][0]["success_gate"] = "log contains approved review"
            manifest["steps"][0]["produces"] = ["artifacts/01-review.done"]
            manifest["residual_nondeterminism"] = ["none"]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-review.sh").write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "mkdir -p artifacts\n"
                "touch artifacts/01-review.done\n"
                "echo approved review\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-review.sh").chmod(0o755)

            blocked = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-review")
            self.assertEqual(blocked.returncode, 3, blocked.stdout + blocked.stderr)
            self.assertIn("Approval required", blocked.stderr)

            approved = run_command(
                str(workflow_dir / "run_workflow.sh"),
                "--approve",
                "01-review",
                "--approval-reason",
                "validated release checklist",
            )
            self.assertEqual(approved.returncode, 0, approved.stderr)

            completed = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-review")
            self.assertEqual(completed.returncode, 0, completed.stderr)

            step_state = (workflow_dir / "state" / "step-status.tsv").read_text(encoding="utf-8")
            approval_state = (workflow_dir / "state" / "approval-status.tsv").read_text(
                encoding="utf-8"
            )
            self.assertIn("01-review\tcomplete", step_state)
            self.assertIn("01-review\tused", approval_state)
            approval_records = (workflow_dir / "state" / "approval-records.jsonl").read_text(
                encoding="utf-8"
            )
            self.assertIn("validated release checklist", approval_records)

            replay = run_command(str(workflow_dir / "run_workflow.sh"), "--replay", "run-0001")
            self.assertEqual(replay.returncode, 0, replay.stderr)
            self.assertIn("APPROVED", replay.stdout)

    def test_policy_override_can_require_approval_for_shell_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "policy-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "policy-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["policy"] = {
                "approval": {
                    "required_for": ["shell"],
                }
            }
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["success_gate"] = "log contains collected"
            manifest["steps"][0]["produces"] = ["artifacts/01-collect.done"]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "mkdir -p artifacts\n"
                "touch artifacts/01-collect.done\n"
                "echo collected\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-collect.sh").chmod(0o755)

            blocked = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(blocked.returncode, 3, blocked.stdout + blocked.stderr)
            self.assertIn("Approval required", blocked.stderr)

            approved = run_command(
                str(workflow_dir / "run_workflow.sh"),
                "--approve",
                "01-collect",
                "--approval-reason",
                "validated by regression plan",
            )
            self.assertEqual(approved.returncode, 0, approved.stderr)

            completed = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_timeout_marks_failed_and_logs_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "timeout-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "timeout-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["timeout_seconds"] = 1
            manifest["steps"][0]["success_gate"] = "log contains done"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\nsleep 2\necho done\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-collect.sh").chmod(0o755)

            timed_out = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(timed_out.returncode, 1, timed_out.stdout + timed_out.stderr)

            state = (workflow_dir / "state" / "step-status.tsv").read_text(encoding="utf-8")
            log_text = (workflow_dir / "logs" / "01-collect.log").read_text(encoding="utf-8")
            self.assertIn("01-collect\tfailed", state)
            self.assertIn("timed out after 1 seconds", log_text)

    def test_dry_run_from_step_previews_only_requested_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "preview-flow",
                "--path",
                str(root),
                "--steps",
                "fetch,validate,test",
            )
            workflow_dir = root / "preview-flow"

            preview = run_command(
                str(workflow_dir / "run_workflow.sh"),
                "--dry-run",
                "--from-step",
                "02-validate",
            )
            self.assertEqual(preview.returncode, 0, preview.stderr)
            self.assertNotIn("01-fetch", preview.stdout)
            self.assertIn("02-validate", preview.stdout)
            self.assertIn("03-test", preview.stdout)

    def test_linter_reports_unresolved_todos(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "lint-flow",
                "--path",
                str(root),
                "--steps",
                "fetch",
            )
            workflow_dir = root / "lint-flow"

            linted = run_command("python3", str(LINT_SCRIPT), str(workflow_dir))
            self.assertEqual(linted.returncode, 1)
            self.assertIn("TODO", linted.stdout)

    def test_contract_enforcement_fails_when_artifact_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "contract-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "contract-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-collect.done",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/01-collect.done", "required": True}
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\necho forgot artifact\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-collect.sh").chmod(0o755)

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            log_text = (workflow_dir / "logs" / "01-collect.log").read_text(encoding="utf-8")
            self.assertIn("Missing required artifact", log_text)

    def test_doctor_and_repair_handle_interrupted_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "repair-flow",
                "--path",
                str(root),
                "--steps",
                "collect,verify",
            )
            workflow_dir = root / "repair-flow"
            (workflow_dir / "state" / "step-status.tsv").write_text(
                "01-collect\trunning\n02-verify\tpending\n", encoding="utf-8"
            )

            diagnosed = run_command(str(workflow_dir / "run_workflow.sh"), "--doctor")
            self.assertEqual(diagnosed.returncode, 0, diagnosed.stderr)
            self.assertIn("interrupted", diagnosed.stdout)

            repaired = run_command(str(workflow_dir / "run_workflow.sh"), "--repair")
            self.assertEqual(repaired.returncode, 0, repaired.stderr)

            state = (workflow_dir / "state" / "step-status.tsv").read_text(encoding="utf-8")
            self.assertIn("01-collect\tpending", state)

    def test_native_file_exists_step_class_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "native-flow",
                "--path",
                str(root),
                "--steps",
                "check",
            )
            workflow_dir = root / "native-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["type"] = "file-exists"
            manifest["steps"][0]["script"] = ""
            manifest["steps"][0]["executor_config"] = {"path": "artifacts/seed.txt"}
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/seed.txt",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/seed.txt", "required": True}
            ]
            manifest["steps"][0]["validation_checks"] = [
                {"type": "file_exists", "path": "artifacts/seed.txt"}
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            (workflow_dir / "artifacts").mkdir(exist_ok=True)
            (workflow_dir / "artifacts" / "seed.txt").write_text("seed\n", encoding="utf-8")

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-check")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_retry_limit_retries_before_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "retry-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "retry-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["retry_limit"] = 1
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-collect.done",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/01-collect.done", "required": True}
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "mkdir -p state artifacts\n"
                "count_file=state/retry-count.txt\n"
                "count=0\n"
                'if [[ -f "$count_file" ]]; then count=$(cat "$count_file"); fi\n'
                "count=$((count + 1))\n"
                'echo "$count" > "$count_file"\n'
                'if [[ "$count" -eq 1 ]]; then\n'
                "  echo first attempt fails >&2\n"
                "  exit 1\n"
                "fi\n"
                "touch artifacts/01-collect.done\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-collect.sh").chmod(0o755)

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            commands_log = next((workflow_dir / "audit" / "runs").iterdir()) / "commands.log"
            self.assertIn("RETRY", commands_log.read_text(encoding="utf-8"))

    def test_dag_parallel_execution_respects_max_parallel(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "parallel-flow",
                "--path",
                str(root),
                "--steps",
                "first,second,join",
            )
            workflow_dir = root / "parallel-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["policy"] = {"execution": {"max_parallel": 2}}
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][1]["depends_on"] = []
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-first.done",
            }
            manifest["steps"][1]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/02-second.done",
            }
            manifest["steps"][2]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/03-join.done",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/01-first.done", "required": True}
            ]
            manifest["steps"][1]["produces"] = [
                {"type": "file", "path": "artifacts/02-second.done", "required": True}
            ]
            manifest["steps"][2]["produces"] = [
                {"type": "file", "path": "artifacts/03-join.done", "required": True}
            ]
            manifest["steps"][2]["consumes"] = [
                {"type": "file", "path": "artifacts/01-first.done", "required": True},
                {"type": "file", "path": "artifacts/02-second.done", "required": True},
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            for step_id in ("01-first", "02-second", "03-join"):
                script = workflow_dir / "steps" / f"{step_id}.sh"
                if step_id == "03-join":
                    content = (
                        "#!/usr/bin/env bash\n"
                        "set -euo pipefail\n"
                        "mkdir -p artifacts\n"
                        "touch artifacts/03-join.done\n"
                    )
                else:
                    content = (
                        "#!/usr/bin/env bash\n"
                        "set -euo pipefail\n"
                        "sleep 1\n"
                        "mkdir -p artifacts\n"
                        f"touch artifacts/{step_id}.done\n"
                    )
                script.write_text(content, encoding="utf-8")
                script.chmod(0o755)

            started = time.monotonic()
            result = run_command(str(workflow_dir / "run_workflow.sh"))
            duration = time.monotonic() - started
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertLess(
                duration, 8.0, f"expected parallel DAG execution, got duration={duration}"
            )

    def test_offline_policy_blocks_network_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "offline-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "offline-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["policy_pack"] = "offline-only"
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-collect.done",
            }
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\ncurl https://example.com >/dev/null\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-collect.sh").chmod(0o755)

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn(
                "Offline policy blocks network commands",
                (workflow_dir / "logs" / "01-collect.log").read_text(encoding="utf-8"),
            )

    def test_rollback_runs_on_failure_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "rollback-flow",
                "--path",
                str(root),
                "--steps",
                "publish",
            )
            workflow_dir = root / "rollback-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["policy"] = {"approval": {"required_for": [], "require_reason": False}}
            manifest["steps"][0]["requires_approval"] = False
            manifest["steps"][0]["rollback"] = {
                "script": "steps/01-publish.rollback.sh",
                "when": "on_failure",
                "preconditions": ["artifacts/01-publish.done"],
            }
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-publish.done",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/01-publish.done", "required": True}
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            (workflow_dir / "steps" / "01-publish.sh").write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "mkdir -p artifacts\n"
                "touch artifacts/01-publish.done\n"
                "echo fail >&2\n"
                "exit 1\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-publish.sh").chmod(0o755)
            (workflow_dir / "steps" / "01-publish.rollback.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\nrm -f artifacts/01-publish.done\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-publish.rollback.sh").chmod(0o755)

            result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-publish")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertFalse((workflow_dir / "artifacts" / "01-publish.done").exists())

    def test_migration_upgrades_legacy_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "migrate-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "migrate-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["schema_version"] = 3
            manifest.pop("environment", None)
            manifest.pop("tooling", None)
            manifest.pop("migrations", None)
            manifest["steps"][0]["produces"] = ["artifacts/01-collect.done"]
            manifest["steps"][0]["consumes"] = []
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            result = run_command("python3", str(MIGRATE_SCRIPT), str(workflow_dir), "--write")
            self.assertEqual(result.returncode, 0, result.stderr)
            migrated = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(migrated["schema_version"], 4)
            self.assertIn("environment", migrated)
            self.assertIsInstance(migrated["steps"][0]["produces"][0], dict)

    def test_doctor_and_repair_fix_corrupted_state_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "corrupt-flow",
                "--path",
                str(root),
                "--steps",
                "collect,verify",
            )
            workflow_dir = root / "corrupt-flow"
            (workflow_dir / "state" / "step-status.tsv").write_text(
                "broken-line-without-tab\n", encoding="utf-8"
            )
            (workflow_dir / "state" / "approval-status.tsv").write_text(
                "also-broken\n", encoding="utf-8"
            )
            (workflow_dir / "state" / "runtime-state.json").write_text(
                "{not json\n", encoding="utf-8"
            )

            diagnosed = run_command(str(workflow_dir / "run_workflow.sh"), "--doctor")
            self.assertEqual(diagnosed.returncode, 0, diagnosed.stderr)
            self.assertIn("corrupt-state", diagnosed.stdout)

            repaired = run_command(str(workflow_dir / "run_workflow.sh"), "--repair")
            self.assertEqual(repaired.returncode, 0, repaired.stderr)

            healthy = run_command(str(workflow_dir / "run_workflow.sh"), "--doctor")
            self.assertEqual(healthy.returncode, 0, healthy.stderr)
            self.assertIn("Workflow state looks healthy.", healthy.stdout)

    def test_security_audit_warns_on_remote_pipe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "security-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "security-flow"
            (workflow_dir / "steps" / "01-collect.sh").write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "curl https://example.com/install.sh | bash\n",
                encoding="utf-8",
            )
            audited = run_command("python3", str(SECURITY_SCRIPT), str(workflow_dir))
            self.assertEqual(audited.returncode, 0, audited.stderr)
            self.assertIn("Remote script execution", audited.stdout)

    def test_manifest_fuzz_verify_never_crashes(self) -> None:
        random.seed(0)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "fuzz-flow",
                "--path",
                str(root),
                "--steps",
                "collect,verify",
            )
            workflow_dir = root / "fuzz-flow"
            manifest_path = workflow_dir / "workflow.json"
            baseline = json.loads(manifest_path.read_text(encoding="utf-8"))

            for index in range(10):
                mutated = json.loads(json.dumps(baseline))
                if index % 2 == 0:
                    mutated["steps"][0]["timeout_seconds"] = -1
                if index % 3 == 0:
                    mutated["steps"][1]["depends_on"] = ["missing-step"]
                if index % 4 == 0:
                    mutated["sidecars"] = [{"id": "broken"}]
                if index % 5 == 0:
                    mutated["graph"]["execution_model"] = "unknown"
                manifest_path.write_text(json.dumps(mutated, indent=2) + "\n", encoding="utf-8")
                verified = run_command("python3", str(VERIFY_SCRIPT), str(workflow_dir), "--json")
                self.assertIn(verified.returncode, {0, 1})
                payload = json.loads(verified.stdout)
                self.assertIn("issues", payload)

            manifest_path.write_text(json.dumps(baseline, indent=2) + "\n", encoding="utf-8")

    def test_soak_multiple_runs_stay_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "soak-flow",
                "--path",
                str(root),
                "--steps",
                "collect",
            )
            workflow_dir = root / "soak-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-collect.done",
            }
            manifest["steps"][0]["produces"] = [
                {"type": "file", "path": "artifacts/01-collect.done", "required": True}
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            script = workflow_dir / "steps" / "01-collect.sh"
            script.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "mkdir -p artifacts\n"
                "echo stable > artifacts/01-collect.done\n",
                encoding="utf-8",
            )
            script.chmod(0o755)

            for _ in range(3):
                result = run_command(str(workflow_dir / "run_workflow.sh"), "--step", "01-collect")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                reset = run_command(str(workflow_dir / "run_workflow.sh"), "--reset")
                self.assertEqual(reset.returncode, 0, reset.stderr)

            runs = run_command(str(workflow_dir / "run_workflow.sh"), "--list-runs")
            self.assertEqual(runs.returncode, 0, runs.stderr)
            self.assertGreaterEqual(
                len([line for line in runs.stdout.splitlines() if line.strip()]), 3
            )

    def test_verifier_rejects_cyclic_dag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "cycle-flow",
                "--path",
                str(root),
                "--steps",
                "collect,verify",
            )
            workflow_dir = root / "cycle-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["depends_on"] = ["02-verify"]
            manifest["steps"][1]["depends_on"] = ["01-collect"]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            verified = run_command("python3", str(VERIFY_SCRIPT), str(workflow_dir))
            self.assertEqual(verified.returncode, 1, verified.stdout + verified.stderr)
            self.assertIn("contains a cycle", verified.stdout)

    def test_compiler_creates_sidecars_and_prompt_assets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            compiled = run_command(
                "python3",
                str(COMPILE_SCRIPT),
                "Fix the failing CI test in the payment service and make it deterministic.",
                "--path",
                str(root),
                "--name",
                "payment-ci-fix",
            )
            self.assertEqual(compiled.returncode, 0, compiled.stderr)

            workflow_dir = root / "payment-ci-fix"
            manifest = json.loads((workflow_dir / "workflow.json").read_text(encoding="utf-8"))
            sidecar_ids = [sidecar["id"] for sidecar in manifest["sidecars"]]
            self.assertEqual(manifest["schema_version"], 4)
            self.assertEqual(manifest["policy_pack"], "ai-sidecar-safe")
            self.assertIn("candidate-generation", sidecar_ids)
            self.assertTrue(
                (workflow_dir / "assets" / "prompts" / "candidate-generation.prompt.md").exists()
            )

            sidecars = run_command(str(workflow_dir / "run_workflow.sh"), "--sidecars")
            self.assertEqual(sidecars.returncode, 0, sidecars.stderr)
            self.assertIn("candidate-generation", sidecars.stdout)

    def test_benchmark_compiles_expected_shapes(self) -> None:
        for benchmark_path in sorted(BENCHMARK_DIR.glob("*.json")):
            with (
                self.subTest(benchmark=benchmark_path.name),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
                root = Path(temp_dir)
                compiled = run_command(
                    "python3",
                    str(COMPILE_SCRIPT),
                    benchmark["request"],
                    "--path",
                    str(root),
                )
                self.assertEqual(compiled.returncode, 0, compiled.stderr)

                workflow_dirs = [path for path in root.iterdir() if path.is_dir()]
                self.assertEqual(len(workflow_dirs), 1)
                workflow_dir = workflow_dirs[0]
                manifest = json.loads((workflow_dir / "workflow.json").read_text(encoding="utf-8"))
                step_ids = [step["id"] for step in manifest["steps"]]
                sidecar_ids = [sidecar["id"] for sidecar in manifest["sidecars"]]

                self.assertEqual(step_ids, benchmark["expected_steps"])
                self.assertEqual(sidecar_ids, benchmark["expected_sidecars"])
                self.assertEqual(manifest["policy_pack"], benchmark["expected_policy_pack"])

                verified = run_command(
                    "python3", str(VERIFY_SCRIPT), str(workflow_dir), "--simulate"
                )
                self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
                self.assertIn("[SIMULATION] step_order=", verified.stdout)

    def test_auto_hardener_and_diff_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "harden-flow",
                "--path",
                str(root),
                "--steps",
                "collect,review,publish",
            )
            workflow_dir = root / "harden-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest.pop("policy_pack", None)
            manifest["residual_nondeterminism"] = []
            manifest["steps"][1]["success_gate"] = "TODO"
            manifest["sidecars"] = []
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            hardened = run_command("python3", str(HARDEN_SCRIPT), str(workflow_dir), "--write")
            self.assertEqual(hardened.returncode, 0, hardened.stderr)

            hardened_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertIn("policy_pack", hardened_manifest)
            self.assertTrue(hardened_manifest["sidecars"])

            other_root = Path(temp_dir) / "other"
            other_root.mkdir()
            run_command(
                "python3",
                str(COMPILE_SCRIPT),
                "Build a deterministic release workflow to deploy the app after validation and approval.",
                "--path",
                str(other_root),
            )
            other_dir = next(other_root.iterdir())
            diffed = run_command("python3", str(DIFF_SCRIPT), str(workflow_dir), str(other_dir))
            self.assertEqual(diffed.returncode, 0, diffed.stderr)
            self.assertIn("Policy pack:", diffed.stdout)

    def test_benchmark_evaluator_and_replay_simulation(self) -> None:
        scored = run_command("python3", str(EVAL_SCRIPT))
        self.assertEqual(scored.returncode, 0, scored.stderr)
        self.assertIn("TOTAL", scored.stdout)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "sim-flow",
                "--path",
                str(root),
                "--steps",
                "collect,verify",
            )
            workflow_dir = root / "sim-flow"
            simulated = run_command(str(workflow_dir / "run_workflow.sh"), "--dry-run")
            self.assertEqual(simulated.returncode, 0, simulated.stderr)
            self.assertIn("WOULD RUN", simulated.stdout)

    def test_package_skill_creates_installable_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "dist"
            packaged = run_command("python3", str(PACKAGE_SCRIPT), "--output-dir", str(output_dir))
            self.assertEqual(packaged.returncode, 0, packaged.stderr)

            archives = sorted(output_dir.glob("deterministic-workflow-builder-skill-v*.zip"))
            self.assertEqual(len(archives), 1)
            archive_path = archives[0]

            with zipfile.ZipFile(archive_path) as archive:
                names = set(archive.namelist())
                self.assertIn("deterministic-workflow-builder/SKILL.md", names)
                self.assertIn("deterministic-workflow-builder/README.md", names)
                self.assertIn(
                    "deterministic-workflow-builder/scripts/init_deterministic_workflow.py", names
                )
                self.assertIn(
                    "deterministic-workflow-builder/tests/test_deterministic_workflow.py", names
                )
                archive.extractall(Path(temp_dir) / "extracted")

            extracted_root = Path(temp_dir) / "extracted" / "deterministic-workflow-builder"
            generated = run_command(
                "python3",
                str(extracted_root / "scripts" / "init_deterministic_workflow.py"),
                "smoke-flow",
                "--path",
                str(Path(temp_dir) / "extracted"),
                "--steps",
                "collect",
            )
            self.assertEqual(generated.returncode, 0, generated.stderr)

            listed = run_command(
                str(Path(temp_dir) / "extracted" / "smoke-flow" / "run_workflow.sh"), "--list"
            )
            self.assertEqual(listed.returncode, 0, listed.stderr)
            self.assertIn("01-collect", listed.stdout)


if __name__ == "__main__":
    unittest.main()

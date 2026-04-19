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
            isolated_home = root / "isolated-home"
            isolated_home.mkdir()
            env = dict(os.environ)
            env["HOME"] = str(isolated_home)
            env["CODEX_HOME"] = str(root / "isolated-codex")
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

            blocked = run_command(
                str(workflow_dir / "run_workflow.sh"), "--step", "01-review", env=env
            )
            self.assertEqual(blocked.returncode, 3, blocked.stdout + blocked.stderr)
            self.assertIn("Approval required", blocked.stderr)

            approved = run_command(
                str(workflow_dir / "run_workflow.sh"),
                "--approve",
                "01-review",
                "--approval-reason",
                "validated release checklist",
                env=env,
            )
            self.assertEqual(approved.returncode, 0, approved.stderr)

            completed = run_command(
                str(workflow_dir / "run_workflow.sh"), "--step", "01-review", env=env
            )
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

            runs = run_command(str(workflow_dir / "run_workflow.sh"), "--list-runs", env=env)
            self.assertEqual(runs.returncode, 0, runs.stderr)
            latest_run = [line for line in runs.stdout.splitlines() if line.strip()][-1].strip()

            replay = run_command(
                str(workflow_dir / "run_workflow.sh"), "--replay", latest_run, env=env
            )
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


class McpStepTests(unittest.TestCase):
    RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"

    def test_mcp_step_fails_gracefully_without_library(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_command(
                "python3",
                str(INIT_SCRIPT),
                "mcp-flow",
                "--path",
                str(root),
                "--steps",
                "notify",
            )
            workflow_dir = root / "mcp-flow"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["type"] = "mcp"
            manifest["steps"][0]["script"] = ""
            manifest["steps"][0]["success_gate"] = "todo"
            manifest["steps"][0]["executor_config"] = {
                "server": "fake",
                "tool": "foo",
                "params": {},
            }
            # Add registry so the server is found (ImportError should then trigger)
            manifest["mcp_servers"] = {
                "mcpServers": {"fake": {"command": "npx", "args": ["-y", "fake-mcp"]}}
            }
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            # Patch mcp import via wrapper script
            wrapper = root / "run_mcp_test.py"
            step_id = manifest["steps"][0]["id"]
            wrapper.write_text(
                "import sys, builtins\n"
                "real_import = builtins.__import__\n"
                "def patched_import(name, *args, **kwargs):\n"
                "    if name == 'mcp' or name.startswith('mcp.'):\n"
                "        raise ImportError('mocked mcp missing')\n"
                "    return real_import(name, *args, **kwargs)\n"
                "builtins.__import__ = patched_import\n"
                f"sys.argv = ['run_workflow.py', '--workflow-dir', {str(workflow_dir)!r},"
                f" '--step', {step_id!r}]\n"
                f"sys.path.insert(0, {str(self.RUN_SCRIPT.parent)!r})\n"
                "import run_workflow\n"
                "sys.exit(run_workflow.main(sys.argv[1:]))\n",
                encoding="utf-8",
            )
            result = run_command("python3", str(wrapper))
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            log_path = workflow_dir / "logs" / f"{step_id}.log"
            self.assertTrue(log_path.exists(), f"log file missing: {result.stderr}")
            log_text = log_path.read_text(encoding="utf-8")
            self.assertIn("pip install", log_text)

    def test_mcp_param_template_expansion(self) -> None:
        import sys

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "artifacts").mkdir()
            (tmp_path / "artifacts" / "result.json").write_text(
                json.dumps({"version": "1.2.3"}), encoding="utf-8"
            )
            # Import expand_mcp_params directly — ensure scripts dir is on path
            scripts_dir = str(self.RUN_SCRIPT.parent)
            if scripts_dir not in sys.path:
                sys.path.insert(0, scripts_dir)
            import run_workflow as rw  # noqa: PLC0415

            result = rw.expand_mcp_params({"text": "v={{artifacts/result.json:version}}"}, tmp_path)
            self.assertEqual(result, {"text": "v=1.2.3"})


class SidecarMutationTests(unittest.TestCase):
    RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"

    def _base_manifest(self, name: str, steps: list, sidecars: list | None = None) -> dict:
        return {
            "schema_version": 4,
            "workflow_name": name,
            "version": 1,
            "goal": f"test {name}",
            "policy_pack": "strict-prod",
            "policy": {},
            "working_directory": ".",
            "inputs": [],
            "outputs": [],
            "graph": {"execution_model": "dag"},
            "environment": {"network_mode": "inherit"},
            "tooling": {
                "allowlisted_commands": [
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
            },
            "migrations": {"current_from": None},
            "failure_policy": {"on_error": "stop", "max_retries": 0},
            "audit": {"enabled": True, "directory": "audit/runs"},
            "residual_nondeterminism": ["none"],
            "steps": steps,
            "sidecars": sidecars or [],
        }

    def _scaffold(self, tmp_dir: Path, name: str) -> Path:
        """Create a valid workflow via init script."""
        run_command("python3", str(INIT_SCRIPT), name, "--path", str(tmp_dir), "--steps", "run")
        return tmp_dir / name

    def test_sidecar_mutation_proposal_stored(self) -> None:
        mutation_json = json.dumps(
            {
                "version": 1,
                "description": "Add a verification step",
                "type": "add_step",
                "payload": {
                    "step": {
                        "id": "02-verify",
                        "name": "verify",
                        "type": "shell",
                        "script": "steps/02-verify.sh",
                        "depends_on": ["01-run"],
                        "success_gate": "todo",
                        "requires_approval": False,
                        "retry_limit": 0,
                        "timeout_seconds": 1800,
                        "gate_type": "artifact",
                    }
                },
            }
        )
        sidecar_content = (
            "#!/usr/bin/env bash\n"
            "echo ---PROPOSE_MUTATION---\n"
            f"echo '{mutation_json}'\n"
            "echo ---END_MUTATION---\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            workflow_dir = self._scaffold(Path(tmp), "mut-wf")
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest["steps"][0]["success_gate"] = {
                "type": "file_exists",
                "path": "artifacts/01-run.done",
            }
            # Replace the TODO step script with one that passes
            (workflow_dir / "steps" / "01-run.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\nmkdir -p artifacts\ntouch artifacts/01-run.done\n",
                encoding="utf-8",
            )
            (workflow_dir / "steps" / "01-run.sh").chmod(0o755)
            sidecar_script = workflow_dir / "steps" / "sidecar.sh"
            sidecar_script.write_text(sidecar_content, encoding="utf-8")
            sidecar_script.chmod(0o755)
            manifest["sidecars"] = [
                {
                    "id": "sc-propose",
                    "name": "propose mutation",
                    "purpose": "test",
                    "when": "after",
                    "kind": "skill",
                    "script": "steps/sidecar.sh",
                    "skill_path": "assets/skills/test-skill",
                    "containment": {
                        "mode": "advisory-only",
                        "enforced_by": "test",
                        "notes": "test sidecar",
                    },
                    "output_schema": {"type": "object"},
                    "validator": "json-object",
                    "consumer_step": "01-run",
                }
            ]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            result = run_command(
                "python3",
                str(self.RUN_SCRIPT),
                "--workflow-dir",
                str(workflow_dir),
                "--step",
                "01-run",
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            mutations_path = workflow_dir / "state" / "proposed-mutations.json"
            self.assertTrue(mutations_path.exists(), "proposed-mutations.json missing")
            data = json.loads(mutations_path.read_text(encoding="utf-8"))
            self.assertTrue(len(data["mutations"]) > 0)
            self.assertEqual(data["mutations"][0]["status"], "pending")
            self.assertEqual(data["mutations"][0]["type"], "add_step")

    def test_approve_mutation_applies_add_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workflow_dir = self._scaffold(Path(tmp), "approve-wf")
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            # Build a valid new_step by cloning the existing scaffolded step
            new_step = dict(manifest["steps"][0])
            new_step["id"] = "02-new"
            new_step["name"] = "new step"
            new_step["script"] = "steps/02-new.sh"
            new_step["depends_on"] = [manifest["steps"][0]["id"]]

            mutations_data = {
                "mutations": [
                    {
                        "id": "mut-testaaaa",
                        "proposed_by": "test-sidecar",
                        "proposed_at": "2024-01-01T00:00:00Z",
                        "run_id": "run-0001",
                        "description": "Add new step",
                        "type": "add_step",
                        "payload": {"step": new_step},
                        "status": "pending",
                    }
                ]
            }
            (workflow_dir / "state" / "proposed-mutations.json").write_text(
                json.dumps(mutations_data, indent=2), encoding="utf-8"
            )

            result = run_command(
                "python3",
                str(self.RUN_SCRIPT),
                "--workflow-dir",
                str(workflow_dir),
                "--approve-mutation",
                "mut-testaaaa",
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            updated = json.loads(manifest_path.read_text(encoding="utf-8"))
            step_ids = [s["id"] for s in updated["steps"]]
            self.assertIn("02-new", step_ids)

            mutations = json.loads(
                (workflow_dir / "state" / "proposed-mutations.json").read_text(encoding="utf-8")
            )
            self.assertEqual(mutations["mutations"][0]["status"], "applied")

    def test_reject_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workflow_dir = self._scaffold(Path(tmp), "reject-wf")
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["residual_nondeterminism"] = ["none"]
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            original_step_count = len(manifest["steps"])

            mutations_data = {
                "mutations": [
                    {
                        "id": "mut-rejectbb",
                        "proposed_by": "test-sidecar",
                        "proposed_at": "2024-01-01T00:00:00Z",
                        "run_id": "run-0001",
                        "description": "Some modification",
                        "type": "modify_step",
                        "payload": {
                            "step_id": manifest["steps"][0]["id"],
                            "changes": {"retry_limit": 3},
                        },
                        "status": "pending",
                    }
                ]
            }
            (workflow_dir / "state" / "proposed-mutations.json").write_text(
                json.dumps(mutations_data, indent=2), encoding="utf-8"
            )

            result = run_command(
                "python3",
                str(self.RUN_SCRIPT),
                "--workflow-dir",
                str(workflow_dir),
                "--reject-mutation",
                "mut-rejectbb",
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Rejected", result.stdout)

            mutations = json.loads(
                (workflow_dir / "state" / "proposed-mutations.json").read_text(encoding="utf-8")
            )
            self.assertEqual(mutations["mutations"][0]["status"], "rejected")

            # workflow.json step count unchanged
            updated = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(updated["steps"]), original_step_count)


RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"
SCHEDULE_SCRIPT = SKILL_DIR / "scripts" / "schedule_workflow.py"
DASHBOARD_SCRIPT = SKILL_DIR / "scripts" / "dashboard.py"


def _scaffold_minimal(root: Path, name: str, extra_steps: list[dict] | None = None) -> Path:
    """Create a minimal valid workflow for new-feature tests."""
    import sys  # noqa: PLC0415

    result = run_command(
        "python3",
        str(INIT_SCRIPT),
        name,
        "--path",
        str(root),
        "--steps",
        "fetch",
    )
    workflow_dir = root / name
    if extra_steps:
        manifest_path = workflow_dir / "workflow.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["steps"].extend(extra_steps)
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return workflow_dir


class ClaudeStepTests(unittest.TestCase):
    """Tests for type:claude steps (mocked via a fake 'claude' binary)."""

    def _fake_claude_bin(self, tmp: Path, response: str) -> Path:
        """Write a fake claude binary that prints `response` and exits 0."""
        fake = tmp / "claude"
        fake.write_text(f"#!/usr/bin/env bash\necho '{response}'\n")
        fake.chmod(0o755)
        return fake

    def test_claude_step_writes_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            fake_claude = self._fake_claude_bin(tmp, "hello from claude")

            result = run_command(
                "python3",
                str(INIT_SCRIPT), "claude-wf", "--path", str(tmp), "--steps", "fetch",
            )
            workflow_dir = tmp / "claude-wf"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["steps"].append({
                "id": "summarise",
                "name": "Summarise output",
                "type": "claude",
                "prompt": "Say hello",
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 30,
                "depends_on": [],
            })
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            env = dict(os.environ)
            env["PATH"] = str(tmp) + ":" + env.get("PATH", "")
            result = run_command(
                "python3", str(RUN_SCRIPT), str(workflow_dir), "--step", "summarise",
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            artifact = workflow_dir / "artifacts" / "summarise.out"
            self.assertTrue(artifact.exists(), "Artifact not written")
            self.assertIn("hello", artifact.read_text(encoding="utf-8"))

    def test_claude_step_missing_prompt_fails_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            result = run_command(
                "python3",
                str(INIT_SCRIPT), "claude-validate-wf", "--path", str(tmp), "--steps", "fetch",
            )
            workflow_dir = tmp / "claude-validate-wf"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["steps"].append({
                "id": "no-prompt",
                "name": "Missing prompt",
                "type": "claude",
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 30,
                "depends_on": [],
            })
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            result = run_command(
                "python3", str(RUN_SCRIPT), str(workflow_dir), "--list",
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("prompt", result.stderr + result.stdout)

    def test_extract_json_from_claude_output(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from run_workflow import extract_json_from_claude_output  # type: ignore[import]  # noqa: PLC0415

        # fenced block
        output = 'Sure!\n```json\n{"a": 1}\n```\nDone.'
        self.assertEqual(extract_json_from_claude_output(output), {"a": 1})

        # bare JSON
        output2 = 'Here is the JSON: {"b": 2} — end'
        self.assertEqual(extract_json_from_claude_output(output2), {"b": 2})


class BranchStepTests(unittest.TestCase):
    """Tests for type:branch steps."""

    def _make_branch_workflow(self, root: Path, name: str, condition_exit: int) -> Path:
        result = run_command(
            "python3", str(INIT_SCRIPT), name, "--path", str(root), "--steps", "fetch",
        )
        workflow_dir = root / name

        cond_script = workflow_dir / "steps" / "check.sh"
        cond_script.write_text(
            f"#!/usr/bin/env bash\nexit {condition_exit}\n", encoding="utf-8"
        )
        cond_script.chmod(0o755)

        for sid in ("on-true-step", "on-false-step"):
            s = workflow_dir / "steps" / f"{sid}.sh"
            s.write_text(f"#!/usr/bin/env bash\necho '{sid} ran'\n")
            s.chmod(0o755)

        manifest_path = workflow_dir / "workflow.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # Clear init-generated contracts on existing steps so they don't require artifacts
        for step in manifest["steps"]:
            step["produces"] = []
            step["consumes"] = []
            step["validation_checks"] = []
        manifest["steps"].extend([
            {
                "id": "branch-gate",
                "name": "Branch gate",
                "type": "branch",
                "condition": "steps/check.sh",
                "on_true": ["on-true-step"],
                "on_false": ["on-false-step"],
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 10,
                "depends_on": ["01-fetch"],
            },
            {
                "id": "on-true-step",
                "name": "On-true step",
                "type": "shell",
                "script": "steps/on-true-step.sh",
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 10,
                "depends_on": ["branch-gate"],
            },
            {
                "id": "on-false-step",
                "name": "On-false step",
                "type": "shell",
                "script": "steps/on-false-step.sh",
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 10,
                "depends_on": ["branch-gate"],
            },
        ])
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        # Make 01-fetch succeed
        fetch_sh = workflow_dir / "steps" / "01-fetch.sh"
        fetch_sh.write_text("#!/usr/bin/env bash\necho ok\n")
        fetch_sh.chmod(0o755)

        return workflow_dir

    def test_branch_on_true_skips_false_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            workflow_dir = self._make_branch_workflow(tmp, "branch-true-wf", condition_exit=0)
            result = run_command("python3", str(RUN_SCRIPT), str(workflow_dir))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            state = (workflow_dir / "state" / "step-status.tsv").read_text()
            self.assertIn("on-true-step\tcomplete", state)
            self.assertIn("on-false-step\tskipped", state)

    def test_branch_on_false_skips_true_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            workflow_dir = self._make_branch_workflow(tmp, "branch-false-wf", condition_exit=1)
            result = run_command("python3", str(RUN_SCRIPT), str(workflow_dir))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            state = (workflow_dir / "state" / "step-status.tsv").read_text()
            self.assertIn("on-false-step\tcomplete", state)
            self.assertIn("on-true-step\tskipped", state)

    def test_branch_validation_requires_condition_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            result = run_command(
                "python3", str(INIT_SCRIPT), "branch-val-wf", "--path", str(tmp), "--steps", "fetch",
            )
            workflow_dir = tmp / "branch-val-wf"
            manifest_path = workflow_dir / "workflow.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["steps"].append({
                "id": "bad-branch",
                "name": "Bad branch",
                "type": "branch",
                # missing 'condition', 'on_true', 'on_false'
                "success_gate": "",
                "gate_type": "artifact",
                "requires_approval": False,
                "retry_limit": 0,
                "timeout_seconds": 10,
                "depends_on": [],
            })
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            result = run_command("python3", str(RUN_SCRIPT), str(workflow_dir), "--list")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("condition", result.stderr + result.stdout)


class TriggerTests(unittest.TestCase):
    """Tests for schedule/webhook trigger installation."""

    def test_triggers_schema_valid(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from workflow_schema import validate_manifest  # type: ignore[import]  # noqa: PLC0415

        manifest = {
            "schema_version": 4, "workflow_name": "trig-wf", "version": 1,
            "goal": "test", "policy_pack": "strict-prod",
            "graph": {"execution_model": "dag"}, "steps": [],
            "triggers": [
                {"type": "schedule", "cron": "0 9 * * 1-5"},
                {"type": "webhook", "port": 9090},
            ],
        }
        from pathlib import Path as P  # noqa: PLC0415, N814
        issues = validate_manifest(manifest, P("/fake/workflow.json"))
        errors = [i for i in issues if i.severity == "error" and "trigger" in i.message.lower()]
        self.assertEqual(errors, [], [i.message for i in errors])

    def test_triggers_schema_invalid_type(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from workflow_schema import validate_manifest  # type: ignore[import]  # noqa: PLC0415

        manifest = {
            "schema_version": 4, "workflow_name": "bad-trig", "version": 1,
            "goal": "test", "policy_pack": "strict-prod",
            "graph": {"execution_model": "dag"}, "steps": [],
            "triggers": [{"type": "unknown-type"}],
        }
        from pathlib import Path as P  # noqa: PLC0415, N814
        issues = validate_manifest(manifest, P("/fake/workflow.json"))
        errors = [i for i in issues if i.severity == "error"]
        self.assertTrue(any("trigger" in i.message.lower() for i in errors))

    def test_webhook_server_script_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            import sys  # noqa: PLC0415
            sys.path.insert(0, str(SKILL_DIR / "scripts"))
            from schedule_workflow import install_webhook_trigger  # type: ignore[import]  # noqa: PLC0415

            workflow_dir = tmp / "webhook-wf"
            workflow_dir.mkdir()
            code = install_webhook_trigger({"type": "webhook", "port": 9999}, workflow_dir)
            self.assertEqual(code, 0)
            server_script = workflow_dir / "scripts" / "webhook_server.py"
            self.assertTrue(server_script.exists())
            content = server_script.read_text(encoding="utf-8")
            self.assertIn("9999", content)


class NewStepTypeTests(unittest.TestCase):
    """Tests for http, switch, loop, wait, merge, workflow step types."""

    RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"

    def _base_step(self, step_id: str, step_type: str, extra: dict | None = None) -> dict:
        s = {
            "id": step_id, "name": step_id, "type": step_type,
            "success_gate": "", "gate_type": "artifact",
            "requires_approval": False, "retry_limit": 0,
            "timeout_seconds": 30, "depends_on": [],
            "produces": [], "consumes": [], "validation_checks": [],
        }
        if extra:
            s.update(extra)
        return s

    def _scaffold(self, root: Path, name: str, steps: list[dict]) -> Path:
        run_command("python3", str(INIT_SCRIPT), name, "--path", str(root), "--steps", "fetch")
        wf = root / name
        # Clear contracts on init-generated step
        mp = wf / "workflow.json"
        m = json.loads(mp.read_text())
        for s in m["steps"]:
            s["produces"] = []; s["consumes"] = []; s["validation_checks"] = []
        m["steps"].extend(steps)
        mp.write_text(json.dumps(m, indent=2) + "\n")
        # Make fetch succeed
        (wf / "steps" / "01-fetch.sh").write_text("#!/usr/bin/env bash\necho ok\n")
        (wf / "steps" / "01-fetch.sh").chmod(0o755)
        return wf

    # ── http ──────────────────────────────────────────────────────────────────
    def test_http_step_calls_url_and_writes_artifact(self) -> None:
        import http.server, threading  # noqa: E401, PLC0415
        responses = []
        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200); self.end_headers()
                self.wfile.write(b'{"ok":true}')
            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.handle_request)
        t.start()
        with tempfile.TemporaryDirectory() as tmp:
            wf = self._scaffold(Path(tmp), "http-wf", [
                self._base_step("call-api", "http", {
                    "url": f"http://127.0.0.1:{port}/",
                    "method": "GET",
                    "depends_on": ["01-fetch"],
                })
            ])
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf))
            t.join(timeout=5)
            srv.server_close()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            artifact = wf / "artifacts" / "call-api.json"
            self.assertTrue(artifact.exists())
            data = json.loads(artifact.read_text())
            self.assertEqual(data["status_code"], 200)

    def test_http_step_fails_on_4xx_by_default(self) -> None:
        import http.server, threading  # noqa: E401, PLC0415
        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(404); self.end_headers()
            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.handle_request)
        t.start()
        with tempfile.TemporaryDirectory() as tmp:
            wf = self._scaffold(Path(tmp), "http-fail-wf", [
                self._base_step("call-404", "http", {
                    "url": f"http://127.0.0.1:{port}/",
                    "depends_on": ["01-fetch"],
                })
            ])
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf), "--step", "call-404")
            t.join(timeout=5)
            srv.server_close()
            self.assertNotEqual(result.returncode, 0)

    # ── switch ────────────────────────────────────────────────────────────────
    def test_switch_step_skips_non_matching_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wf = self._scaffold(root, "switch-wf", [
                self._base_step("decide", "switch", {
                    "expression": "{{env:DEPLOY_ENV}}",
                    "cases": [
                        {"value": "prod",    "steps": ["deploy-prod"]},
                        {"value": "staging", "steps": ["deploy-staging"]},
                    ],
                    "default": ["deploy-staging"],
                    "depends_on": ["01-fetch"],
                }),
                self._base_step("deploy-prod",    "shell", {"script": "steps/deploy-prod.sh",    "depends_on": ["decide"]}),
                self._base_step("deploy-staging", "shell", {"script": "steps/deploy-staging.sh", "depends_on": ["decide"]}),
            ])
            for name in ("deploy-prod", "deploy-staging"):
                s = wf / "steps" / f"{name}.sh"
                s.write_text(f"#!/usr/bin/env bash\necho {name}\n"); s.chmod(0o755)
            env = dict(os.environ); env["DEPLOY_ENV"] = "prod"
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf), env=env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            state = (wf / "state" / "step-status.tsv").read_text()
            self.assertIn("deploy-prod\tcomplete", state)
            self.assertIn("deploy-staging\tskipped", state)

    # ── loop ──────────────────────────────────────────────────────────────────
    def test_loop_step_iterates_over_json_array(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wf = self._scaffold(root, "loop-wf", [
                self._base_step("process-items", "loop", {
                    "items_from": "items-list",
                    "script": "steps/process-item.sh",
                    "depends_on": ["01-fetch"],
                })
            ])
            (wf / "artifacts").mkdir(exist_ok=True)
            (wf / "artifacts" / "items-list.json").write_text('["apple","banana","cherry"]')
            proc_sh = wf / "steps" / "process-item.sh"
            proc_sh.write_text("#!/usr/bin/env bash\necho \"processing: $LOOP_ITEM\"\n"); proc_sh.chmod(0o755)
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            out = json.loads((wf / "artifacts" / "process-items.json").read_text())
            self.assertEqual(len(out), 3)
            self.assertTrue(all(r["returncode"] == 0 for r in out))

    # ── wait ──────────────────────────────────────────────────────────────────
    def test_wait_step_sleeps_for_duration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wf = self._scaffold(Path(tmp), "wait-wf", [
                self._base_step("pause", "wait", {"seconds": 0.1, "depends_on": ["01-fetch"]})
            ])
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            state = (wf / "state" / "step-status.tsv").read_text()
            self.assertIn("pause\tcomplete", state)

    # ── merge ─────────────────────────────────────────────────────────────────
    def test_merge_step_concatenates_json_arrays(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wf = self._scaffold(Path(tmp), "merge-wf", [
                self._base_step("combine", "merge", {
                    "inputs": ["part-a", "part-b"],
                    "mode": "concat",
                    "depends_on": ["01-fetch"],
                })
            ])
            (wf / "artifacts").mkdir(exist_ok=True)
            (wf / "artifacts" / "part-a.json").write_text('[1, 2]')
            (wf / "artifacts" / "part-b.json").write_text('[3, 4]')
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            out = json.loads((wf / "artifacts" / "combine.json").read_text())
            self.assertEqual(sorted(out), [1, 2, 3, 4])

    # ── workflow (sub-workflow) ───────────────────────────────────────────────
    def test_workflow_step_runs_sub_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Create the sub-workflow
            run_command("python3", str(INIT_SCRIPT), "sub-wf", "--path", str(root), "--steps", "work")
            sub_wf = root / "sub-wf"
            sm = sub_wf / "workflow.json"
            sm_data = json.loads(sm.read_text())
            for s in sm_data["steps"]:
                s["produces"] = []; s["consumes"] = []; s["validation_checks"] = []
            sm.write_text(json.dumps(sm_data, indent=2) + "\n")
            (sub_wf / "steps" / "01-work.sh").write_text("#!/usr/bin/env bash\necho sub-done\n")
            (sub_wf / "steps" / "01-work.sh").chmod(0o755)

            # Parent workflow with a workflow step
            wf = self._scaffold(root, "parent-wf", [
                self._base_step("run-sub", "workflow", {
                    "workflow_dir": str(sub_wf),
                    "depends_on": ["01-fetch"],
                })
            ])
            result = run_command("python3", str(self.RUN_SCRIPT), str(wf))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            state = (wf / "state" / "step-status.tsv").read_text()
            self.assertIn("run-sub\tcomplete", state)


class DashboardTests(unittest.TestCase):
    """Tests for the run history dashboard."""

    def test_dashboard_generates_html_with_run_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            import sys  # noqa: PLC0415
            sys.path.insert(0, str(SKILL_DIR / "scripts"))
            from dashboard import load_all_runs, generate_dashboard_html  # type: ignore[import]  # noqa: PLC0415

            audit_root = tmp / "audit"
            run_dir = audit_root / "runs" / "run-0001"
            run_dir.mkdir(parents=True)
            metrics = {
                "started_at": "2025-01-01T10:00:00Z",
                "ended_at": "2025-01-01T10:00:42Z",
                "status": "complete",
                "steps": {
                    "fetch": {"duration_seconds": 3.2, "returncode": 0, "status": "complete"},
                },
            }
            (run_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

            runs = load_all_runs(audit_root)
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], "run-0001")
            self.assertEqual(runs[0]["status"], "complete")

            html = generate_dashboard_html(runs, "test-workflow")
            self.assertIn("run-0001", html)
            self.assertIn("test-workflow", html)

    def test_dashboard_empty_runs_shows_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            import sys  # noqa: PLC0415
            sys.path.insert(0, str(SKILL_DIR / "scripts"))
            from dashboard import generate_dashboard_html  # type: ignore[import]  # noqa: PLC0415

            html = generate_dashboard_html([], "empty-wf")
            self.assertIn("No runs recorded", html)


DISCOVER_SCRIPT = SKILL_DIR / "scripts" / "discover_skills.py"


class SkillDiscoveryTests(unittest.TestCase):
    """Tests for discover_skills.py and type:skill step execution."""

    RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"

    def _base_step(self, step_id: str, step_type: str, extra: dict | None = None) -> dict:
        s = {
            "id": step_id, "name": step_id, "type": step_type,
            "success_gate": "", "gate_type": "artifact",
            "requires_approval": False, "retry_limit": 0,
            "timeout_seconds": 30, "depends_on": [],
            "produces": [], "consumes": [], "validation_checks": [],
        }
        if extra:
            s.update(extra)
        return s

    def test_discover_finds_skills_in_custom_path(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from discover_skills import discover  # type: ignore[import]  # noqa: PLC0415

        with tempfile.TemporaryDirectory() as tmp:
            skills_root = Path(tmp) / "skills"
            # Create two fake skills
            for name, has_md in (("code-reviewer", True), ("security-auditor", True)):
                d = skills_root / name
                d.mkdir(parents=True)
                if has_md:
                    (d / "SKILL.md").write_text(f"# {name}\nDoes {name} things.\n")

            found = discover(extra_paths=[skills_root])
            names = [s["name"] for s in found]
            self.assertIn("code-reviewer", names)
            self.assertIn("security-auditor", names)

    def test_discover_deduplicates_by_name(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from discover_skills import discover  # type: ignore[import]  # noqa: PLC0415

        with tempfile.TemporaryDirectory() as tmp:
            root1 = Path(tmp) / "path1"
            root2 = Path(tmp) / "path2"
            for r in (root1, root2):
                d = r / "my-skill"
                d.mkdir(parents=True)
                (d / "SKILL.md").write_text("# my-skill\n")

            found = discover(extra_paths=[root1, root2])
            matching = [s for s in found if s["name"] == "my-skill"]
            self.assertEqual(len(matching), 1)

    def test_skill_step_not_found_fails_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_command("python3", str(INIT_SCRIPT), "skill-wf", "--path", str(root), "--steps", "fetch")
            wf = root / "skill-wf"
            mp = wf / "workflow.json"
            m = json.loads(mp.read_text())
            for s in m["steps"]:
                s["produces"] = []; s["consumes"] = []; s["validation_checks"] = []
            m["steps"].append(self._base_step("run-skill", "skill", {
                "skill": "nonexistent-skill-xyz",
                "instruction": "do something",
                "depends_on": ["01-fetch"],
            }))
            mp.write_text(json.dumps(m, indent=2) + "\n")
            (wf / "steps" / "01-fetch.sh").write_text("#!/usr/bin/env bash\necho ok\n")
            (wf / "steps" / "01-fetch.sh").chmod(0o755)

            result = run_command("python3", str(self.RUN_SCRIPT), str(wf), "--step", "run-skill")
            self.assertNotEqual(result.returncode, 0)
            log = (wf / "logs" / "run-skill.log").read_text()
            self.assertIn("not found", log)

    def test_discover_skills_command_exits_zero(self) -> None:
        result = run_command("python3", str(self.RUN_SCRIPT), "--discover-skills")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_skill_validation_requires_skill_name(self) -> None:
        import sys  # noqa: PLC0415
        sys.path.insert(0, str(SKILL_DIR / "scripts"))
        from workflow_schema import validate_manifest  # type: ignore[import]  # noqa: PLC0415
        from pathlib import Path as P  # noqa: PLC0415, N814

        manifest = {
            "schema_version": 4, "workflow_name": "sk-val", "version": 1,
            "goal": "test", "policy_pack": "strict-prod",
            "graph": {"execution_model": "dag"},
            "steps": [{
                "id": "bad-skill", "name": "bad", "type": "skill",
                # missing "skill" field
                "success_gate": "", "gate_type": "artifact",
                "requires_approval": False, "retry_limit": 0, "timeout_seconds": 30,
            }],
        }
        issues = validate_manifest(manifest, P("/fake/workflow.json"))
        errors = [i for i in issues if i.severity == "error"]
        self.assertTrue(any("skill" in i.message.lower() for i in errors))


class N8nImportTests(unittest.TestCase):
    """Tests for --import-n8n / import_n8n.py."""

    RUN_SCRIPT = SKILL_DIR / "scripts" / "run_workflow.py"
    IMPORT_SCRIPT = SKILL_DIR / "scripts" / "import_n8n.py"

    def _load_importer(self):
        import importlib.util  # noqa: PLC0415
        spec = importlib.util.spec_from_file_location("import_n8n", self.IMPORT_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _minimal_n8n(self, name: str = "Test Workflow") -> dict:
        return {
            "name": name,
            "nodes": [
                {
                    "id": "n1",
                    "name": "Execute Command",
                    "type": "n8n-nodes-base.executeCommand",
                    "parameters": {"command": "echo hello"},
                    "position": [100, 200],
                },
                {
                    "id": "n2",
                    "name": "HTTP Request",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {"method": "GET", "url": "https://example.com/api"},
                    "position": [300, 200],
                },
            ],
            "connections": {
                "Execute Command": {
                    "main": [[{"node": "HTTP Request", "type": "main", "index": 0}]]
                }
            },
        }

    def test_convert_basic_workflow(self) -> None:
        mod = self._load_importer()
        manifest, proposals = mod.convert(self._minimal_n8n())
        self.assertEqual(manifest["schema_version"], 4)
        steps = manifest["steps"]
        self.assertEqual(len(steps), 2)
        ids = [s["id"] for s in steps]
        self.assertIn("execute-command", ids)
        self.assertIn("http-request", ids)

    def test_dependency_preserved(self) -> None:
        mod = self._load_importer()
        manifest, _ = mod.convert(self._minimal_n8n())
        steps_by_id = {s["id"]: s for s in manifest["steps"]}
        # http-request depends on execute-command
        self.assertIn("execute-command", steps_by_id["http-request"].get("needs", []))

    def test_http_node_mapped_correctly(self) -> None:
        mod = self._load_importer()
        manifest, _ = mod.convert(self._minimal_n8n())
        http_step = next(s for s in manifest["steps"] if s["id"] == "http-request")
        self.assertEqual(http_step["type"], "http")
        self.assertEqual(http_step["method"], "GET")
        self.assertEqual(http_step["url"], "https://example.com/api")

    def test_triggers_extracted(self) -> None:
        mod = self._load_importer()
        export = self._minimal_n8n()
        export["nodes"].append({
            "id": "t1",
            "name": "Cron",
            "type": "n8n-nodes-base.cron",
            "parameters": {"rule": {"interval": [{"field": "hours", "hoursInterval": 2}]}},
            "position": [0, 0],
        })
        manifest, _ = mod.convert(export)
        triggers = manifest.get("triggers", [])
        self.assertEqual(len(triggers), 1)
        self.assertEqual(triggers[0]["type"], "schedule")
        self.assertIn("*/2", triggers[0]["cron"])

    def test_service_node_generates_improvement_proposal(self) -> None:
        mod = self._load_importer()
        export = {
            "name": "Slack Notifier",
            "nodes": [{
                "id": "s1",
                "name": "Send Slack",
                "type": "n8n-nodes-base.slack",
                "parameters": {"resource": "message", "operation": "post"},
                "position": [0, 0],
            }],
            "connections": {},
        }
        _, proposals = mod.convert(export)
        self.assertTrue(any("mcp" in p.get("rationale", "").lower() for p in proposals))

    def test_skip_nodes_excluded(self) -> None:
        mod = self._load_importer()
        export = {
            "name": "With Sticky",
            "nodes": [
                {
                    "id": "sticky",
                    "name": "Note",
                    "type": "n8n-nodes-base.stickyNote",
                    "parameters": {},
                    "position": [0, 0],
                },
                {
                    "id": "cmd",
                    "name": "Run",
                    "type": "n8n-nodes-base.executeCommand",
                    "parameters": {"command": "echo ok"},
                    "position": [100, 0],
                },
            ],
            "connections": {},
        }
        manifest, _ = mod.convert(export)
        self.assertEqual(len(manifest["steps"]), 1)
        self.assertEqual(manifest["steps"][0]["id"], "run")

    def test_cli_import_n8n_flag(self) -> None:
        import tempfile  # noqa: PLC0415
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            export_file = tmp / "export.json"
            export_file.write_text(json.dumps(self._minimal_n8n()), encoding="utf-8")
            out_dir = tmp / "imported"
            result = run_command(
                "python3", str(self.RUN_SCRIPT),
                "--import-n8n", str(export_file),
                "--output-dir", str(out_dir),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((out_dir / "workflow.json").exists())
            self.assertTrue((out_dir / "run_workflow.sh").exists())


if __name__ == "__main__":
    unittest.main()

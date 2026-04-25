#!/usr/bin/env python3
"""Install and manage workflow triggers (schedule + webhook)."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from pathlib import Path

RUNNER_PATH = Path(__file__).resolve().parent / "run_workflow.py"

_LAUNCHD_PLIST_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>{runner}</string>
        <string>{workflow_dir}</string>
    </array>
    <key>StartCalendarInterval</key>
    {calendar_interval}
    <key>StandardOutPath</key>
    <string>{log_out}</string>
    <key>StandardErrorPath</key>
    <string>{log_err}</string>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
"""

_WEBHOOK_SERVER_TEMPLATE = """\
#!/usr/bin/env python3
\"\"\"Minimal webhook trigger server for a deterministic workflow.\"\"\"
from __future__ import annotations
import hashlib
import hmac
import http.server
import subprocess
import sys
from pathlib import Path

RUNNER = {runner!r}
WORKFLOW_DIR = {workflow_dir!r}
PORT = {port}
EXPECTED_PATH = {path!r}
SECRET_TOKEN = {secret!r}  # empty string = no auth required
BIND_HOST = "127.0.0.1"  # localhost only — expose via reverse proxy if needed


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        # Path check
        if self.path != EXPECTED_PATH:
            self.send_response(404)
            self.end_headers()
            return
        # Token auth (constant-time compare)
        if SECRET_TOKEN:
            auth = self.headers.get("X-Webhook-Token", "")
            if not hmac.compare_digest(auth, SECRET_TOKEN):
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b"Unauthorized\\n")
                return
        self.send_response(202)
        self.end_headers()
        self.wfile.write(b"Workflow triggered\\n")
        subprocess.Popen(
            [sys.executable, RUNNER, WORKFLOW_DIR],
            stdout=open(Path(WORKFLOW_DIR) / "logs" / "webhook.log", "a"),
            stderr=subprocess.STDOUT,
        )

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    auth_note = f"token auth enabled" if SECRET_TOKEN else "WARNING: no token auth configured"
    print(f"Webhook server on {{BIND_HOST}}:{{PORT}}{{EXPECTED_PATH}} ({{auth_note}})")
    print(f"Workflow: {{WORKFLOW_DIR}}")
    if SECRET_TOKEN:
        print(f"Trigger: curl -X POST -H 'X-Webhook-Token: {{SECRET_TOKEN}}' http://{{BIND_HOST}}:{{PORT}}{{EXPECTED_PATH}}")
    http.server.HTTPServer((BIND_HOST, PORT), Handler).serve_forever()
"""


def _parse_cron_to_calendar(cron: str) -> str:
    """Convert a 5-field cron expression to a launchd StartCalendarInterval XML snippet."""
    fields = cron.split()
    if len(fields) != 5:
        raise ValueError(f"Expected 5-field cron, got: {cron!r}")
    minute, hour, day, month, weekday = fields
    parts: list[str] = []

    def _add(key: str, val: str) -> None:
        if val != "*":
            parts.append(f"<key>{key}</key><integer>{val}</integer>")

    _add("Minute", minute)
    _add("Hour", hour)
    _add("Day", day)
    _add("Month", month)
    _add("Weekday", weekday)
    if not parts:
        return "<dict/>"
    return "<dict>" + "".join(parts) + "</dict>"


def install_schedule_trigger(trigger: dict, workflow_dir: Path) -> int:
    cron = trigger.get("cron", "")
    description = trigger.get("description", workflow_dir.name)
    label = f"com.deterministic-workflow.{workflow_dir.name}"
    log_dir = workflow_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if platform.system() == "Darwin":
        try:
            calendar = _parse_cron_to_calendar(cron)
        except ValueError as exc:
            print(f"[triggers] Invalid cron expression: {exc}", file=sys.stderr)
            return 1
        plist_dir = Path.home() / "Library" / "LaunchAgents"
        plist_dir.mkdir(parents=True, exist_ok=True)
        plist_path = plist_dir / f"{label}.plist"
        plist_content = _LAUNCHD_PLIST_TEMPLATE.format(
            label=label,
            python=sys.executable,
            runner=str(RUNNER_PATH),
            workflow_dir=str(workflow_dir),
            calendar_interval=calendar,
            log_out=str(log_dir / "trigger.stdout.log"),
            log_err=str(log_dir / "trigger.stderr.log"),
        )
        plist_path.write_text(plist_content, encoding="utf-8")
        subprocess.run(["launchctl", "load", str(plist_path)], check=False)
        print(f"[triggers] Installed launchd schedule trigger: {plist_path}")
        print(f"  Schedule: {cron!r}  ({description})")
        print(f"  To unload: launchctl unload {plist_path}")
    else:
        existing = ""
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
            existing = result.stdout if result.returncode == 0 else ""
        except FileNotFoundError:
            print("[triggers] crontab not available on this system.", file=sys.stderr)
            return 1
        entry = f"{cron}  {sys.executable} {RUNNER_PATH} {workflow_dir}  # {label}\n"
        if label in existing:
            print(f"[triggers] Cron entry for {label} already exists. Remove it first.")
            return 1
        new_crontab = existing + entry
        proc = subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=False)
        if proc.returncode != 0:
            print("[triggers] Failed to install cron entry.", file=sys.stderr)
            return 1
        print(f"[triggers] Installed cron schedule trigger: {cron!r}  ({description})")
    return 0


def install_webhook_trigger(trigger: dict, workflow_dir: Path) -> int:
    port = trigger.get("port", 8080)
    path = trigger.get("path", "/webhook")
    secret = trigger.get("secret", "")
    server_script = workflow_dir / "scripts" / "webhook_server.py"
    server_script.parent.mkdir(parents=True, exist_ok=True)
    server_script.write_text(
        _WEBHOOK_SERVER_TEMPLATE.format(
            runner=str(RUNNER_PATH),
            workflow_dir=str(workflow_dir),
            port=port,
            path=path,
            secret=secret,
        ),
        encoding="utf-8",
    )
    server_script.chmod(0o755)
    print(f"[triggers] Created webhook server: {server_script}")
    print(f"  Port: {port}  Path: {path}")
    print(f"  Start: python3 {server_script}")
    auth_note = f" -H 'X-Webhook-Token: {secret}'" if secret else ""
    print(f"  Trigger: curl -X POST{auth_note} http://localhost:{port}{path}")
    return 0


def install_triggers(manifest: dict, workflow_dir: Path) -> int:
    triggers = manifest.get("triggers", [])
    if not triggers:
        print("[triggers] No triggers defined in workflow.json.")
        return 0
    code = 0
    for trigger in triggers:
        ttype = trigger.get("type")
        if ttype == "schedule":
            code = max(code, install_schedule_trigger(trigger, workflow_dir))
        elif ttype == "webhook":
            code = max(code, install_webhook_trigger(trigger, workflow_dir))
        else:
            print(f"[triggers] Unknown trigger type: {ttype!r}", file=sys.stderr)
            code = 1
    return code


def main(argv: list[str]) -> int:
    if len(argv) < 1:
        print("Usage: schedule_workflow.py <workflow-dir>")
        return 1
    workflow_dir = Path(argv[0]).resolve()
    manifest_path = workflow_dir / "workflow.json"
    if not manifest_path.exists():
        print(f"[triggers] No workflow.json found at {manifest_path}", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return install_triggers(manifest, workflow_dir)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

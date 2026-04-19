#!/usr/bin/env python3
"""Generate a self-contained HTML run history dashboard for a deterministic workflow."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any


def load_all_runs(audit_root: Path, max_runs: int = 100) -> list[dict[str, Any]]:
    runs_dir = audit_root / "runs"
    if not runs_dir.exists():
        return []
    run_dirs = sorted(runs_dir.iterdir())[-max_runs:]
    summaries = []
    for run_dir in run_dirs:
        if not run_dir.is_dir():
            continue
        summary = compute_run_summary(run_dir)
        if summary:
            summaries.append(summary)
    return list(reversed(summaries))


def compute_run_summary(run_dir: Path) -> dict[str, Any] | None:
    metrics_path = run_dir / "metrics.json"
    events_path = run_dir / "events.jsonl"
    if not metrics_path.exists() and not events_path.exists():
        return None

    run_id = run_dir.name
    started_at = ""
    ended_at = ""
    steps: list[dict[str, Any]] = []
    overall_status = "unknown"

    if metrics_path.exists():
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            started_at = metrics.get("started_at", "")
            ended_at = metrics.get("ended_at", "")
            overall_status = metrics.get("status", "unknown")
            for step_id, step_data in metrics.get("steps", {}).items():
                steps.append(
                    {
                        "step_id": step_id,
                        "duration_seconds": step_data.get("duration_seconds", 0),
                        "returncode": step_data.get("returncode", -1),
                        "status": step_data.get("status", "unknown"),
                    }
                )
        except (json.JSONDecodeError, OSError):
            pass

    if not started_at and events_path.exists():
        try:
            lines = events_path.read_text(encoding="utf-8").splitlines()
            for line in lines:
                ev = json.loads(line)
                if ev.get("event") == "run_started":
                    started_at = ev.get("timestamp", "")
                    break
        except (json.JSONDecodeError, OSError):
            pass

    duration_seconds: float = 0
    if started_at and ended_at:
        from datetime import datetime, timezone  # noqa: PLC0415

        try:
            fmt = "%Y-%m-%dT%H:%M:%SZ"
            t0 = datetime.strptime(started_at, fmt).replace(tzinfo=timezone.utc)
            t1 = datetime.strptime(ended_at, fmt).replace(tzinfo=timezone.utc)
            duration_seconds = (t1 - t0).total_seconds()
        except ValueError:
            pass

    failed_count = sum(1 for s in steps if s.get("returncode", 0) != 0)
    viz_path = run_dir / "workflow-graph.html"

    return {
        "run_id": run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "step_count": len(steps),
        "failed_count": failed_count,
        "status": overall_status,
        "steps": steps,
        "viz_path": str(viz_path) if viz_path.exists() else None,
    }


def _status_badge(status: str) -> str:
    colors = {
        "complete": "#22c55e",
        "failed": "#ef4444",
        "partial": "#f59e0b",
        "unknown": "#6b7280",
    }
    color = colors.get(status, "#6b7280")
    return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px">{status}</span>'


def _duration_str(seconds: float) -> str:
    if seconds <= 0:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}m {s}s"


def generate_dashboard_html(runs: list[dict[str, Any]], workflow_name: str) -> str:
    rows = ""
    for run in runs:
        run_id = run["run_id"]
        viz = run.get("viz_path")
        link = f'<a href="{viz}" target="_blank">{run_id}</a>' if viz else run_id
        rows += f"""
        <tr>
            <td>{link}</td>
            <td>{run.get("started_at", "—")}</td>
            <td>{_duration_str(run.get("duration_seconds", 0))}</td>
            <td>{run.get("step_count", 0)} / {run.get("failed_count", 0)} failed</td>
            <td>{_status_badge(run.get("status", "unknown"))}</td>
        </tr>"""

    no_runs_msg = "" if runs else '<p style="color:#6b7280;text-align:center;padding:32px">No runs recorded yet.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Dashboard — {workflow_name}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }}
  h1 {{ font-size: 20px; margin-bottom: 4px; }}
  .subtitle {{ color: #94a3b8; font-size: 13px; margin-bottom: 24px; }}
  table {{ width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 8px; overflow: hidden; }}
  th {{ background: #334155; padding: 10px 16px; text-align: left; font-size: 12px; text-transform: uppercase; color: #94a3b8; }}
  td {{ padding: 10px 16px; border-top: 1px solid #334155; font-size: 13px; }}
  tr:hover td {{ background: #263044; }}
  a {{ color: #60a5fa; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>{workflow_name} — Run History</h1>
<p class="subtitle">{len(runs)} run(s) recorded</p>
{no_runs_msg}
<table>
  <thead>
    <tr>
      <th>Run ID</th>
      <th>Started</th>
      <th>Duration</th>
      <th>Steps</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>{rows}
  </tbody>
</table>
</body>
</html>
"""


def run_dashboard(workflow_dir: Path) -> int:
    workflow_dir = Path(workflow_dir).resolve()
    manifest_path = workflow_dir / "workflow.json"
    workflow_name = workflow_dir.name
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            workflow_name = manifest.get("workflow_name", workflow_name)
        except (json.JSONDecodeError, OSError):
            pass

    audit_root = workflow_dir / "audit"
    runs = load_all_runs(audit_root)
    html = generate_dashboard_html(runs, workflow_name)

    out_path = workflow_dir / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"[dashboard] Wrote {out_path}  ({len(runs)} run(s))")

    opener = "open" if platform.system() == "Darwin" else "xdg-open"
    try:
        subprocess.run([opener, str(out_path)], check=False)
    except FileNotFoundError:
        pass

    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 1:
        print("Usage: dashboard.py <workflow-dir>")
        return 1
    return run_dashboard(Path(argv[0]))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

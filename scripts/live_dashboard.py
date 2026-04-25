#!/usr/bin/env python3
"""Live streaming dashboard for deterministic workflow runs.

Serves an SSE endpoint that tails the latest run's events.jsonl and pushes
step-state changes to the browser in real time.  No external dependencies.

Usage:
    python3 live_dashboard.py <workflow-dir> [--port 7474]
    # or via run_workflow.py --live [PORT]
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Workflow Live</title>
<style>
  body{font-family:monospace;background:#0f0f0f;color:#d4d4d4;margin:0;padding:20px}
  h1{font-size:1.1rem;color:#888;margin:0 0 16px}
  #grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
  .step{border:1px solid #333;border-radius:6px;padding:10px 12px;transition:all .3s}
  .step .id{font-weight:bold;font-size:.85rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .step .status{font-size:.75rem;margin-top:4px;color:#888}
  .step .dur{font-size:.7rem;color:#555;margin-top:2px}
  .pending  {border-color:#333;background:#161616}
  .running  {border-color:#4a9eff;background:#0d1a2e;animation:pulse 1.2s ease-in-out infinite}
  .complete {border-color:#3fb950;background:#0d1f14}
  .failed   {border-color:#f85149;background:#1f0d0d}
  .skipped  {border-color:#6e40c9;background:#150d2a;opacity:.6}
  .waiting-approval{border-color:#d29922;background:#1f180d}
  @keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(74,158,255,.4)}50%{box-shadow:0 0 8px 2px rgba(74,158,255,.25)}}
  #log{margin-top:20px;font-size:.72rem;color:#555;max-height:200px;overflow-y:auto}
  #log p{margin:2px 0}
  #conn{position:fixed;top:8px;right:12px;font-size:.7rem;color:#555}
</style>
</head>
<body>
<span id="conn">connecting…</span>
<h1>⚙ workflow live — <span id="wf-name">—</span></h1>
<div id="grid"></div>
<div id="log"></div>
<script>
const grid = document.getElementById('grid');
const log  = document.getElementById('log');
const conn = document.getElementById('conn');
const wfn  = document.getElementById('wf-name');
const steps = {};

function card(id) {
  if (!steps[id]) {
    const el = document.createElement('div');
    el.className = 'step pending';
    el.id = 'step-' + id;
    el.innerHTML = '<div class="id">' + id + '</div><div class="status">pending</div><div class="dur"></div>';
    grid.appendChild(el);
    steps[id] = el;
  }
  return steps[id];
}

function setStatus(id, status, extra) {
  const el = card(id);
  el.className = 'step ' + status.replace(/-/g, '-');
  el.querySelector('.status').textContent = status;
  if (extra) el.querySelector('.dur').textContent = extra;
}

function addLog(msg) {
  const p = document.createElement('p');
  p.textContent = new Date().toISOString().slice(11,19) + '  ' + msg;
  log.prepend(p);
  if (log.children.length > 100) log.lastChild.remove();
}

const es = new EventSource('/events');
es.onopen = () => { conn.textContent = '● live'; conn.style.color = '#3fb950'; };
es.onerror = () => { conn.textContent = '○ reconnecting'; conn.style.color = '#f85149'; };
es.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  if (ev.event === 'workflow_start') {
    wfn.textContent = ev.workflow_name || '';
    (ev.steps || []).forEach(id => card(id));
    return;
  }
  const id = ev.step_id;
  if (!id) return;
  if (ev.event === 'step_started') {
    setStatus(id, 'running', 'attempt ' + (ev.attempt || 1));
    addLog('→ ' + id + ' started');
  } else if (ev.event === 'step_completed') {
    const dur = ev.duration_seconds != null ? ev.duration_seconds.toFixed(1) + 's' : '';
    setStatus(id, 'complete', dur);
    addLog('✓ ' + id + (dur ? ' (' + dur + ')' : ''));
  } else if (ev.event === 'step_failed') {
    setStatus(id, 'failed', ev.category || '');
    addLog('✗ ' + id + ' failed' + (ev.category ? ' [' + ev.category + ']' : ''));
  } else if (ev.event === 'step_skipped') {
    setStatus(id, 'skipped', '');
    addLog('↷ ' + id + ' skipped');
  } else if (ev.event === 'approval_required') {
    setStatus(id, 'waiting-approval', '');
    addLog('⏸ ' + id + ' waiting approval');
  } else if (ev.event === 'workflow_complete') {
    conn.textContent = ev.success ? '✓ done' : '✗ failed';
    conn.style.color = ev.success ? '#3fb950' : '#f85149';
    addLog('workflow ' + (ev.success ? 'complete' : 'failed'));
  } else if (ev.event === 'rollback') {
    addLog('↩ rollback ' + id);
  }
};
</script>
</body>
</html>
"""


def _find_latest_run_events(audit_root: Path) -> Path | None:
    """Return the events.jsonl path for the most-recently-created run dir."""
    if not audit_root.exists():
        return None
    run_dirs = sorted(
        (d for d in audit_root.iterdir() if d.is_dir()),
        key=lambda d: d.stat().st_mtime,
    )
    for run_dir in reversed(run_dirs):
        candidate = run_dir / "events.jsonl"
        if candidate.exists():
            return candidate
    return None


class _Handler(BaseHTTPRequestHandler):
    """Serve the dashboard HTML and the SSE event stream."""

    audit_root: Path
    manifest: dict

    def do_GET(self):  # noqa: N802
        if self.path == "/":
            body = _HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/events":
            self._stream_events()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):  # silence access log
        pass

    def _sse(self, payload: dict) -> bytes:
        return ("data: " + json.dumps(payload) + "\n\n").encode()

    def _stream_events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        # Send a synthetic workflow_start event so the browser knows all step IDs.
        manifest = self.manifest
        step_ids = [s["id"] for s in manifest.get("steps", [])]
        try:
            self.wfile.write(
                self._sse(
                    {
                        "event": "workflow_start",
                        "workflow_name": manifest.get("workflow_name", ""),
                        "steps": step_ids,
                    }
                )
            )
            self.wfile.flush()
        except OSError:
            return

        # Tail the latest run's events.jsonl; keep looking if it doesn't exist yet.
        events_path: Path | None = None
        offset = 0
        poll_interval = 0.25  # seconds

        try:
            while True:
                # Rediscover latest run if needed (new run may have started).
                latest = _find_latest_run_events(self.audit_root)
                if latest != events_path:
                    events_path = latest
                    offset = 0

                if events_path is None or not events_path.exists():
                    time.sleep(poll_interval)
                    continue

                text = events_path.read_text(encoding="utf-8", errors="replace")
                lines = text.splitlines()
                for line in lines[offset:]:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self.wfile.write(self._sse(ev))
                    self.wfile.flush()
                offset = len(lines)
                time.sleep(poll_interval)
        except OSError:
            pass  # client disconnected


def serve_live(workflow_dir: Path, port: int = 7474) -> None:
    """Start the live dashboard server (blocks until KeyboardInterrupt)."""
    from run_workflow import build_paths, load_manifest  # local import to avoid circular  # noqa: PLC0415

    paths = build_paths(workflow_dir)
    manifest = load_manifest(paths.manifest_path)

    # Bind handler attributes for the request handler.
    handler = type(
        "_BoundHandler",
        (_Handler,),
        {"audit_root": paths.audit_root, "manifest": manifest},
    )

    httpd = HTTPServer(("127.0.0.1", port), handler)
    wf_name = manifest.get("workflow_name", workflow_dir.name)
    print(f"[live] Dashboard: http://127.0.0.1:{port}/  ({wf_name})", flush=True)
    print(f"[live] Watching: {paths.audit_root}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Live workflow dashboard.")
    parser.add_argument("workflow_dir", help="Workflow directory.")
    parser.add_argument("--port", type=int, default=7474, help="Port to listen on.")
    args = parser.parse_args(argv)
    serve_live(Path(args.workflow_dir).resolve(), port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Generate an n8n-style HTML visualization of a deterministic workflow."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

NODE_W = 220
NODE_H = 84
COL_GAP = 260
ROW_GAP = 110
PADDING = 70
SIDECAR_W = 190
SIDECAR_H = 62
SIDECAR_GAP = 44


def compute_depths(steps: list[dict]) -> dict[str, int]:
    id_set = {s["id"] for s in steps}
    dep_map = {s["id"]: [d for d in (s.get("depends_on") or []) if d in id_set] for s in steps}
    memo: dict[str, int] = {}

    def depth(sid: str, stack: frozenset[str] = frozenset()) -> int:
        if sid in memo:
            return memo[sid]
        if sid in stack:
            return 0
        d = max((depth(d, stack | {sid}) + 1 for d in dep_map.get(sid, [])), default=0)
        memo[sid] = d
        return d

    for s in steps:
        depth(s["id"])
    return memo


def compute_layout(
    steps: list[dict], sidecars: list[dict]
) -> tuple[dict[str, tuple[int, int]], dict[str, tuple[int, int]], int, int]:
    depths = compute_depths(steps)
    cols: dict[int, list[str]] = {}
    for s in steps:
        cols.setdefault(depths[s["id"]], []).append(s["id"])

    max_col = max(depths.values(), default=0)
    max_rows = max((len(v) for v in cols.values()), default=1)
    main_height = max_rows * NODE_H + (max_rows - 1) * ROW_GAP

    positions: dict[str, tuple[int, int]] = {}
    for col_idx, step_ids in cols.items():
        x = PADDING + col_idx * (NODE_W + COL_GAP)
        col_h = len(step_ids) * NODE_H + (len(step_ids) - 1) * ROW_GAP
        start_y = PADDING + (main_height - col_h) // 2
        for row_idx, step_id in enumerate(step_ids):
            positions[step_id] = (x, start_y + row_idx * (NODE_H + ROW_GAP))

    canvas_w = PADDING * 2 + (max_col + 1) * NODE_W + max_col * COL_GAP
    canvas_h = PADDING * 2 + main_height

    # Sidecars: positioned directly below their consumer node
    sidecar_positions: dict[str, tuple[int, int]] = {}
    consumer_idx: dict[str, int] = {}
    for sc in sidecars:
        consumer = sc.get("consumer_step", "")
        if consumer not in positions:
            continue
        idx = consumer_idx.get(consumer, 0)
        cx, cy = positions[consumer]
        sidecar_positions[sc["id"]] = (cx + idx * (SIDECAR_W + 12), cy + NODE_H + SIDECAR_GAP)
        consumer_idx[consumer] = idx + 1

    if sidecar_positions:
        max_sy = max(y + SIDECAR_H + PADDING for _, (_, y) in sidecar_positions.items())
        canvas_h = max(canvas_h, max_sy)

    return positions, sidecar_positions, max(canvas_w, 600), canvas_h


def read_tsv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "\t" in line:
            k, v = line.split("\t", 1)
            result[k.strip()] = v.strip()
    return result


def read_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def generate_html(workflow_dir: Path) -> str:
    manifest_path = workflow_dir / "workflow.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"No workflow.json in {workflow_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    steps = manifest.get("steps", [])
    sidecars = manifest.get("sidecars", [])

    step_status = read_tsv(workflow_dir / "state" / "step-status.tsv")
    metrics_steps = read_json(workflow_dir / "state" / "metrics.json", {}).get("steps", {})
    runtime_steps = read_json(workflow_dir / "state" / "runtime-state.json", {}).get("steps", {})

    # Load pending mutations
    mutations_data = read_json(
        workflow_dir / "state" / "proposed-mutations.json", {"mutations": []}
    )
    pending_mutations = [
        m for m in mutations_data.get("mutations", []) if m.get("status") == "pending"
    ]

    # Build a map: step_id -> list of pending mutations that affect it
    step_pending_mutations: dict[str, list[dict]] = {}
    for m in pending_mutations:
        affected: list[str] = []
        if m["type"] == "add_step":
            after_id = m["payload"].get("after")
            before_id = m["payload"].get("before")
            if after_id:
                affected.append(after_id)
            elif before_id:
                affected.append(before_id)
        elif m["type"] == "modify_step":
            affected.append(m["payload"].get("step_id", ""))
        for sid in affected:
            if sid:
                step_pending_mutations.setdefault(sid, []).append(m)

    positions, sidecar_positions, canvas_w, canvas_h = compute_layout(steps, sidecars)

    def fmt_contract(c: object) -> str:
        if isinstance(c, str):
            return c
        if isinstance(c, dict):
            return c.get("path", str(c))
        return str(c)

    graph_data = {
        "workflow_name": manifest.get("name", workflow_dir.name),
        "policy_pack": manifest.get("policy_pack", "strict-prod"),
        "canvas_w": canvas_w,
        "canvas_h": canvas_h,
        "pending_mutations": [
            {
                "id": m["id"],
                "type": m["type"],
                "description": m["description"],
                "affects": (
                    [m["payload"].get("after") or m["payload"].get("before", "")]
                    if m["type"] == "add_step"
                    else ([m["payload"].get("step_id", "")] if m["type"] == "modify_step" else [])
                ),
            }
            for m in pending_mutations
        ],
        "steps": [
            {
                "id": s["id"],
                "type": s.get("type", "shell"),
                "depends_on": s.get("depends_on") or [],
                "produces": [fmt_contract(c) for c in (s.get("produces") or [])],
                "consumes": [fmt_contract(c) for c in (s.get("consumes") or [])],
                "script": s.get("script") or "",
                "requires_approval": bool(s.get("requires_approval")),
                "success_gate": str(s.get("success_gate") or ""),
                "failure_policy": str(s.get("failure_policy") or ""),
                "retry_limit": int(s.get("retry_limit") or 0),
                "status": step_status.get(s["id"], "pending"),
                "duration_seconds": metrics_steps.get(s["id"], {}).get("last_duration_seconds"),
                "runs": metrics_steps.get(s["id"], {}).get("runs", 0),
                "failures": metrics_steps.get(s["id"], {}).get("failures", 0),
                "last_error": runtime_steps.get(s["id"], {}).get("last_error") or "",
                "pending_mutations": [
                    {"id": m["id"], "description": m["description"]}
                    for m in step_pending_mutations.get(s["id"], [])
                ],
                "x": positions[s["id"]][0],
                "y": positions[s["id"]][1],
            }
            for s in steps
            if s["id"] in positions
        ],
        "sidecars": [
            {
                "id": sc["id"],
                "kind": sc.get("kind", ""),
                "consumer_step": sc.get("consumer_step", ""),
                "when": sc.get("when", ""),
                "purpose": sc.get("purpose", ""),
                "containment": (sc.get("containment") or {}).get("mode", ""),
                "x": sidecar_positions[sc["id"]][0],
                "y": sidecar_positions[sc["id"]][1],
            }
            for sc in sidecars
            if sc["id"] in sidecar_positions
        ],
    }

    return HTML_TEMPLATE.replace("__GRAPH_DATA__", json.dumps(graph_data, indent=2))


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Workflow Visualizer</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0f0f17; color: #e2e8f0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  overflow: hidden; height: 100vh;
  display: flex; flex-direction: column;
}

/* ── Toolbar ──────────────────────────────────────────────── */
#toolbar {
  background: #13131f; border-bottom: 1px solid #1e1e30;
  padding: 0 16px; display: flex; align-items: center; gap: 10px;
  flex-shrink: 0; height: 46px; z-index: 10;
}
#wf-title { font-size: 14px; font-weight: 700; color: #e2e8f0; white-space: nowrap; }
.badge {
  font-size: 10px; background: #1e1e30; color: #64748b;
  padding: 2px 8px; border-radius: 999px; border: 1px solid #2d2d4a;
  white-space: nowrap;
}
#live-dot {
  width: 7px; height: 7px; border-radius: 50%; background: #22c55e;
  flex-shrink: 0; transition: background 0.3s;
}
#live-dot.offline { background: #4b5563; }
#live-label { font-size: 11px; color: #64748b; white-space: nowrap; }

#progress-wrap {
  display: flex; align-items: center; gap: 8px;
  background: #1e1e30; border: 1px solid #2d2d4a;
  border-radius: 6px; padding: 4px 10px; flex-shrink: 0;
}
#progress-track {
  width: 80px; height: 4px; background: #2d2d4a; border-radius: 2px; overflow: hidden;
}
#progress-fill { height: 100%; background: #22c55e; border-radius: 2px; transition: width 0.4s; }
#progress-text { font-size: 11px; color: #94a3b8; white-space: nowrap; }

#search-wrap { position: relative; }
#search-input {
  background: #1e1e30; border: 1px solid #2d2d4a; color: #e2e8f0;
  border-radius: 6px; padding: 4px 8px 4px 28px;
  font-size: 12px; outline: none; width: 150px; transition: border-color 0.15s;
}
#search-input:focus { border-color: #3b82f6; }
#search-input::placeholder { color: #475569; }
#search-icon {
  position: absolute; left: 8px; top: 50%; transform: translateY(-50%);
  color: #475569; font-size: 12px; pointer-events: none;
}

.spacer { flex: 1; min-width: 0; }

#type-legend {
  display: flex; align-items: center; gap: 5px; flex-shrink: 0;
}
.tl-pill {
  font-size: 10px; font-weight: 600; padding: 2px 7px;
  border-radius: 999px; border: 1px solid; white-space: nowrap;
  letter-spacing: 0.02em;
}

#btn-export, #btn-fit, #btn-reset {
  background: #1e1e30; color: #94a3b8; border: 1px solid #2d2d4a;
  border-radius: 6px; padding: 4px 10px; font-size: 11px;
  cursor: pointer; transition: background 0.15s, color 0.15s; white-space: nowrap;
}
#btn-export:hover, #btn-fit:hover, #btn-reset:hover {
  background: #2d2d4a; color: #e2e8f0;
}

/* ── Canvas ───────────────────────────────────────────────── */
#canvas-wrap {
  flex: 1; position: relative; overflow: hidden; cursor: grab;
}
#canvas-wrap.dragging { cursor: grabbing; }
#svg-canvas { position: absolute; top: 0; left: 0; width: 100%; height: 100%; }

/* ── Minimap ──────────────────────────────────────────────── */
#minimap {
  position: absolute; bottom: 16px; right: 16px;
  background: #13131f; border: 1px solid #2d2d4a; border-radius: 8px;
  padding: 6px; z-index: 6; cursor: pointer; box-shadow: 0 4px 20px rgba(0,0,0,0.5);
}
#minimap-canvas { display: block; border-radius: 4px; }
#minimap-label {
  font-size: 9px; color: #475569; text-align: center;
  margin-top: 4px; letter-spacing: 0.06em; text-transform: uppercase;
}

/* ── Tooltip ──────────────────────────────────────────────── */
#tooltip {
  position: fixed; display: none; z-index: 100;
  background: #1e1e30; border: 1px solid #3d3d5c;
  border-radius: 8px; padding: 8px 12px; pointer-events: none;
  box-shadow: 0 8px 24px rgba(0,0,0,0.6); max-width: 240px;
}
#tt-id { font-size: 13px; font-weight: 700; color: #e2e8f0; margin-bottom: 2px; }
#tt-meta { font-size: 11px; color: #64748b; display: flex; gap: 8px; }
#tt-status-dot {
  display: inline-block; width: 7px; height: 7px;
  border-radius: 50%; margin-right: 4px; vertical-align: middle;
}

/* ── Inspector ────────────────────────────────────────────── */
#inspector {
  position: absolute; top: 0; right: 0; width: 300px; height: 100%;
  background: #13131f; border-left: 1px solid #1e1e30;
  padding: 16px; overflow-y: auto;
  transform: translateX(100%); transition: transform 0.2s ease; z-index: 5;
}
#inspector.open { transform: translateX(0); }
#insp-header {
  display: flex; align-items: flex-start; gap: 8px; margin-bottom: 12px;
}
#insp-title-wrap { flex: 1; min-width: 0; }
#insp-id {
  font-size: 14px; font-weight: 700; color: #e2e8f0;
  word-break: break-all; line-height: 1.3;
}
.type-pill {
  display: inline-block; font-size: 10px; font-weight: 600;
  padding: 2px 8px; border-radius: 999px; letter-spacing: 0.04em;
  text-transform: uppercase; margin-top: 4px;
}
.close-btn {
  background: none; border: none; color: #4b5563;
  cursor: pointer; font-size: 16px; padding: 2px; flex-shrink: 0;
  transition: color 0.15s;
}
.close-btn:hover { color: #e2e8f0; }

.status-row {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 10px; background: #0f0f17;
  border: 1px solid #1e1e30; border-radius: 6px; margin-bottom: 12px;
}
.status-dot-lg { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
.status-text { font-size: 12px; font-weight: 600; }
.status-meta { font-size: 11px; color: #475569; margin-left: auto; }

.insp-section { margin-bottom: 12px; }
.insp-label {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.08em; color: #475569; margin-bottom: 5px;
}
.insp-value {
  font-size: 12px; color: #cbd5e1; background: #0f0f17;
  border: 1px solid #1e1e30; border-radius: 6px; padding: 7px 10px;
  word-break: break-all; line-height: 1.5;
}
.insp-value.mono { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 11px; }
.insp-value.muted { color: #475569; font-style: italic; }
.insp-value.error { color: #f87171; border-color: #ef444422; background: #1a0f0f; }
.insp-list { display: flex; flex-direction: column; gap: 3px; }
.insp-chip {
  font-size: 11px; color: #94a3b8; background: #0f0f17;
  border: 1px solid #1e1e30; border-radius: 4px; padding: 4px 8px;
  font-family: 'SF Mono', 'Fira Code', monospace;
}
.insp-stats {
  display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 12px;
}
.stat-box {
  background: #0f0f17; border: 1px solid #1e1e30; border-radius: 6px;
  padding: 8px 10px; text-align: center;
}
.stat-value { font-size: 20px; font-weight: 700; color: #e2e8f0; }
.stat-label { font-size: 10px; color: #475569; margin-top: 1px; text-transform: uppercase; letter-spacing: 0.05em; }

.kbd {
  display: inline-block; background: #1e1e30; border: 1px solid #2d2d4a;
  border-radius: 4px; padding: 1px 5px; font-size: 10px;
  font-family: monospace; color: #64748b;
}

@keyframes pulse-dot { 0%,100% { opacity:1; } 50% { opacity:0.3; } }
.running-pulse { animation: pulse-dot 1.4s ease-in-out infinite; }
@keyframes fadeIn { from { opacity:0; } to { opacity:1; } }
.fade-in { animation: fadeIn 0.15s ease forwards; }
@keyframes pulse-amber { 0%,100% { opacity:0.8; } 50% { opacity:0.2; } }
.mutation-ring { animation: pulse-amber 1.8s ease-in-out infinite; }

/* ── Keyboard shortcuts overlay ──────────────────────────── */
#shortcuts {
  position: absolute; bottom: 16px; left: 16px; z-index: 6;
  display: flex; flex-direction: column; gap: 4px; pointer-events: none;
}
.shortcut-row { display: flex; align-items: center; gap: 6px; font-size: 10px; color: #334155; }
</style>
</head>
<body>

<div id="toolbar">
  <span id="wf-title">Workflow</span>
  <span class="badge" id="policy-label">strict-prod</span>
  <div id="progress-wrap">
    <div id="progress-track"><div id="progress-fill" style="width:0%"></div></div>
    <span id="progress-text">loading…</span>
  </div>
  <div id="live-wrap" style="display:flex;align-items:center;gap:5px;">
    <div id="live-dot" class="offline"></div>
    <span id="live-label">static</span>
  </div>
  <div id="search-wrap">
    <span id="search-icon">⌕</span>
    <input id="search-input" type="text" placeholder="Filter steps…" autocomplete="off">
  </div>
  <div class="spacer"></div>
  <div id="type-legend"></div>
  <button id="btn-export">Export SVG</button>
  <button id="btn-fit">Fit <span class="kbd">F</span></button>
  <button id="btn-reset">Reset</button>
</div>

<div id="canvas-wrap">
  <svg id="svg-canvas" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <pattern id="dots" x="0" y="0" width="28" height="28" patternUnits="userSpaceOnUse">
        <circle cx="1" cy="1" r="1" fill="#161622"/>
      </pattern>
      <marker id="arrow" markerWidth="8" markerHeight="7" refX="7" refY="3.5" orient="auto">
        <path d="M0,0 L0,7 L8,3.5 z" fill="#3d3d5c"/>
      </marker>
      <marker id="arrow-active" markerWidth="8" markerHeight="7" refX="7" refY="3.5" orient="auto">
        <path d="M0,0 L0,7 L8,3.5 z" fill="#22c55e"/>
      </marker>
      <marker id="arrow-sc" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
        <path d="M0,0 L0,7 L7,3.5 z" fill="#8b5cf655"/>
      </marker>
      <filter id="glow" x="-40%" y="-40%" width="180%" height="180%">
        <feGaussianBlur stdDeviation="4" result="blur"/>
        <feComposite in="SourceGraphic" in2="blur" operator="over"/>
      </filter>
      <filter id="shadow">
        <feDropShadow dx="0" dy="3" stdDeviation="5" flood-color="#000" flood-opacity="0.45"/>
      </filter>
    </defs>
    <rect width="100%" height="100%" fill="url(#dots)"/>
    <g id="viewport">
      <g id="edges-layer"></g>
      <g id="nodes-layer"></g>
    </g>
  </svg>

  <div id="minimap">
    <canvas id="minimap-canvas" width="160" height="90"></canvas>
    <div id="minimap-label">Minimap</div>
  </div>

  <div id="shortcuts">
    <div class="shortcut-row"><span class="kbd">F</span> fit · <span class="kbd">/</span> search · <span class="kbd">Esc</span> close</div>
  </div>
</div>

<div id="inspector">
  <div id="insp-header">
    <div id="insp-title-wrap">
      <div id="insp-id"></div>
      <span id="insp-type-pill" class="type-pill"></span>
    </div>
    <button class="close-btn" id="insp-close" title="Close (Esc)">✕</button>
  </div>
  <div class="status-row">
    <div class="status-dot-lg" id="insp-status-dot"></div>
    <span class="status-text" id="insp-status-text"></span>
    <span class="status-meta" id="insp-duration"></span>
  </div>
  <div class="insp-stats" id="insp-stats" style="display:none">
    <div class="stat-box"><div class="stat-value" id="stat-runs">0</div><div class="stat-label">Runs</div></div>
    <div class="stat-box"><div class="stat-value" id="stat-failures">0</div><div class="stat-label">Failures</div></div>
  </div>
  <div class="insp-section" id="sec-error" style="display:none">
    <div class="insp-label">Last Error</div>
    <div class="insp-value error mono" id="insp-error"></div>
  </div>
  <div class="insp-section" id="sec-script" style="display:none">
    <div class="insp-label">Script</div>
    <div class="insp-value mono" id="insp-script"></div>
  </div>
  <div class="insp-section" id="sec-gate" style="display:none">
    <div class="insp-label">Success Gate</div>
    <div class="insp-value mono" id="insp-gate"></div>
  </div>
  <div class="insp-section" id="sec-fp" style="display:none">
    <div class="insp-label">Failure Policy</div>
    <div class="insp-value" id="insp-fp"></div>
  </div>
  <div class="insp-section" id="sec-retry" style="display:none">
    <div class="insp-label">Retry Limit</div>
    <div class="insp-value" id="insp-retry"></div>
  </div>
  <div class="insp-section" id="sec-produces" style="display:none">
    <div class="insp-label">Produces</div>
    <div class="insp-list" id="insp-produces"></div>
  </div>
  <div class="insp-section" id="sec-consumes" style="display:none">
    <div class="insp-label">Consumes</div>
    <div class="insp-list" id="insp-consumes"></div>
  </div>
  <div class="insp-section" id="sec-deps" style="display:none">
    <div class="insp-label">Depends On</div>
    <div class="insp-list" id="insp-deps"></div>
  </div>
  <div class="insp-section" id="sec-mutations" style="display:none">
    <div class="insp-label" style="color:#f59e0b">Pending Mutations</div>
    <div class="insp-list" id="insp-mutations"></div>
  </div>
</div>

<div id="tooltip">
  <div id="tt-id"></div>
  <div id="tt-meta">
    <span><span id="tt-status-dot" class="status-dot-lg" style="display:inline-block;width:7px;height:7px;border-radius:50%;vertical-align:middle;margin-right:3px;"></span><span id="tt-status"></span></span>
    <span id="tt-type" style="color:#4b5563"></span>
  </div>
</div>

<script>
const GRAPH = __GRAPH_DATA__;

const NW = 220, NH = 84, SCW = 190, SCH = 62;

const STATUS_COLOR = {
  complete:          '#22c55e',
  running:           '#3b82f6',
  failed:            '#ef4444',
  'waiting-approval':'#f59e0b',
  interrupted:       '#f97316',
  pending:           '#334155',
};
const STATUS_BG = {
  complete:          '#22c55e18',
  running:           '#3b82f618',
  failed:            '#ef444418',
  'waiting-approval':'#f59e0b18',
  interrupted:       '#f9731618',
  pending:           '',
};
const TYPE_COLOR = {
  shell:            '#3b82f6', test:             '#10b981',
  'manual-approval':'#f59e0b', 'sidecar-consume':'#8b5cf6',
  python:           '#06b6d4', transform:        '#3b82f6',
  publish:          '#ef4444', approval:         '#f59e0b',
  'file-exists':    '#94a3b8', 'json-validate':  '#a78bfa',
  copy:             '#94a3b8', 'http-check':     '#fb923c',
  'git-diff-check': '#34d399',
};
const TYPE_ICON = {
  shell:            '⚙',  test:             '✓',
  'manual-approval':'⏸',  'sidecar-consume':'◈',
  python:           '⬡',  transform:        '⟳',
  publish:          '⬆',  approval:         '⏸',
  'file-exists':    '📄', 'json-validate':  '{}',
  copy:             '⧉',  'http-check':     '↗',
  'git-diff-check': '⑂',
};
const TYPE_DISPLAY = {
  shell:'shell', test:'test', 'manual-approval':'approval', 'sidecar-consume':'sidecar',
  python:'python', transform:'transform', publish:'publish', 'file-exists':'file-exists',
  'json-validate':'json-validate', copy:'copy', 'http-check':'http-check',
  'git-diff-check':'git-diff',
};

const svg    = document.getElementById('svg-canvas');
const viewport = document.getElementById('viewport');
const edgesLayer = document.getElementById('edges-layer');
const nodesLayer = document.getElementById('nodes-layer');
const wrap   = document.getElementById('canvas-wrap');

let vx = 0, vy = 0, vscale = 1;
function applyTransform() {
  viewport.setAttribute('transform', `translate(${vx},${vy}) scale(${vscale})`);
  drawMinimap();
}

function svgEl(tag, attrs) {
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k, v] of Object.entries(attrs || {})) el.setAttribute(k, v);
  return el;
}

function sc(s)  { return STATUS_COLOR[s] || '#334155'; }
function tc(t)  { return TYPE_COLOR[t]   || '#64748b'; }
function ti(t)  { return TYPE_ICON[t]    || '▣'; }

const stepById = {};
GRAPH.steps.forEach(s => { stepById[s.id] = s; });

// ── Type legend ────────────────────────────────────────────
(function buildLegend() {
  const seen = new Set();
  const legend = document.getElementById('type-legend');
  GRAPH.steps.forEach(s => {
    if (seen.has(s.type)) return;
    seen.add(s.type);
    const pill = document.createElement('span');
    pill.className = 'tl-pill';
    pill.textContent = TYPE_DISPLAY[s.type] || s.type;
    pill.style.cssText = `background:${tc(s.type)}18;border-color:${tc(s.type)}55;color:${tc(s.type)}`;
    legend.appendChild(pill);
  });
  if (GRAPH.sidecars.length) {
    const pill = document.createElement('span');
    pill.className = 'tl-pill';
    pill.textContent = 'sidecar';
    pill.style.cssText = `background:#8b5cf618;border-color:#8b5cf655;color:#8b5cf6`;
    legend.appendChild(pill);
  }
})();

// ── Progress bar ──────────────────────────────────────────
function updateProgress() {
  const total   = GRAPH.steps.length;
  const done    = GRAPH.steps.filter(s => s.status === 'complete').length;
  const failed  = GRAPH.steps.filter(s => s.status === 'failed').length;
  const waiting = GRAPH.steps.filter(s => s.status === 'waiting-approval').length;
  const running = GRAPH.steps.filter(s => s.status === 'running').length;
  const pct     = total ? (done / total * 100) : 0;

  document.getElementById('progress-fill').style.width = pct + '%';
  document.getElementById('progress-fill').style.background =
    failed ? '#ef4444' : waiting ? '#f59e0b' : '#22c55e';

  const parts = [`${done}/${total} complete`];
  if (running)  parts.push(`${running} running`);
  if (waiting)  parts.push(`${waiting} approval`);
  if (failed)   parts.push(`${failed} failed`);
  document.getElementById('progress-text').textContent = parts.join(' · ');
}

// ── Bezier edges ──────────────────────────────────────────
function bezierPath(x1, y1, x2, y2) {
  const dx = Math.max(Math.abs(x2 - x1) * 0.48, 80);
  return `M ${x1} ${y1} C ${x1+dx} ${y1}, ${x2-dx} ${y2}, ${x2} ${y2}`;
}

function renderEdge(from, to) {
  const x1 = from.x + NW, y1 = from.y + NH / 2;
  const x2 = to.x,        y2 = to.y + NH / 2;
  const active = from.status === 'complete' && to.status !== 'pending';
  const color = active ? (sc(to.status) === '#334155' ? '#22c55e' : sc(to.status)) : '#1e2030';
  const markerId = active ? 'arrow-active' : 'arrow';

  edgesLayer.appendChild(svgEl('path', {
    d: bezierPath(x1, y1, x2, y2), fill: 'none',
    stroke: color, 'stroke-width': active ? '2' : '1.5',
    'marker-end': `url(#${markerId})`, opacity: active ? '1' : '0.5',
    class: `edge-from-${from.id}`,
  }));
}

function renderSidecarEdge(consumer, sc_node) {
  const x1 = consumer.x + NW / 2, y1 = consumer.y + NH;
  const x2 = sc_node.x + SCW / 2, y2 = sc_node.y;
  edgesLayer.appendChild(svgEl('path', {
    d: `M ${x1} ${y1} C ${x1} ${y1+20}, ${x2} ${y2-20}, ${x2} ${y2}`,
    fill: 'none', stroke: '#8b5cf644', 'stroke-width': '1.5',
    'stroke-dasharray': '4 3', 'marker-end': 'url(#arrow-sc)',
  }));
}

// ── Step nodes ─────────────────────────────────────────────
const nodeEls = {};

function renderNode(step) {
  const color  = tc(step.type);
  const scolor = sc(step.status);
  const isRun  = step.status === 'running';
  const isFail = step.status === 'failed';
  const isOk   = step.status === 'complete';

  const g = svgEl('g', {
    transform: `translate(${step.x},${step.y})`,
    class: 'step-node', style: 'cursor:pointer',
    'data-id': step.id,
  });

  // Drop shadow / glow
  const body = svgEl('rect', {
    x:0, y:0, width:NW, height:NH, rx:9, ry:9,
    fill: STATUS_BG[step.status] || '#16162a',
    stroke: isRun ? '#3b82f6' : isFail ? '#ef4444' : isOk ? '#22c55e55' : '#1e2030',
    'stroke-width': (isRun || isFail) ? '2' : '1',
    filter: (isOk || isFail) ? 'url(#glow)' : 'url(#shadow)',
    class: isRun ? 'running-pulse' : '',
  });
  g.appendChild(body);

  // Color stripe top
  g.appendChild(svgEl('rect', { x:0,y:0, width:NW, height:4, rx:9, fill:color }));
  g.appendChild(svgEl('rect', { x:0,y:2, width:NW, height:2, fill:color }));

  // Icon circle
  const ic = svgEl('circle', { cx:22, cy:NH/2+2, r:14, fill:color+'1a', stroke:color+'44', 'stroke-width':'1' });
  const it = svgEl('text', { x:22, y:NH/2+7, 'text-anchor':'middle', 'font-size':'14', fill:color });
  it.textContent = ti(step.type);
  g.appendChild(ic); g.appendChild(it);

  // Step ID label
  const maxChars = step.requires_approval ? 12 : 15;
  const label = step.id.length > maxChars ? step.id.slice(0, maxChars-1)+'…' : step.id;
  const idT = svgEl('text', { x:44, y:NH/2-4, 'font-size':'13', 'font-weight':'600', fill:'#e2e8f0' });
  idT.textContent = label;
  g.appendChild(idT);

  // Type sublabel
  const typeT = svgEl('text', { x:44, y:NH/2+12, 'font-size':'10', fill:'#475569', 'letter-spacing':'0.03em' });
  typeT.textContent = step.type;
  g.appendChild(typeT);

  // Duration badge (bottom-right if available)
  if (step.duration_seconds != null && step.duration_seconds > 0) {
    const dur = step.duration_seconds >= 60
      ? `${Math.round(step.duration_seconds/60)}m`
      : `${step.duration_seconds.toFixed(1)}s`;
    const dt = svgEl('text', { x:NW-8, y:NH-8, 'text-anchor':'end', 'font-size':'9', fill:'#334155' });
    dt.textContent = dur;
    g.appendChild(dt);
  }

  // Approval GATE badge
  if (step.requires_approval) {
    const br = svgEl('rect', { x:NW-48, y:NH/2-10, width:40, height:18, rx:9, fill:'#f59e0b1a', stroke:'#f59e0b44', 'stroke-width':'1' });
    const bt = svgEl('text', { x:NW-28, y:NH/2+4, 'text-anchor':'middle', 'font-size':'9', 'font-weight':'700', fill:'#f59e0b', 'letter-spacing':'0.06em' });
    bt.textContent = 'GATE';
    g.appendChild(br); g.appendChild(bt);
  }

  // Pending mutation amber ring
  if (step.pending_mutations && step.pending_mutations.length > 0) {
    const ring = svgEl('circle', {
      cx: NW/2, cy: NH/2, r: String(NH/2 + 6),
      fill: 'none', stroke: '#f59e0b', 'stroke-width': '2', opacity: '0.8',
      class: 'mutation-ring',
    });
    g.appendChild(ring);
    // Mutation count badge top-left
    const mbg = svgEl('rect', { x: 2, y: 2, width: 28, height: 16, rx: 8, fill: '#f59e0b22', stroke: '#f59e0b55', 'stroke-width': '1' });
    const mt = svgEl('text', { x: 16, y: 14, 'text-anchor': 'middle', 'font-size': '9', 'font-weight': '700', fill: '#f59e0b' });
    mt.textContent = '\u26A1 ' + step.pending_mutations.length;
    g.appendChild(mbg); g.appendChild(mt);
  }

  // Status dot top-right
  const dot = svgEl('circle', { cx:NW-10, cy:12, r:5, fill:scolor, class:isRun?'running-pulse':'' });
  g.appendChild(dot);

  // Tooltip events
  const tooltip = document.getElementById('tooltip');
  g.addEventListener('mousemove', e => {
    tooltip.style.display = 'block';
    const x = e.clientX + 14, y = e.clientY + 14;
    tooltip.style.left = Math.min(x, window.innerWidth-260) + 'px';
    tooltip.style.top  = Math.min(y, window.innerHeight-80)  + 'px';
    document.getElementById('tt-id').textContent = step.id;
    const sd = document.getElementById('tt-status-dot');
    sd.style.background = scolor;
    document.getElementById('tt-status').textContent = step.status;
    document.getElementById('tt-type').textContent = step.type;
  });
  g.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

  // Click → inspector
  g.addEventListener('click', e => { e.stopPropagation(); showInspector(step); });

  // Hover highlight
  g.addEventListener('mouseenter', () => body.setAttribute('stroke', color));
  g.addEventListener('mouseleave', () => {
    body.setAttribute('stroke', isRun?'#3b82f6':isFail?'#ef4444':isOk?'#22c55e55':'#1e2030');
  });

  nodeEls[step.id] = g;
  nodesLayer.appendChild(g);
}

function renderSidecar(sc) {
  const g = svgEl('g', { transform:`translate(${sc.x},${sc.y})`, style:'cursor:default' });
  g.appendChild(svgEl('rect', { x:0,y:0, width:SCW, height:SCH, rx:7,
    fill:'#18122a', stroke:'#8b5cf633', 'stroke-width':'1.5', 'stroke-dasharray':'5 3' }));
  g.appendChild(svgEl('rect', { x:0,y:0, width:SCW, height:3, rx:7, fill:'#8b5cf6' }));
  g.appendChild(svgEl('rect', { x:0,y:1, width:SCW, height:2, fill:'#8b5cf6' }));

  // Icon
  const ic = svgEl('circle', { cx:16, cy:SCH/2+2, r:10, fill:'#8b5cf61a', stroke:'#8b5cf633', 'stroke-width':'1' });
  const it = svgEl('text', { x:16, y:SCH/2+6, 'text-anchor':'middle', 'font-size':'11', fill:'#8b5cf6' });
  it.textContent = '◈';
  g.appendChild(ic); g.appendChild(it);

  const label = sc.id.length > 18 ? sc.id.slice(0,17)+'…' : sc.id;
  const lt = svgEl('text', { x:33, y:SCH/2-3, 'font-size':'11', 'font-weight':'600', fill:'#a78bfa' });
  lt.textContent = label;
  g.appendChild(lt);

  const kt = svgEl('text', { x:33, y:SCH/2+11, 'font-size':'9', fill:'#6b5b8f' });
  kt.textContent = `${sc.kind}  ·  ${sc.when}`;
  g.appendChild(kt);

  const tooltip = document.getElementById('tooltip');
  g.addEventListener('mousemove', e => {
    tooltip.style.display = 'block';
    tooltip.style.left = (e.clientX+14)+'px';
    tooltip.style.top  = (e.clientY+14)+'px';
    document.getElementById('tt-id').textContent = sc.id;
    document.getElementById('tt-status-dot').style.background = '#8b5cf6';
    document.getElementById('tt-status').textContent = sc.when;
    document.getElementById('tt-type').textContent = sc.kind;
  });
  g.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
  nodesLayer.appendChild(g);
}

// ── Inspector ─────────────────────────────────────────────
function showInspector(step) {
  document.getElementById('insp-id').textContent = step.id;
  const pill = document.getElementById('insp-type-pill');
  pill.textContent = step.type;
  pill.style.cssText = `background:${tc(step.type)}22;color:${tc(step.type)};border:1px solid ${tc(step.type)}44`;

  const dot = document.getElementById('insp-status-dot');
  dot.style.background = sc(step.status);
  const stT = document.getElementById('insp-status-text');
  stT.textContent = step.status; stT.style.color = sc(step.status);

  const dur = document.getElementById('insp-duration');
  dur.textContent = step.duration_seconds > 0
    ? (step.duration_seconds >= 60 ? `${(step.duration_seconds/60).toFixed(1)}m` : `${step.duration_seconds.toFixed(2)}s`)
    : '';

  // Stats
  const statsEl = document.getElementById('insp-stats');
  if (step.runs > 0) {
    statsEl.style.display = 'grid';
    document.getElementById('stat-runs').textContent = step.runs;
    const failEl = document.getElementById('stat-failures');
    failEl.textContent = step.failures;
    failEl.style.color = step.failures > 0 ? '#ef4444' : '#e2e8f0';
  } else { statsEl.style.display = 'none'; }

  function sec(id, val, mono, cls='') {
    const s = document.getElementById('sec-' + id);
    const v = document.getElementById('insp-' + id);
    if (val) {
      s.style.display = ''; v.textContent = val;
      v.className = 'insp-value' + (mono ? ' mono' : '') + (cls ? ' '+cls : '');
    } else { s.style.display = 'none'; }
  }

  sec('error',  step.last_error, true, 'error');
  sec('script', step.script,     true);
  sec('gate',   step.success_gate, true);
  sec('fp',     step.failure_policy, false);
  sec('retry',  step.retry_limit > 0 ? `${step.retry_limit} retries allowed` : '', false);

  function secList(id, items) {
    const s = document.getElementById('sec-' + id);
    const el = document.getElementById('insp-' + id);
    el.innerHTML = '';
    if (items && items.length) {
      s.style.display = '';
      items.forEach(item => { const d=document.createElement('div'); d.className='insp-chip'; d.textContent=item; el.appendChild(d); });
    } else { s.style.display = 'none'; }
  }

  secList('produces', step.produces);
  secList('consumes', step.consumes);
  secList('deps', step.depends_on);

  // Pending mutations section
  const mutSec = document.getElementById('sec-mutations');
  const mutList = document.getElementById('insp-mutations');
  mutList.innerHTML = '';
  if (step.pending_mutations && step.pending_mutations.length > 0) {
    mutSec.style.display = '';
    step.pending_mutations.forEach(m => {
      const d = document.createElement('div');
      d.className = 'insp-chip';
      d.style.cssText = 'border-color:#f59e0b44;color:#f59e0b;';
      d.textContent = '\u26A1 [' + m.id + '] ' + m.description;
      mutList.appendChild(d);
    });
  } else { mutSec.style.display = 'none'; }

  document.getElementById('inspector').classList.add('open');
  document.getElementById('inspector').classList.add('fade-in');
}

document.getElementById('insp-close').addEventListener('click', closeInspector);
wrap.addEventListener('click', e => { if (!e.target.closest('.step-node') && !e.target.closest('#inspector')) closeInspector(); });
function closeInspector() {
  const insp = document.getElementById('inspector');
  insp.classList.remove('open');
  insp.classList.remove('fade-in');
}

// ── Zoom / Pan ────────────────────────────────────────────
let dragging=false, dragX=0, dragY=0, dragVX=0, dragVY=0;
wrap.addEventListener('mousedown', e => {
  if (e.target.closest('.step-node')) return;
  dragging=true; dragX=e.clientX; dragY=e.clientY; dragVX=vx; dragVY=vy;
  wrap.classList.add('dragging');
});
window.addEventListener('mousemove', e => {
  if (!dragging) return;
  vx = dragVX + (e.clientX - dragX);
  vy = dragVY + (e.clientY - dragY);
  applyTransform();
});
window.addEventListener('mouseup', () => { dragging=false; wrap.classList.remove('dragging'); });
wrap.addEventListener('wheel', e => {
  e.preventDefault();
  const r = wrap.getBoundingClientRect();
  const mx = e.clientX - r.left, my = e.clientY - r.top;
  const delta = e.deltaY > 0 ? 0.88 : 1.12;
  const ns = Math.min(Math.max(vscale * delta, 0.15), 5);
  vx = mx - (mx - vx) * (ns / vscale);
  vy = my - (my - vy) * (ns / vscale);
  vscale = ns; applyTransform();
}, { passive: false });

function fitToScreen() {
  const r = wrap.getBoundingClientRect();
  const inspOpen = document.getElementById('inspector').classList.contains('open');
  const uw = r.width - (inspOpen ? 316 : 0) - 80;
  const uh = r.height - 80;
  vscale = Math.min(uw / GRAPH.canvas_w, uh / GRAPH.canvas_h, 1);
  vx = (uw - GRAPH.canvas_w * vscale) / 2 + 40;
  vy = (r.height - GRAPH.canvas_h * vscale) / 2;
  applyTransform();
}

document.getElementById('btn-fit').addEventListener('click', fitToScreen);
document.getElementById('btn-reset').addEventListener('click', () => { vx=50; vy=50; vscale=1; applyTransform(); });

// ── Keyboard shortcuts ────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.target.matches('input')) return;
  if (e.key === 'Escape') closeInspector();
  if (e.key === 'f' || e.key === 'F') fitToScreen();
  if (e.key === '/') { e.preventDefault(); document.getElementById('search-input').focus(); }
});

// ── Search / filter ───────────────────────────────────────
document.getElementById('search-input').addEventListener('input', e => {
  const q = e.target.value.toLowerCase().trim();
  Object.values(nodeEls).forEach(g => {
    const id = g.getAttribute('data-id');
    const step = stepById[id];
    const match = !q || id.toLowerCase().includes(q)
      || (step && step.type.toLowerCase().includes(q))
      || (step && step.status.toLowerCase().includes(q));
    g.style.opacity = match ? '1' : '0.12';
  });
});
document.getElementById('search-input').addEventListener('keydown', e => {
  if (e.key === 'Escape') { e.target.value=''; e.target.blur(); Object.values(nodeEls).forEach(g => g.style.opacity='1'); }
});

// ── Export SVG ────────────────────────────────────────────
document.getElementById('btn-export').addEventListener('click', () => {
  const el = document.getElementById('svg-canvas');
  const ser = new XMLSerializer();
  let str = ser.serializeToString(el);
  const blob = new Blob([str], { type:'image/svg+xml' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href=url; a.download=GRAPH.workflow_name+'.svg'; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});

// ── Minimap ───────────────────────────────────────────────
const mcanvas = document.getElementById('minimap-canvas');
const mctx = mcanvas.getContext('2d');
const MW = 160, MH = 90;
const mScaleX = MW / GRAPH.canvas_w;
const mScaleY = MH / GRAPH.canvas_h;
const mScale  = Math.min(mScaleX, mScaleY);

function drawMinimap() {
  mctx.clearRect(0,0,MW,MH);
  mctx.fillStyle='#0f0f17'; mctx.fillRect(0,0,MW,MH);

  GRAPH.steps.forEach(s => {
    mctx.fillStyle = STATUS_COLOR[s.status] || '#334155';
    mctx.globalAlpha = 0.8;
    mctx.fillRect(s.x*mScale, s.y*mScale, Math.max(NW*mScale,2), Math.max(NH*mScale,1.5));
  });
  GRAPH.sidecars.forEach(sc => {
    mctx.fillStyle = '#8b5cf6';
    mctx.globalAlpha = 0.4;
    mctx.fillRect(sc.x*mScale, sc.y*mScale, Math.max(SCW*mScale,2), Math.max(SCH*mScale,1));
  });
  mctx.globalAlpha = 1;

  // Viewport rect
  const r = wrap.getBoundingClientRect();
  const viewX = -vx / vscale, viewY = -vy / vscale;
  const vw = r.width / vscale, vh = r.height / vscale;
  mctx.strokeStyle = '#6b7280';
  mctx.lineWidth = 1;
  mctx.strokeRect(viewX*mScale, viewY*mScale, vw*mScale, vh*mScale);
}

mcanvas.addEventListener('click', e => {
  const r = mcanvas.getBoundingClientRect();
  const mx = (e.clientX - r.left) / mScale;
  const my = (e.clientY - r.top)  / mScale;
  const wr = wrap.getBoundingClientRect();
  vx = wr.width/2  - mx * vscale;
  vy = wr.height/2 - my * vscale;
  applyTransform();
});

// ── Auto-refresh status ───────────────────────────────────
async function refreshStatus() {
  try {
    const resp = await fetch('state/step-status.tsv?' + Date.now(), {cache:'no-store'});
    if (!resp.ok) throw new Error();
    const text = await resp.text();
    const newStatus = {};
    text.split('\n').forEach(line => {
      const parts = line.split('\t');
      if (parts.length >= 2) newStatus[parts[0].trim()] = parts[1].trim();
    });
    let changed = false;
    GRAPH.steps.forEach(step => {
      const ns = newStatus[step.id];
      if (ns && ns !== step.status) { step.status = ns; changed = true; }
    });
    if (changed) { reRenderAll(); }
    document.getElementById('live-dot').classList.remove('offline');
    document.getElementById('live-label').textContent = 'live';
  } catch {
    document.getElementById('live-dot').classList.add('offline');
    document.getElementById('live-label').textContent = 'static';
  }
}

function reRenderAll() {
  edgesLayer.innerHTML = '';
  nodesLayer.innerHTML = '';
  GRAPH.steps.forEach(s => {
    s.depends_on.forEach(dep => { const d=stepById[dep]; if(d) renderEdge(d,s); });
  });
  GRAPH.sidecars.forEach(sc => {
    const consumer = stepById[sc.consumer_step];
    if (consumer) renderSidecarEdge(consumer, sc);
  });
  GRAPH.steps.forEach(renderNode);
  GRAPH.sidecars.forEach(renderSidecar);
  updateProgress();
  drawMinimap();
}

setInterval(refreshStatus, 3000);
refreshStatus();

// ── Initial render ────────────────────────────────────────
document.getElementById('wf-title').textContent  = GRAPH.workflow_name;
document.getElementById('policy-label').textContent = GRAPH.policy_pack;

// Render edges first
GRAPH.steps.forEach(step => {
  step.depends_on.forEach(dep => { const d=stepById[dep]; if(d) renderEdge(d, step); });
});
GRAPH.sidecars.forEach(sc => {
  const consumer = stepById[sc.consumer_step];
  if (consumer) renderSidecarEdge(consumer, sc);
});

// Render nodes
GRAPH.steps.forEach(renderNode);
GRAPH.sidecars.forEach(renderSidecar);

updateProgress();
fitToScreen();
</script>
</body>
</html>
"""


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Generate n8n-style HTML workflow visualization.")
    parser.add_argument(
        "--workflow-dir", default=".", help="Workflow directory containing workflow.json."
    )
    parser.add_argument("--output", default=None, help="Output HTML path.")
    parser.add_argument("--open", action="store_true", help="Open browser after generating.")
    args = parser.parse_args(argv)

    workflow_dir = Path(args.workflow_dir).resolve()
    output_path = Path(args.output) if args.output else workflow_dir / "workflow-graph.html"

    try:
        html = generate_html(workflow_dir)
    except FileNotFoundError as exc:
        print(f"[visualize] Error: {exc}", file=sys.stderr)
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"[visualize] Wrote {output_path}")

    if args.open:
        try:
            subprocess.Popen(["open", str(output_path)])
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

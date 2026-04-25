#!/usr/bin/env python3
"""Risk-classify mutation proposals and analyze workflow run history."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Risk ordinal — lower is safer.
RISK_ORDER: dict[str, int] = {"low": 0, "medium": 1, "high": 2}

# Keys on a modify_step payload's "changes" dict, grouped by risk level.
_HIGH_RISK_KEYS: frozenset[str] = frozenset(
    {"script", "type", "success_gate", "depends_on", "condition", "cases", "on_true", "on_false"}
)
_MEDIUM_RISK_KEYS: frozenset[str] = frozenset(
    {"executor_config", "url", "method", "prompt", "policy", "env", "working_directory"}
)
_LOW_RISK_KEYS: frozenset[str] = frozenset(
    {"timeout_seconds", "retry_limit", "description", "name", "auto_heal"}
)

# Base risk by mutation type (modify_step is resolved from its change set).
_TYPE_RISK: dict[str, str] = {
    "add_step": "medium",
    "remove_step": "high",
    "add_sidecar": "low",
}


def classify_risk(mutation: dict[str, Any]) -> str:
    """Return 'low', 'medium', or 'high' risk for *mutation*.

    Rules (in priority order):
      remove_step          → high   (destructive, irreversible)
      add_step             → medium (additive but untested)
      add_sidecar          → low    (observability-only, non-destructive)
      modify_step          → derived from the changed keys:
        any _HIGH_RISK_KEYS  → high
        any _MEDIUM_RISK_KEYS → medium
        otherwise             → low
      unknown type         → medium (conservative default)
    """
    mtype = mutation.get("type", "")
    if mtype in _TYPE_RISK:
        return _TYPE_RISK[mtype]
    if mtype == "modify_step":
        changes = set(mutation.get("payload", {}).get("changes", {}).keys())
        if changes & _HIGH_RISK_KEYS:
            return "high"
        if changes & _MEDIUM_RISK_KEYS:
            return "medium"
        return "low"
    return "medium"


def risk_at_most(mutation: dict[str, Any], max_risk: str) -> bool:
    """Return True if *mutation*'s risk is ≤ *max_risk* (e.g. 'low')."""
    return RISK_ORDER[classify_risk(mutation)] <= RISK_ORDER[max_risk]


# ---------------------------------------------------------------------------
# Run-history analysis
# ---------------------------------------------------------------------------


def _read_events(run_dir: Path) -> list[dict[str, Any]]:
    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in events_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return events


def analyze_run_history(audit_root: Path) -> dict[str, dict[str, Any]]:
    """Scan all recorded run directories under *audit_root* and return per-step stats.

    Returns a dict keyed by step_id:
      {
        "runs":         int,    # times the step was attempted
        "failures":     int,    # times it failed
        "failure_rate": float,  # failures / runs  (0.0–1.0)
        "avg_duration": float,  # mean duration in seconds
        "trend":        str,    # "improving", "stable", "degrading", or "unknown"
      }
    """
    if not audit_root.exists():
        return {}

    step_stats: dict[str, dict[str, Any]] = {}

    for run_dir in sorted(audit_root.iterdir()):
        if not run_dir.is_dir():
            continue
        events = _read_events(run_dir)
        durations: dict[str, float] = {}
        for ev in events:
            etype = ev.get("event", "")
            sid = ev.get("step_id", "")
            if not sid:
                continue
            if etype == "step_started":
                stats = step_stats.setdefault(sid, {"runs": 0, "failures": 0, "durations": []})
                stats["runs"] += 1
            elif etype == "step_completed":
                stats = step_stats.setdefault(sid, {"runs": 0, "failures": 0, "durations": []})
                dur = ev.get("duration_seconds")
                if isinstance(dur, (int, float)):
                    stats["durations"].append(float(dur))
            elif etype == "step_failed":
                stats = step_stats.setdefault(sid, {"runs": 0, "failures": 0, "durations": []})
                stats["failures"] += 1

    # Summarize
    result: dict[str, dict[str, Any]] = {}
    for sid, raw in step_stats.items():
        runs = raw["runs"]
        failures = raw["failures"]
        durations = raw["durations"]
        failure_rate = failures / runs if runs else 0.0
        avg_duration = sum(durations) / len(durations) if durations else 0.0
        # Simple trend: compare last 3 vs first 3 durations
        trend = "unknown"
        if len(durations) >= 6:
            early_avg = sum(durations[:3]) / 3
            recent_avg = sum(durations[-3:]) / 3
            if recent_avg < early_avg * 0.9:
                trend = "improving"
            elif recent_avg > early_avg * 1.1:
                trend = "degrading"
            else:
                trend = "stable"
        result[sid] = {
            "runs": runs,
            "failures": failures,
            "failure_rate": round(failure_rate, 3),
            "avg_duration": round(avg_duration, 2),
            "trend": trend,
        }
    return result


def improvement_summary(
    pending_mutations: list[dict[str, Any]],
    history: dict[str, dict[str, Any]],
    max_risk: str = "low",
) -> dict[str, Any]:
    """Summarize which mutations are auto-approvable and highlight unhealthy steps.

    Returns:
      {
        "auto_approvable":  [mutation, ...],   # risk ≤ max_risk
        "needs_review":     [mutation, ...],   # risk > max_risk
        "unhealthy_steps":  [step_id, ...],    # failure_rate > 0.2
      }
    """
    auto: list[dict[str, Any]] = []
    needs: list[dict[str, Any]] = []
    for mut in pending_mutations:
        if mut.get("status") != "pending":
            continue
        if risk_at_most(mut, max_risk):
            auto.append(mut)
        else:
            needs.append(mut)

    unhealthy = [sid for sid, stats in history.items() if stats.get("failure_rate", 0) > 0.2]

    return {
        "auto_approvable": auto,
        "needs_review": needs,
        "unhealthy_steps": sorted(unhealthy),
    }

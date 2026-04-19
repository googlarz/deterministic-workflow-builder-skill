#!/usr/bin/env python3
"""Discover available Codex and Claude Code skills for use as workflow steps."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Canonical search paths — order matters: more specific paths win on name collision.
_SEARCH_PATHS = [
    Path.home() / ".codex" / "skills",
    Path.home() / ".claude" / "plugins" / "cache",
    Path.home() / ".claude" / "skills",
    Path.home() / ".claude" / "plugins",
]


def _read_skill_description(skill_dir: Path) -> str:
    """Extract a one-line description from SKILL.md or README.md."""
    for fname in ("SKILL.md", "README.md", "skill.md", "readme.md"):
        p = skill_dir / fname
        if p.exists():
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    return line[:120]
            # Fall back to first heading
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("#"):
                    return line.lstrip("#").strip()[:120]
    return ""


def _skill_entry(skill_dir: Path, source: str) -> dict[str, Any]:
    name = skill_dir.name
    skill_md = skill_dir / "SKILL.md"
    readme = skill_dir / "README.md"
    doc_path = skill_md if skill_md.exists() else (readme if readme.exists() else None)
    return {
        "name": name,
        "path": str(skill_dir),
        "source": source,
        "description": _read_skill_description(skill_dir),
        "skill_md": str(doc_path) if doc_path else None,
        "has_skill_md": skill_md.exists(),
    }


def discover(extra_paths: list[Path] | None = None) -> list[dict[str, Any]]:
    """Return a deduplicated list of available skills from all known locations."""
    search = list(_SEARCH_PATHS) + (extra_paths or [])
    seen: dict[str, dict[str, Any]] = {}

    for base in search:
        if not base.exists():
            continue
        source = "codex" if "codex" in str(base) else "claude"
        for candidate in sorted(base.iterdir()):
            if not candidate.is_dir():
                continue
            # Skip hidden dirs, cache metadata, version dirs
            if candidate.name.startswith(".") or candidate.name.startswith("_"):
                continue
            # Skip versioned sub-dirs like "1.0.1" that live inside a plugin dir
            if candidate.name[0].isdigit() and "." in candidate.name:
                continue
            has_doc = any((candidate / f).exists() for f in ("SKILL.md", "README.md"))
            if not has_doc:
                # Look one level deeper (e.g. cache/plugin-name/1.0.0/SKILL.md)
                for sub in sorted(candidate.iterdir()):
                    if sub.is_dir() and any((sub / f).exists() for f in ("SKILL.md", "README.md")):
                        entry = _skill_entry(sub, source)
                        entry["name"] = candidate.name  # use parent as canonical name
                        if entry["name"] not in seen:
                            seen[entry["name"]] = entry
                continue
            entry = _skill_entry(candidate, source)
            if entry["name"] not in seen:
                seen[entry["name"]] = entry

    return sorted(seen.values(), key=lambda e: e["name"])


def format_for_prompt(skills: list[dict[str, Any]]) -> str:
    """Format skill list as a compact string to embed in a Claude prompt."""
    if not skills:
        return "(no skills discovered)"
    lines = []
    for s in skills:
        desc = f" — {s['description']}" if s["description"] else ""
        lines.append(f"  - {s['name']} [{s['source']}]{desc}")
    return "\n".join(lines)


def read_skill_md(skill: dict[str, Any]) -> str:
    """Return the full SKILL.md content for a skill entry."""
    p = skill.get("skill_md")
    if p and Path(p).exists():
        return Path(p).read_text(encoding="utf-8", errors="replace")
    return ""


def find_skill(name: str, skills: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find a skill by exact name or case-insensitive prefix match."""
    for s in skills:
        if s["name"] == name:
            return s
    name_lower = name.lower()
    for s in skills:
        if s["name"].lower() == name_lower:
            return s
    # prefix match
    matches = [s for s in skills if s["name"].lower().startswith(name_lower)]
    return matches[0] if len(matches) == 1 else None


def main(argv: list[str]) -> int:
    skills = discover()
    if "--json" in argv:
        print(json.dumps(skills, indent=2))
    else:
        if not skills:
            print("No skills found.")
            return 0
        print(f"Found {len(skills)} skill(s):\n")
        for s in skills:
            marker = "✓" if s["has_skill_md"] else "·"
            print(f"  {marker} {s['name']:40s} [{s['source']}]  {s['description'][:60]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

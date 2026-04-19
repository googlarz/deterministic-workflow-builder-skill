#!/usr/bin/env python3
"""Package the deterministic workflow builder skill as a release zip."""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parents[1]
TOP_LEVEL_DIR = "deterministic-workflow-builder"
REQUIRED_PATHS = (
    "SKILL.md",
    "README.md",
    "VERSION",
    "CHANGELOG.md",
    "COMPATIBILITY.md",
    "LICENSE",
    "scripts/run_workflow.py",
    "scripts/init_deterministic_workflow.py",
    "scripts/compile_workflow.py",
    "tests/test_deterministic_workflow.py",
)
EXCLUDED_DIRS = {
    ".git", ".github", "__pycache__", ".ruff_cache", ".tox", ".venv",
    "dist", "build",
    # runtime-generated directories that must never appear in a release archive
    "artifacts", "logs", "state", "audit", "runs",
}
EXCLUDED_FILES = {".DS_Store"}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package the skill into a distributable zip archive."
    )
    parser.add_argument(
        "--output-dir",
        default="dist",
        help="Directory where the zip archive should be written (default: dist).",
    )
    return parser.parse_args(argv)


def read_version() -> str:
    return (SKILL_DIR / "VERSION").read_text(encoding="utf-8").strip()


def iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts):
            continue
        if path.name in EXCLUDED_FILES:
            continue
        files.append(path)
    return files


def validate_required_paths(files: list[Path], root: Path) -> None:
    available = {str(path.relative_to(root)) for path in files}
    missing = [path for path in REQUIRED_PATHS if path not in available]
    if missing:
        raise SystemExit(
            f"Refusing to package incomplete skill; missing required paths: {', '.join(missing)}"
        )


def build_archive(output_dir: Path) -> Path:
    version = read_version()
    archive_path = output_dir / f"deterministic-workflow-builder-skill-v{version}.zip"
    files = iter_files(SKILL_DIR)
    validate_required_paths(files, SKILL_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            relative = path.relative_to(SKILL_DIR)
            archive.write(path, Path(TOP_LEVEL_DIR) / relative)
    return archive_path


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    archive_path = build_archive(Path(args.output_dir))
    print(f"[OK] Wrote {archive_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

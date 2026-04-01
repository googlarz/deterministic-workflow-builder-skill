# Compatibility

- Latest schema: `4`
- Supported schema versions: `2`, `3`, `4`
- Migration path: `python scripts/migrate_workflow.py <workflow-dir> --write`
- Supported Python versions: `3.9`, `3.10`, `3.11`, `3.12`
- Python support policy:
  - The project actively tests every commit against the versions above in GitHub Actions and `tox`.
  - New Python minor versions are added after CI and smoke-install coverage are green.
  - Python versions may be removed only after they leave practical support for this project, and that change should be called out in [CHANGELOG.md](./CHANGELOG.md).
- Runtime model:
  - `schema_version: 4` gets full contract enforcement and structured audit features
  - `schema_version: 3` and `2` can be migrated forward before production use
- Packaging compatibility:
  - Release archives are expected to unpack into a top-level `deterministic-workflow-builder/` directory.
  - The packaged skill must remain runnable from an extracted archive without requiring a preinstalled global copy under `~/.codex/skills`.

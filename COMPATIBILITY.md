# Compatibility

- Latest schema: `4`
- Supported schema versions: `2`, `3`, `4`
- Migration path: `python scripts/migrate_workflow.py <workflow-dir> --write`
- Python: tested with `python3`
- Runtime model:
  - `schema_version: 4` gets full contract enforcement and structured audit features
  - `schema_version: 3` and `2` can be migrated forward before production use

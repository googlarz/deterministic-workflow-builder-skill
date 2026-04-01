# Contributing

Thanks for helping improve the deterministic workflow builder.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ruff>=0.14,<0.15" "pre-commit>=4.3,<5"
pre-commit install
```

## Local Validation

Run the same checks that protect the repository in CI:

```bash
ruff check scripts tests
ruff format --check scripts tests
python -m py_compile scripts/*.py
python -m unittest discover -s tests -p 'test_*.py'
python scripts/evaluate_benchmarks.py
python scripts/package_skill.py --output-dir dist
pre-commit run --all-files
```

## Change Expectations

- Keep runtime behavior deterministic unless the change is explicitly about bounded sidecars.
- Prefer standard-library-only scripts unless an added dependency is clearly justified.
- Update `VERSION` and `CHANGELOG.md` for user-visible changes.
- Add or update tests for behavior changes, especially around manifests, packaging, approvals, and recovery.
- Do not break packaged-skill execution from an extracted archive.

## Release Process

1. Update `VERSION`.
2. Summarize the release in `CHANGELOG.md`.
3. Ensure local validation passes.
4. Push a matching `v*` tag.

The release workflow packages the skill and publishes the zip to GitHub Releases.

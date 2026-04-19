# Deterministic Workflow Builder

[![CI](https://github.com/googlarz/deterministic-workflow-builder-skill/actions/workflows/python-matrix.yml/badge.svg)](https://github.com/googlarz/deterministic-workflow-builder-skill/actions/workflows/python-matrix.yml)
[![Release](https://img.shields.io/github/v/release/googlarz/deterministic-workflow-builder-skill?display_name=tag)](https://github.com/googlarz/deterministic-workflow-builder-skill/releases)
[![License](https://img.shields.io/github/license/googlarz/deterministic-workflow-builder-skill)](https://github.com/googlarz/deterministic-workflow-builder-skill/blob/main/LICENSE)

Build deterministic, auditable, repeatable workflows for Codex and Claude.

This skill turns vague "make it deterministic" requests into a workflow package with:
- a typed `workflow.json` manifest
- explicit `steps/*.sh`
- approval gates
- machine-checkable contracts
- replayable audits
- rollback hooks
- doctor and repair flows
- bounded AI sidecars that stay advisory

## Why

Most agent workflows fail in production because runtime behavior is too implicit:
- the model decides at execution time
- approvals are informal
- outputs are not contract-checked
- retries and rollback are ad hoc
- state corruption is unrecoverable

This skill pushes the opposite direction: deterministic runtime, explicit state, and observable evidence.

## What It Builds

```text
                user request
                     |
                     v
          +----------------------+
          | compile_workflow.py  |
          +----------------------+
                     |
                     v
    +----------------------------------------+
    | workflow.json + steps/*.sh + prompts   |
    +----------------------------------------+
                     |
          +----------+----------+
          |                     |
          v                     v
   verify_workflow.py     security_audit.py
          |                     |
          +----------+----------+
                     |
                     v
             run_workflow.sh
                     |
   +-----------------+------------------+
   |                 |                  |
   v                 v                  v
 approvals      DAG execution      rollback/repair
   |                 |                  |
   +-----------------+------------------+
                     |
                     v
             audit runs + replay
```

## Core Capabilities

- Typed schema v4 with migration support
- Runtime enforcement of produced and consumed artifact contracts
- Structured approvals with approver, reason, and change reference
- Parallel DAG execution with deterministic dependency handling
- Rollback hooks for failure recovery
- Doctor and repair commands for corrupted or interrupted state
- Security audit for workflow packages
- Prompt-asset pinning and sidecar output schemas
- Benchmarks and tests for regression checking

## Workflow Visualization

Every run automatically generates `workflow-graph.html` — an n8n-style interactive DAG viewer — in the workflow directory. Open it in any browser, no server required.

```bash
# Generate or refresh the visualization without running the workflow
python3 scripts/run_workflow.py <workflow-dir> --visualize

# Or generate it directly
python3 scripts/visualize_workflow.py --workflow-dir <workflow-dir>
```

**Features:**
- Live-updating status via XHR poll (every 3 s) with **live / static** indicator
- Color-coded nodes by step type (shell, test, python, json-validate, http-check, approval, …)
- Bezier edges colored by source step status — green for complete, orange for waiting-approval
- **GATE** badge on manual-approval steps
- Sidecar AI advisor nodes anchored below their consumer
- Click any node → inspector panel: type, status, script, dependencies, runtime metrics
- Click empty canvas or press **Esc** to close inspector
- **F** = fit-to-screen, **/** = search/filter, minimap, Export SVG
- Progress bar: `N/total complete · M approvals`

![Workflow Visualization](docs/visualization-preview.png)

## Repository Layout

```text
deterministic-workflow-builder/
├── SKILL.md
├── README.md
├── VERSION
├── CHANGELOG.md
├── COMPATIBILITY.md
├── acceptance.md
├── assets/
│   ├── policies/
│   ├── prompts/
│   └── sidecar-registry.json
├── benchmarks/
├── references/
├── scripts/
│   ├── compile_workflow.py
│   ├── run_workflow.py
│   ├── verify_workflow.py
│   ├── security_audit.py
│   ├── migrate_workflow.py
│   └── ...
└── tests/
```

## Quick Start

Install the skill by copying this folder into `.codex/skills/` or by downloading a release zip and extracting the `deterministic-workflow-builder/` directory into your skills directory.

Create a scaffold:

```bash
python3 scripts/init_deterministic_workflow.py demo-flow --path . --steps collect,review,publish
```

Compile from a natural-language request:

```bash
python3 scripts/compile_workflow.py "Fix the failing CI test and make it deterministic." --path .
```

Verify and inspect:

```bash
python3 scripts/verify_workflow.py ./demo-flow --simulate
python3 scripts/security_audit.py ./demo-flow
./demo-flow/run_workflow.sh --list
./demo-flow/run_workflow.sh --doctor
```

Run and recover:

```bash
./demo-flow/run_workflow.sh --dry-run
./demo-flow/run_workflow.sh --approve 02-review --approval-reason "release checklist passed"
./demo-flow/run_workflow.sh
./demo-flow/run_workflow.sh --replay run-0001
./demo-flow/run_workflow.sh --repair
```

## Testing

```bash
python3 -m pip install "ruff>=0.14,<0.15" "pre-commit>=4.3,<5"
ruff check scripts tests
ruff format --check scripts tests
python3 -m py_compile scripts/*.py
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/evaluate_benchmarks.py
python3 scripts/security_audit.py <workflow-dir>
python3 scripts/package_skill.py --output-dir dist
pre-commit run --all-files
```

## Release

Cut a release by updating [VERSION](./VERSION), updating [CHANGELOG.md](./CHANGELOG.md), and pushing a matching git tag:

```bash
git tag "v$(cat VERSION)"
git push origin --tags
```

The release workflow packages the skill, uploads the zip artifact, and publishes a GitHub release for matching `v*` tags.

## Intended Use

Use this when you want:
- "no AI in the loop" runtime behavior
- reproducible workflow execution
- approval checkpoints
- auditable state transitions
- safe hybrid workflows where AI only suggests and never decides

See [SKILL.md](./SKILL.md) for the full Codex skill behavior.

## Project Policies

- License: [MIT](./LICENSE)
- Contributing guide: [CONTRIBUTING.md](./CONTRIBUTING.md)
- Security policy: [SECURITY.md](./SECURITY.md)
- Compatibility policy: [COMPATIBILITY.md](./COMPATIBILITY.md)

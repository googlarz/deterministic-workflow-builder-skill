# Deterministic Workflow Builder

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
python3 -m py_compile scripts/*.py
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/evaluate_benchmarks.py
python3 scripts/security_audit.py <workflow-dir>
```

## Intended Use

Use this when you want:
- "no AI in the loop" runtime behavior
- reproducible workflow execution
- approval checkpoints
- auditable state transitions
- safe hybrid workflows where AI only suggests and never decides

See [SKILL.md](./SKILL.md) for the full Codex skill behavior.

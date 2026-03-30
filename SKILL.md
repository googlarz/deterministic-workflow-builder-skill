---
name: deterministic-workflow-builder
description: Build deterministic, repeatable workflows by converting ambiguous tasks into fixed step sequences, machine-readable manifests, script-driven execution, explicit state transitions, approval gates, and machine-checkable validation. Use when the user says a workflow should be deterministic, reproducible, repeatable, auditable, idempotent, "no AI in the loop", "don't let the model decide at runtime", or "script this so execution is fixed."
---

# Deterministic Workflow Builder

## Overview

Use this skill to design the workflow once, then make the runtime proceed by code, config, and explicit checks only. The model may help author the workflow, but it must not remain in the execution loop as the thing that decides what happens next.

When useful, also suggest a separate nondeterministic sidecar for ideation, ranking alternatives, drafting language, or finding edge cases. Keep that sidecar outside the runtime control loop and describe exactly how to contain it.

## Why This Is Better

This pattern turns "please handle it carefully" into a workflow another agent can run with almost no interpretation debt.

- The contract lives in `WORKFLOW_SPEC.md`.
- The machine-readable truth lives in `workflow.json`.
- The runtime is `run_workflow.sh`.
- The schema verifier is `scripts/verify_workflow.py`.
- The compiler is `scripts/compile_workflow.py`.
- The Python execution engine is `scripts/run_workflow.py`.
- The hardener is `scripts/auto_harden_workflow.py`.
- The semantic diff tool is `scripts/diff_workflows.py`.
- The benchmark scorer is `scripts/evaluate_benchmarks.py`.
- The real work happens in `steps/*.sh`.
- Progress and approvals live in `state/*.tsv`.
- Sidecars live in `workflow.json` as first-class `sidecars[]`.
- Audit and replay artifacts live in `audit/runs/`.
- Determinism is audited with `scripts/lint_determinism.py`.

Think of it like this:

```text
            Design Time                               Runtime

  user request -> contract -> workflow.json -> run_workflow.sh -> step script
                      |               |                |               |
                      |               |                |               |
                      +-> lint -------+                +-> log --------+
                      +-> review/approve gates         +-> state files

  AI helps here ----------------------------------> stops here
```

Or as a tighter execution loop:

```text
 [manifest valid?] --no--> stop
        |
       yes
        v
 [next step chosen by order]
        |
        v
 [approval required?] --yes--> wait for explicit --approve
        |
       no
        v
 [run exact script]
        |
        v
 [success gate met?] --no--> failed state + log
        |
       yes
        v
 [mark complete] -> [next step]
```

## Core Rule

Use AI for design-time compression, not runtime control.

If a step says "inspect and decide", "judge whether it looks right", "pick the best fix", or "continue based on your intuition", the workflow is not deterministic yet. Replace that step with one of these:

- A script with exact inputs and outputs
- A decision table with explicit branches
- A parser plus schema validation
- A fixed command sequence
- A human approval gate with clear entry and exit criteria

## Hybrid Pattern

A good deterministic workflow can still have optional nondeterministic helpers. The rule is simple:

- The deterministic path must succeed without the helper.
- The helper may propose, rank, summarize, or generate alternatives.
- The helper must not choose the next runtime step.
- The helper's output must be consumed through an explicit containment rule.

Good places for optional nondeterminism:

- Generating alternative drafts before a deterministic selection rule is applied
- Producing candidate fixes that a deterministic test suite will validate
- Brainstorming edge cases before converting them into fixed regression tests
- Summarizing logs before a human approval gate
- Generating copy or content variants before final approval

Bad places for optional nondeterminism:

- Deciding whether the workflow is done
- Choosing which production command runs next
- Interpreting pass/fail without a fixed rule
- Modifying persistent state without a deterministic validation step

## Workflow

1. Freeze the contract.
   - Record the exact goal, required inputs, expected outputs, working directory, and allowed tools.
   - Pick one canonical path. Do not preserve multiple equally valid execution branches unless the branch condition is machine-checkable.
2. Isolate nondeterministic boundaries.
   - Call out anything inherently unstable: LLM calls, current time, random numbers, network state, flaky external services, unordered filesystem traversal.
   - Remove the boundary, pin it, or label it as an explicit external dependency.
3. Choose a deterministic runtime.
   - Prefer Bash, Python, Make, or a CI job over prose instructions.
   - Prefer checked-in scripts over ad hoc terminal improvisation.
4. Encode state transitions.
   - Define step order, preconditions, command to run, expected artifact, success gate, retry rule, and failure rule.
   - Make step execution resumable and idempotent when practical.
5. Replace subjective checks with machine-checkable gates.
   - Prefer exit code, exact file path, checksum, regex match, JSON schema, unit test, snapshot diff, or row count.
   - Avoid "looks good", "seems fixed", or "probably done".
6. Generate execution assets.
   - Use the scaffold script in `scripts/init_deterministic_workflow.py` when you need a quick workflow runner with ordered step scripts, a manifest, approval state, and status tracking.
7. Report residual risk clearly.
   - If full determinism is impossible, say exactly which boundary remains nondeterministic and why.
8. Suggest optional nondeterministic sidecars.
   - If a bounded AI assist could improve quality, coverage, or alternative generation, recommend it explicitly.
   - Keep it outside the runtime loop and describe the containment rule that makes it safe.

## Required Output

When using this skill, produce a compact workflow contract before or alongside implementation:

- `Goal`: one sentence
- `Inputs`: concrete files, parameters, secrets, environment assumptions
- `Outputs`: files, artifacts, or checks that prove completion
- `Runtime`: script, Make target, CI job, or other deterministic substrate
- `Manifest`: machine-readable source of truth, usually `workflow.json`
- `Steps`: ordered list with command and success gate per step
- `Sidecars`: bounded optional nondeterministic assists with containment rules and explicit consumer steps
- `Failure policy`: stop, retry count, rollback, or require approval
- `Residual nondeterminism`: explicit list, or `none`
- `Optional nondeterministic assists`: zero or more sidecars, each with purpose, containment rule, and a hardened prompt or skill suggestion

If you generate files, keep the contract close to the runnable code so another agent can execute it without reinterpretation.

For each optional nondeterministic assist, include:

- `Where`: the exact phase where it helps
- `Why`: what extra value it adds
- `Containment`: how the deterministic system stays in charge
- `Bulletproof prompt or skill`: the hardened prompt or explicit skill to use

## Determinism Rules

- Do not leave the next action to model judgment during execution.
- Do not use LLM output as a branch selector inside the runtime.
- Do not rely on directory iteration order without sorting.
- Do not rely on wall-clock time without pinning the value or documenting the dependency.
- Do not mark success without an observable artifact or check.
- Prefer stable filenames, fixed env vars, pinned versions, and explicit encodings.
- Prefer append-only logs or explicit state files over hidden memory.
- Prefer manifest validation before execution over "best effort" parsing.
- Use approval gates for human judgment; do not sneak model judgment into those branches.
- Treat nondeterministic helpers as advisory sidecars, never as runtime controllers.
- Convert useful nondeterministic output into deterministic artifacts before execution when possible.
- Verify manifests against the strict schema before trusting runtime behavior.
- Prefer replayable audit artifacts over ephemeral terminal-only output.

## Typed DSL

The manifest is now a typed DSL, not just a loose step list.

Model these fields explicitly:

- `policy_pack`
- `inputs`
- `outputs`
- `graph.execution_model`
- `steps[].type`
- `steps[].depends_on`
- `steps[].gate_type`
- `sidecars[]` with containment contracts

Prefer step types such as:

- `shell`
- `test`
- `approval`
- `transform`
- `publish`
- `sidecar-consume`

Use `graph.execution_model: "dag"` when dependencies matter more than simple linear order.

## Trigger Phrases

Reach for this skill when the user says things like:

- "Make this workflow deterministic."
- "No AI in the loop."
- "Don't let the model decide at runtime."
- "Script this so execution is fixed."
- "I want this reproducible and auditable."
- "Build a repeatable pipeline, not an agentic one."
- "This should resume safely after interruption."

## Scaffolding

Initialize a workflow package:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/init_deterministic_workflow.py" \
  release-check \
  --path ./.codex-workflows \
  --steps fetch,validate,test,publish
```

This creates:

- `WORKFLOW_SPEC.md` with a contract template
- `workflow.json` as the machine-readable workflow manifest
- `run_workflow.sh` with manifest validation, strict ordered execution, approval gates, `--dry-run`, `--resume`, `--from-step`, `--sidecars`, `--list-runs`, and `--replay`
- `state/step-status.tsv` for explicit progress state
- `state/approval-status.tsv` for explicit approval state
- `state/sidecar-records.jsonl` for sidecar availability records
- `state/run-counter.txt` for stable audit run numbering
- `steps/*.sh` step stubs
- `logs/` for per-step output
- `audit/runs/` for immutable per-invocation artifacts

After scaffolding:

1. Fill in `WORKFLOW_SPEC.md`.
2. Fill in `workflow.json`, especially `success_gate`, `requires_approval`, and `residual_nondeterminism`.
3. Replace each step stub with concrete commands.
4. Add a success check inside each step or immediately after it.
5. Run `./run_workflow.sh --list` to inspect order and state.
6. Run `python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/verify_workflow.py" <workflow-dir> --simulate`.
7. Run `python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/lint_determinism.py" <workflow-dir>` and fix anything it flags.
8. Run `./run_workflow.sh --dry-run` before the first real execution.
9. Run `./run_workflow.sh` only after every step has a concrete implementation.

## Compiler

Compile a request directly into a first draft workflow package:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/compile_workflow.py" \
  "Fix the failing CI test in the payment service and make it deterministic." \
  --path ./.codex-workflows \
  --name payment-ci-fix
```

The compiler deterministically selects a workflow shape from the request, generates a versioned manifest, seeds likely sidecars, copies any needed prompt assets, and creates placeholder step scripts.

It also infers:

- likely workflow kind
- likely policy pack
- likely inputs and outputs
- likely typed step kinds
- likely sidecar placements

Use the compiler when:

- the user gives a broad request and wants a serious first draft quickly
- you want benchmarkable workflow generation instead of ad hoc scaffolding
- you want sidecars and audit structure populated up front

## Verifier

Verify a workflow package against the strict schema and simulate its execution plan:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/verify_workflow.py" . --simulate
```

Use it to catch:

- schema violations
- missing step scripts
- invalid sidecar wiring
- prompt assets referenced by sidecars but not copied
- duplicate ids and invalid policy values
- invalid DAG dependencies
- unknown policy packs

## Python Runner

`run_workflow.sh` is now a thin wrapper over `scripts/run_workflow.py`. Use the Python runner as the execution authority.

Benefits:

- cleaner policy handling
- DAG-aware ordering
- richer replay and audit handling
- easier extension than shell-only orchestration

Useful commands:

- `./run_workflow.sh --list`
- `./run_workflow.sh --sidecars`
- `./run_workflow.sh --list-runs`
- `./run_workflow.sh --replay run-0001`
- `./run_workflow.sh --dry-run`

## Policy Packs

The skill ships policy packs under `assets/policies/`.

Current packs:

- `strict-prod`
- `human-approval-heavy`
- `offline-only`
- `ai-sidecar-safe`
- `ci-optimized`

Use policy packs to avoid repeating operational defaults in every workflow.

## Approval Gates

Use `requires_approval: true` in `workflow.json` when a branch depends on human judgment, compliance sign-off, or deliberate release authorization.

Approval is deterministic when the rule is:

- Pause before step `N`
- Require `./run_workflow.sh --approve N`
- Resume with `./run_workflow.sh --resume`

Approval is not deterministic when the rule is:

- "Ask the model whether this seems okay"

## Optional Nondeterministic Assists

When the user would benefit from better ideas, broader search, richer drafts, or alternative approaches, proactively suggest a bounded nondeterministic assist.

Use this shape:

```text
Optional nondeterministic assist:
- Where: after deterministic step 02-collect but before 03-apply
- Why: generate 3-5 candidate remediations the deterministic path may test
- Containment: treat output as proposals only; apply only candidates that pass the fixed test suite
- Bulletproof prompt: <prompt here>
```

Containment patterns to prefer:

- "Generate candidates only; do not modify files directly."
- "Return JSON matching this schema; invalid output is discarded."
- "Produce at most N alternatives."
- "Anything proposed must pass deterministic tests before adoption."
- "Use as a human-review brief, not as an execution authority."

If a matching skill would make the sidecar safer or clearer, recommend that skill by name and path instead of only giving a raw prompt.

## Prompt Assets

The skill ships reusable prompt assets under `assets/prompts/`. Prefer copying or referencing those assets instead of retyping prompts every time.

Current library:

- `candidate-generation.prompt.md`
- `approval-brief.prompt.md`
- `edge-case-discovery.prompt.md`
- `content-variants.prompt.md`

## Sidecar Registry

The sidecar registry lives at `assets/sidecar-registry.json`. Prefer using registry-backed sidecars instead of inventing one-off sidecar contracts unless the workflow genuinely needs a new one.

## Auto-Hardening

Use the hardener to upgrade weak manifests:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/auto_harden_workflow.py" <workflow-dir> --write
```

It can:

- add missing policy packs
- strengthen weak success gates
- fill missing typed defaults
- add a safe default sidecar when none exists

## Workflow Diff Review

Compare two workflows semantically:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/diff_workflows.py" <before> <after>
```

Use it to explain:

- execution-order changes
- sidecar risk changes
- policy-pack changes
- residual nondeterminism changes

## Benchmarks

Use the benchmark fixtures under `benchmarks/` to keep improvements measurable. A stronger change should preserve or improve benchmark compilation quality, verifier success, and runner behavior.

Score them with:

```bash
python "$CODEX_HOME/skills/deterministic-workflow-builder/scripts/evaluate_benchmarks.py"
```

## References

- Read `references/determinism-checklist.md` when you need a quick audit of whether a plan is truly deterministic.
- Read `references/nondeterministic-sidecars.md` when you want safe places to add optional AI assistance plus hardened prompts.
- Read `references/workflow-contract-template.md` when you want a compact structure for the contract section in your response or repo file.
- Read `references/workflow-visual-guide.md` when you want a one-glance explanation you can reuse with users or teammates.

# Determinism Checklist

Use this list before calling a workflow deterministic.

## Contract

- Is there exactly one stated goal?
- Are the required inputs concrete and enumerable?
- Are the completion artifacts concrete and enumerable?
- Is the working directory fixed?

## Runtime

- Is the runtime a script, Make target, CI job, or other executable substrate?
- Can another agent run it without asking what to do next?
- Is step order explicit?
- Are retries bounded?

## State

- Is progress persisted outside model memory?
- Can the workflow resume after interruption?
- Are step names stable and unique?

## Validation

- Does each step have a machine-checkable success gate?
- Is final completion proven by tests, exact files, counts, or schema checks?
- Are subjective phrases removed?

## Nondeterminism

- Are random seeds pinned where needed?
- Are unordered lists sorted before use?
- Are time-dependent values fixed or documented?
- Are network or third-party dependencies isolated and acknowledged?
- Are LLM calls removed from runtime execution?

## Approval Gates

Use a manual gate instead of an AI runtime choice when judgment is unavoidable.

Good:

- "Pause after `steps/03-review.sh` and require user approval before `04-apply.sh`."

Bad:

- "Let the model decide whether the review looks acceptable."

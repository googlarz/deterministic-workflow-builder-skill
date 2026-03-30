# Optional Nondeterministic Sidecars

Use this reference when the user wants the workflow runtime to stay deterministic but would still benefit from AI help around the edges.

## Core Rule

Nondeterministic assistance is acceptable only when all of these are true:

- the deterministic workflow still works without it
- the assist cannot choose the next runtime action
- the assist output is bounded
- the assist output is validated, filtered, or approved before use

## Good Sidecar Placements

### 1. Candidate Generation Before Deterministic Testing

Use when:
- you want multiple possible fixes or drafts
- a deterministic test suite or validator can choose what survives

Containment:
- generate candidates only
- do not write final state directly
- apply only candidates that pass fixed checks

Bulletproof prompt:

```text
Produce 3 candidate approaches for the task below.

Constraints:
- Do not decide which candidate is best.
- Do not assume hidden context.
- Keep each candidate independent and concrete.
- Return valid JSON matching this schema exactly:
  {"candidates":[{"id":"c1","summary":"...","changes":["..."],"risks":["..."]}]}
- If uncertain, state the uncertainty inside the candidate instead of inventing facts.

Task:
<task here>
```

### 2. Human Review Brief Before Approval Gate

Use when:
- a human needs a concise review packet
- the runtime already pauses for approval

Containment:
- summary only
- no authority to approve or reject
- human approval remains explicit

Bulletproof prompt:

```text
Summarize the artifacts below for a human approval gate.

Rules:
- Do not approve or reject.
- Do not recommend hidden follow-up actions.
- Extract only observable facts, anomalies, and open questions.
- Output exactly these sections:
  Facts
  Anomalies
  Questions

Artifacts:
<paths, logs, or diffs here>
```

### 3. Edge Case Discovery Before Converting to Tests

Use when:
- you want broader test ideas
- the workflow can then convert accepted ideas into fixed regression tests

Containment:
- suggestions only
- nothing counts until encoded as deterministic tests

Bulletproof prompt:

```text
List up to 10 edge cases for the system below.

Rules:
- Return a numbered list only.
- Keep each edge case to one sentence.
- Do not claim any edge case is real unless it follows from the provided context.
- Prefer edge cases that can be turned into deterministic tests.

System:
<system description here>
```

### 4. Content Variant Generation With Deterministic Selection

Use when:
- you need copy or presentation variants
- a human or fixed rubric will choose one

Containment:
- generate bounded alternatives
- select with an explicit rubric or approval gate

Bulletproof prompt:

```text
Generate exactly 4 variants for the content below.

Rules:
- Keep facts unchanged.
- Vary only tone and structure.
- Label outputs V1 through V4.
- Do not say which is best.
- Do not add new claims.

Content:
<source content here>
```

## Skill-Based Sidecars

If an existing skill provides a tighter and safer prompt surface, prefer recommending the skill directly.

Examples:

- Use [$gh-fix-ci](/Users/dawid/.codex/skills/gh-fix-ci/SKILL.md) to inspect CI failure context, but keep the final change path deterministic through tests and explicit approval.
- Use [$solve-math-rigorously](/Users/dawid/.codex/skills/solve-math-rigorously/SKILL.md) for difficult math reasoning, then feed the verified result into a deterministic downstream step.
- Use [$coach-young-writers](/Users/dawid/.codex/skills/coach-young-writers/SKILL.md) to generate revision guidance, but keep any scoring or final acceptance criteria explicit.

## Output Pattern

Use this format in responses:

```text
Optional nondeterministic assist:
- Where: <exact stage>
- Why: <benefit>
- Containment: <how deterministic control is preserved>
- Bulletproof prompt or skill: <prompt or skill reference>
```

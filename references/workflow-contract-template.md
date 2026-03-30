# Workflow Contract Template

```md
## Deterministic Workflow Contract

Goal: <one sentence>

Inputs:
- <file, parameter, secret, or fixed assumption>

Outputs:
- <artifact or check that proves completion>

Runtime:
- <script, make target, CI job, python entrypoint, etc.>

Steps:
1. <step name> — command: `<exact command>` — success gate: <exact check>
2. <step name> — command: `<exact command>` — success gate: <exact check>
3. <step name> — command: `<exact command>` — success gate: <exact check>

Failure policy:
- <stop immediately / retry once / require approval / rollback rule>

Residual nondeterminism:
- none

Optional nondeterministic assists:
- Where: <exact phase, or `none`>
- Why: <what extra value the assist adds>
- Containment: <how the deterministic path stays in control>
- Bulletproof prompt or skill: <hardened prompt or skill reference>
```

If the workflow cannot honestly say `none`, list the remaining boundary explicitly.

# Deterministic Workflow In One Glance

Use this when you want to explain the workflow to a human quickly.

## Short Pitch

A deterministic workflow is a pipeline where the runtime never asks, "what should I do next?" The answer already exists in the manifest, the step scripts, and the success gates.

## ASCII Tour

```text
          PLAN IT ONCE                        RUN IT MANY TIMES

  user intent
      |
      v
  WORKFLOW_SPEC.md  --->  workflow.json  --->  run_workflow.sh
      |                       |                    |
      |                       |                    |
      |                       +--> ordered step ids|
      |                       +--> approval flags  |
      |                       +--> success gates   |
      |                                            |
      +--> human-readable contract                 v
                                           steps/01-*.sh
                                           steps/02-*.sh
                                           steps/03-*.sh
                                                  |
                                                  v
                                        logs/ + state/*.tsv
```

## Reliability Story

```text
freeform agent runtime:
  observe -> improvise -> decide -> act -> maybe remember

deterministic runtime:
  validate manifest -> run step N -> write state -> verify gate -> continue
```

## Good Mental Model

The model is the architect, not the operator.

- Architect:
  - writes the contract
  - chooses the stable sequence
  - installs approval gates where judgment is unavoidable
- Operator:
  - executes the declared step order
  - records state
  - stops on failed checks
  - never invents a new branch at runtime

## Reusable Explanation

You can describe it like this:

```text
This workflow is deterministic because:
1. the next step comes from the manifest, not from model judgment
2. every step has an explicit success gate
3. approvals are explicit pause points
4. progress is persisted to disk
5. reruns and resumes follow the same declared order
```

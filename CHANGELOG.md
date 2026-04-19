# Changelog

## 1.2.0
- `type: "http"` — Full HTTP request step: method, URL, headers, body, bearer/basic auth, response stored as JSON artifact. Fails on 4xx/5xx by default (`fail_on_error: false` to override). Template expansion supported in url/headers/body.
- `type: "switch"` — Multi-way branch: evaluate an expression against named cases, mark non-matching steps as skipped. Equivalent to n8n's Switch node.
- `type: "loop"` — Iterate a script over every item in a JSON array or newline-separated artifact. Each iteration receives `LOOP_ITEM`, `LOOP_INDEX`, `LOOP_TOTAL` env vars. Results written as JSON array artifact.
- `type: "wait"` — Pause execution for `seconds` or poll a condition script until it exits 0 (`until` field). Configurable timeout and poll interval.
- `type: "merge"` — Combine multiple artifact inputs into one: `concat` (arrays → flat list, objects → merged dict), `zip`, or `first`. Closes the merge-path gap vs n8n.
- `type: "workflow"` — Run another `workflow.json` as a sub-workflow. Pass artifacts in, collect artifacts out. Enables composable workflow libraries.
- 7 new tests; 49/49 pass. No remaining gaps vs n8n's core action nodes.

## 1.1.0
- Feature A — `type: "claude"` step: run a Claude prompt as a workflow step with `{{artifact:id}}` / `{{env:VAR}}` template expansion, optional `output_schema` JSON validation, and automatic artifact capture. Calls claude CLI; falls back to anthropic SDK.
- Feature B — `--generate "description"`: generate a full `workflow.json` + scaffolded directory from a natural language description using Claude.
- Feature C — Auto-heal on step failure: when `"auto_heal": true` is set on a workflow or step, Claude automatically proposes a mutation on failure. Proposal stored as a pending mutation for human review.
- Feature D — `type: "branch"` step: conditional DAG branching via a condition script (exit 0 = true path). Unchosen-branch steps are marked `skipped` and treated as complete for dependency resolution.
- Feature E — Trigger system: new `triggers` array in `workflow.json` supports `schedule` (cron) and `webhook` (HTTP) triggers. `--install-triggers` installs launchd plists on macOS or crontab entries on Linux. New `scripts/schedule_workflow.py` and auto-generated `webhook_server.py`.
- Feature F — Run history dashboard: `--dashboard` generates a self-contained `dashboard.html` showing all past runs (status, duration, step breakdown) with links to per-run visualizations. New `scripts/dashboard.py`.
- 11 new tests; 42/42 pass.

## 1.0.3
- Feature 2 — MCP steps: new `type: "mcp"` step executes a tool call via the MCP protocol using the `.mcp.json` server registry. Params support `{{artifact:id}}` and `{{env:VAR}}` template expansion. Added `assets/mcp-servers.json.example`.
- Feature 3 — Sidecar mutation proposals: sidecar scripts can emit structured JSON proposals (`add_step` / `modify_step` / `remove_step`) via a `---PROPOSE_MUTATION---` sentinel. Runner captures and stores proposals in `state/proposed-mutations.json`. New CLI flags: `--list-mutations`, `--approve-mutation ID`, `--reject-mutation ID`.
- `apply_mutation()` fills in required schema defaults so proposals without full step specs can be approved without manual editing.
- Visualization updated: nodes with pending mutations show an amber pulsing ring; inspector panel lists mutation details.
- UX fixes: workflow execution now prints per-step progress (`→ running`, `✓ complete`, `✗ failed`); approval gates print a clear pause message with instructions; `run_workflow.py` accepts workflow dir as a positional argument; init scaffold no longer generates `success_gate: TODO`.

## 1.0.2
- Added `scripts/visualize_workflow.py`: n8n-style interactive HTML DAG viewer auto-generated after every run.
- Added `--visualize` flag to `run_workflow.py` to generate the graph without executing steps.
- Visualization features: live status polling, color-coded step types, bezier edges, GATE badges, sidecar nodes, inspector panel, search/filter, minimap, Export SVG, keyboard shortcuts (F/Esc//).
- Fixed inspector close: canvas click and Esc now correctly dismiss the inspector panel.
- Fixed `package_skill.py`: runtime directories (`artifacts`, `logs`, `state`, `audit`, `runs`) are now explicitly excluded from release archives to prevent data leaks.
- Fixed `run_workflow.py --step`: removed duplicate `setup_run_audit()` call that created orphan audit runs without finalizing them.

## 1.0.1
- Added project governance files: `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, and `CODEOWNERS`.
- Added repo maintenance tooling with `ruff`, `pre-commit`, and a packaging script for release archives.
- Added CI smoke coverage for packaged install artifacts and a tag-driven GitHub release workflow.
- Clarified Python support policy and release process in the documentation.

## 1.0.0
- Promoted the workflow manifest to schema v4.
- Added runtime contract enforcement for produced and consumed artifacts.
- Added structured approvals, structured event logs, metrics, doctor/repair flows, and rollback support.
- Added prompt-asset pinning, security policy enforcement, and migration tooling.
- Added native step executors and parallel DAG execution support.

# Changelog

## 1.7.0
Two new capabilities: true parallel step execution with live streaming dashboard, and autonomous workflow self-improvement.

**Parallel execution**
- `max_parallel` is now readable from `manifest["graph"]` (not just the policy pack). Set `"graph": {"max_parallel": 4}` to run up to 4 independent steps concurrently. The ThreadPoolExecutor-based DAG scheduler was already in place — this unlocks it from workflow definitions.

**Live streaming dashboard** (`scripts/live_dashboard.py`)
- New `--live [PORT]` flag on `run_workflow.py` starts a local HTTP server (default port 7474) in a background thread alongside the workflow run.
- Serves an SSE stream from the current run's `events.jsonl` (which already captured `step_started`, `step_completed`, `step_failed` events).
- Browser page shows a real-time step grid: pending → running (pulsing) → complete/failed/skipped — no refresh needed. Zero external dependencies (SSE via stdlib `http.server`).
- Also standalone: `python3 scripts/live_dashboard.py <workflow-dir> [--port N]`

**Autonomous improvement loop**
- New `scripts/mutation_classifier.py` — risk-scores mutation proposals as `low / medium / high` based on mutation type and which step fields are being changed. `remove_step` and `script` changes are high; `timeout_seconds`/`retry_limit` changes are low.
- `run_improvement_cycle()` in `run_workflow.py` — auto-approves pending mutations that meet the configured risk threshold and prints a summary. Calls `analyze_run_history()` to surface unhealthy steps (>20% failure rate).
- New `--improve` flag — runs the improvement cycle on demand. `--improve-max-risk low|medium|high` overrides the threshold.
- Workflows opt in to post-run auto-improvement with `"auto_improve": {"enabled": true, "max_risk": "low"}` in `workflow.json`.

21 new tests; 99/99 pass.

## 1.6.0
Five security fixes from adversarial code review:
- **MCP policy bypass** — `enforce_security_policy()` now fully evaluates MCP steps: validates the server name against an optional `allowed_mcp_servers` allowlist in the policy pack, and blocks all MCP steps when `network_mode: offline`.
- **Webhook security** — Generated `webhook_server.py` now binds to `127.0.0.1` only (not `0.0.0.0`), validates the request path against a configurable `EXPECTED_PATH`, and enforces constant-time token auth via `hmac.compare_digest`. `install_webhook_trigger()` accepts `path` and `secret` from the trigger dict and forwards them to the template.
- **Dependency field** — Importer wrote `step["needs"]` but the scheduler reads `step.get("depends_on", [])`. Fixed to write `depends_on`; existing test updated accordingly.
- **Branch/switch contracts** — `type: "branch"` steps now include `condition`, `on_true`, and `on_false` derived from n8n connection output indices (output[0]=true, output[1]=false). `type: "switch"` steps include a `cases` dict keyed by `outputKey`, each mapping to downstream step IDs.
- **Mutation envelope** — Mutation proposal files are now written and read as `{"mutations": [...]}` to match the envelope format that `run_workflow.py` expects; backward-compatible reader accepts both formats.
- 12 new tests; 78/78 pass.

## 1.5.0
- `type: "browser"` — Chrome MCP-backed browser automation step. Claude navigates, clicks, fills forms, reads page content, inspects network, and captures results — all driven by a natural language `instruction` field with full artifact template expansion. No API required.
- `type: "computer-use"` — Desktop automation step via computer-use MCP. Claude controls any native app via screenshots, mouse, keyboard, and scroll. Automates workflows that have no API and no browser interface.
- Both step types write their output as artifacts, support `output_artifact` naming, full retry/timeout/auto-heal, and integrate with the approval gate and audit system.
- 5 new tests; 66/66 pass.

## 1.4.0
- `--import-n8n <file>` — Convert any n8n workflow export JSON to a `workflow.json`. Maps 30+ n8n node types: `executeCommand` → `shell`, `httpRequest` → `http`, `if` → `branch`, `switch` → `switch`, `merge` → `merge`, `wait` → `wait`, `code` → `shell`, langchain/AI nodes → `claude`, service nodes (Slack, Gmail, GitHub, Notion, Jira, Linear, S3, Postgres…) → `http` with placeholder URLs. Extracts `schedule` and `webhook` triggers. Topologically sorts steps and injects `needs` dependencies.
- Auto-improvement proposals — Imported workflows automatically get pending mutation proposals for service nodes (upgrade to `type:mcp`), inline code stubs (port to script), placeholder URLs, and auto-generated branch conditions. Reviewable with `--list-mutations`.
- `scripts/import_n8n.py` — Standalone converter; also callable via `run_workflow.py --import-n8n`.
- 7 new tests; 61/61 pass.

## 1.3.0
- `type: "skill"` — Run any Codex or Claude Code skill as a workflow step. Loads SKILL.md, builds a combined prompt with artifact context, and calls the claude CLI. Skill name resolves by exact match or case-insensitive prefix.
- `scripts/discover_skills.py` — Auto-discovers all installed skills from `~/.codex/skills/`, `~/.claude/plugins/cache`, `~/.claude/skills/`, and `~/.claude/plugins/`. Deduplicates by name (more-specific paths win). Returns name, path, source, description, and whether a SKILL.md exists.
- `--discover-skills` flag — Prints a table of all discovered skills with their source and description.
- `--generate` skill-awareness — Available skills are injected into the system prompt so generated workflows can include `type: "skill"` steps where appropriate.
- 5 new tests; 54/54 pass.

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

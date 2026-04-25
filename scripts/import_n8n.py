#!/usr/bin/env python3
"""Convert an n8n workflow export (JSON) to a deterministic-workflow-builder workflow.json."""

from __future__ import annotations

import json
import re
import sys
import uuid
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Node type → step type mapping
# ---------------------------------------------------------------------------

_NODE_TYPE_MAP: dict[str, str] = {
    "n8n-nodes-base.executeCommand": "shell",
    "n8n-nodes-base.httpRequest": "http",
    "n8n-nodes-base.code": "shell",
    "n8n-nodes-base.function": "shell",
    "n8n-nodes-base.functionItem": "shell",
    "n8n-nodes-base.if": "branch",
    "n8n-nodes-base.switch": "switch",
    "n8n-nodes-base.merge": "merge",
    "n8n-nodes-base.wait": "wait",
    "n8n-nodes-base.set": "shell",
    "n8n-nodes-base.itemLists": "shell",
    # AI / LLM nodes → claude
    "@n8n/n8n-nodes-langchain.agent": "claude",
    "@n8n/n8n-nodes-langchain.lmChatOpenAi": "claude",
    "@n8n/n8n-nodes-langchain.lmOpenAi": "claude",
    "@n8n/n8n-nodes-langchain.chainLlm": "claude",
    "@n8n/n8n-nodes-langchain.chainSummarization": "claude",
    # Service nodes — best-effort http; user can upgrade to mcp
    "n8n-nodes-base.slack": "http",
    "n8n-nodes-base.gmail": "http",
    "n8n-nodes-base.github": "http",
    "n8n-nodes-base.googleSheets": "http",
    "n8n-nodes-base.airtable": "http",
    "n8n-nodes-base.notion": "http",
    "n8n-nodes-base.jira": "http",
    "n8n-nodes-base.linear": "http",
    "n8n-nodes-base.postgres": "shell",
    "n8n-nodes-base.mySql": "shell",
    "n8n-nodes-base.redis": "shell",
    "n8n-nodes-base.s3": "http",
}

# These produce triggers, not steps
_TRIGGER_TYPES = {
    "n8n-nodes-base.manualTrigger",
    "n8n-nodes-base.cron",
    "n8n-nodes-base.webhook",
    "n8n-nodes-base.scheduleTrigger",
    "n8n-nodes-base.formTrigger",
    "n8n-nodes-base.emailReadImap",
}

# Skip entirely
_SKIP_TYPES = {
    "n8n-nodes-base.noOp",
    "n8n-nodes-base.stickyNote",
    "n8n-nodes-base.start",
}

# Service node types that map to http but should hint at MCP upgrade
_SERVICE_TYPES = {
    "n8n-nodes-base.slack",
    "n8n-nodes-base.gmail",
    "n8n-nodes-base.github",
    "n8n-nodes-base.googleSheets",
    "n8n-nodes-base.airtable",
    "n8n-nodes-base.notion",
    "n8n-nodes-base.jira",
    "n8n-nodes-base.linear",
    "n8n-nodes-base.s3",
    "n8n-nodes-base.postgres",
    "n8n-nodes-base.mySql",
    "n8n-nodes-base.redis",
}


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip()).strip("-").lower()
    return slug[:50] or "step"


def _node_id(node: dict) -> str:
    return _slugify(node.get("name", node.get("id", "step")))


# ---------------------------------------------------------------------------
# Per-type parameter mapping
# ---------------------------------------------------------------------------


def _map_http_node(node: dict, step: dict) -> None:
    params = node.get("parameters", {})
    step["method"] = params.get("method", "GET").upper()
    url = params.get("url", "")
    if url:
        step["url"] = url
    headers: dict[str, str] = {}
    for h in params.get("headerParameters", {}).get("parameters", []):
        if h.get("name"):
            headers[h["name"]] = h.get("value", "")
    if headers:
        step["headers"] = headers
    body = params.get("jsonBody", params.get("body", ""))
    if body:
        step["body"] = body if isinstance(body, str) else json.dumps(body)
    step["fail_on_error"] = True


def _map_wait_node(node: dict, step: dict) -> None:
    params = node.get("parameters", {})
    resume = params.get("resume", "timeInterval")
    amount = params.get("amount", 1)
    unit = params.get("unit", "seconds")
    multipliers = {"seconds": 1, "minutes": 60, "hours": 3600}
    step["seconds"] = int(amount) * multipliers.get(unit, 1)
    if resume != "timeInterval":
        step["_n8n_note"] = f"n8n wait resume={resume} — polling not auto-converted"


def _map_merge_node(node: dict, step: dict) -> None:
    params = node.get("parameters", {})
    mode = params.get("mode", "append")
    step["strategy"] = {"append": "concat", "mergeByKey": "zip", "keepKeyMatches": "zip"}.get(
        mode, "concat"
    )


def _map_branch_node(node: dict, step: dict, connections: dict, id_map: dict) -> None:
    """Emit runtime-valid branch contract: condition script, on_true, on_false."""
    cond_script = f"steps/{step['id']}-condition.sh"
    step["condition"] = cond_script
    step["script"] = cond_script  # kept for scaffold stub generation

    # Resolve true/false downstream step IDs from n8n output indices
    node_name = node.get("name", "")
    outputs = connections.get(node_name, {}).get("main", [])
    true_ids = [
        id_map[c["node"]]
        for c in (outputs[0] if len(outputs) > 0 else [])
        if c.get("node") in id_map
    ]
    false_ids = [
        id_map[c["node"]]
        for c in (outputs[1] if len(outputs) > 1 else [])
        if c.get("node") in id_map
    ]
    step["on_true"] = true_ids
    step["on_false"] = false_ids

    # Build condition expression comment for the stub
    params = node.get("parameters", {})
    conditions = params.get("conditions", {})
    parts: list[str] = []
    for cond in conditions.get("string", []):
        v1, op, v2 = cond.get("value1", ""), cond.get("operation", "equal"), cond.get("value2", "")
        if op == "equal":
            parts.append(f'[ "{v1}" = "{v2}" ]')
        elif op == "notEqual":
            parts.append(f'[ "{v1}" != "{v2}" ]')
        elif op == "contains":
            parts.append(f'echo "{v1}" | grep -q "{v2}"')
    for cond in conditions.get("number", []):
        v1, op, v2 = cond.get("value1", 0), cond.get("operation", "equal"), cond.get("value2", 0)
        op_map = {"equal": "-eq", "notEqual": "-ne", "larger": "-gt", "smaller": "-lt"}
        parts.append(f'[ "{v1}" {op_map.get(op, "-eq")} "{v2}" ]')
    step["_condition_expr"] = " && ".join(parts) if parts else "true"


def _map_switch_node(node: dict, step: dict, connections: dict, id_map: dict) -> None:
    """Emit runtime-valid switch contract: script + cases with downstream step lists."""
    step["script"] = f"steps/{step['id']}-switch.sh"
    params = node.get("parameters", {})
    rules = params.get("rules", {}).get("rules", [])

    node_name = node.get("name", "")
    outputs = connections.get(node_name, {}).get("main", [])

    cases: dict[str, list[str]] = {}
    for i, rule in enumerate(rules):
        key = rule.get("outputKey", str(i))
        out_ids = [
            id_map[c["node"]]
            for c in (outputs[i] if i < len(outputs) else [])
            if c.get("node") in id_map
        ]
        cases[key] = out_ids
    if cases:
        step["cases"] = cases


def _map_code_node(node: dict, step: dict) -> None:
    params = node.get("parameters", {})
    lang = params.get("language", "javaScript")
    code = params.get("jsCode", params.get("pythonCode", ""))
    preview = (code[:120].replace("\n", " ") + "…") if len(code) > 120 else code
    step["_n8n_note"] = f"code node lang={lang}: {preview!r}"
    step["script"] = f"steps/{step['id']}.sh"


def _map_langchain_node(node: dict, step: dict) -> None:
    params = node.get("parameters", {})
    text = params.get("text", params.get("query", params.get("prompt", "")))
    step["prompt"] = text or f"# TODO: port n8n AI node '{node.get('name')}'"


def _map_service_node(node: dict, step: dict) -> None:
    ntype = node.get("type", "")
    service = ntype.split(".")[-1]
    params = node.get("parameters", {})
    resource = params.get("resource", "")
    operation = params.get("operation", "")
    label = " ".join(filter(None, [service, resource, operation]))
    step["method"] = "POST"
    step["url"] = f"https://api.example.com/TODO/{label.replace(' ', '/')}"
    step["fail_on_error"] = True
    step["_n8n_note"] = f"n8n {service} node — consider upgrading to type:mcp"


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------


def _build_deps(
    nodes: list[dict], connections: dict[str, Any], id_map: dict[str, str]
) -> dict[str, list[str]]:
    """Return {final_id: [dep_final_ids]} from n8n connections."""
    deps: dict[str, list[str]] = {id_map[n["name"]]: [] for n in nodes}
    for src_name, outputs in connections.items():
        src_id = id_map.get(src_name)
        if src_id is None:
            continue
        for output_list in outputs.get("main", []):
            for conn in output_list or []:
                dst_id = id_map.get(conn.get("node", ""))
                if dst_id and dst_id != src_id and src_id not in deps.get(dst_id, []):
                    deps.setdefault(dst_id, []).append(src_id)
    return deps


def _topo_sort(node_ids: list[str], deps: dict[str, list[str]]) -> list[str]:
    in_degree: dict[str, int] = {n: 0 for n in node_ids}
    children: dict[str, list[str]] = {n: [] for n in node_ids}
    for nid, parents in deps.items():
        for p in parents:
            if p in children:
                children[p].append(nid)
                in_degree[nid] += 1
    queue = [n for n in node_ids if in_degree[n] == 0]
    order: list[str] = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for c in children[n]:
            in_degree[c] -= 1
            if in_degree[c] == 0:
                queue.append(c)
    for n in node_ids:
        if n not in order:
            order.append(n)
    return order


def _extract_triggers(nodes: list[dict]) -> list[dict]:
    triggers: list[dict] = []
    for node in nodes:
        ntype = node.get("type", "")
        if ntype not in _TRIGGER_TYPES:
            continue
        params = node.get("parameters", {})
        if ntype in ("n8n-nodes-base.cron", "n8n-nodes-base.scheduleTrigger"):
            cron_expr = "0 * * * *"
            expr = params.get("rule", {}).get("interval", [{}])
            if isinstance(expr, list) and expr:
                item = expr[0]
                field = item.get("field", "hours")
                val = item.get("minutesInterval", item.get("hoursInterval", 1))
                if field == "minutes":
                    cron_expr = f"*/{val} * * * *"
                elif field == "hours":
                    cron_expr = f"0 */{val} * * *"
            triggers.append({"type": "schedule", "cron": cron_expr, "name": node.get("name")})
        elif ntype == "n8n-nodes-base.webhook":
            triggers.append(
                {
                    "type": "webhook",
                    "path": params.get("path", "/" + _slugify(node.get("name", "webhook"))),
                    "method": params.get("httpMethod", "POST"),
                    "name": node.get("name"),
                }
            )
    return triggers


# ---------------------------------------------------------------------------
# Improvement proposals
# ---------------------------------------------------------------------------


def _improvement_proposals(steps: list[dict]) -> list[dict]:
    """Return pending mutation proposals that improve the imported workflow."""
    proposals: list[dict] = []
    for step in steps:
        note = step.get("_n8n_note", "")
        stype = step.get("type", "shell")

        if "consider upgrading to type:mcp" in note:
            proposals.append(
                {
                    "proposal_id": str(uuid.uuid4())[:8],
                    "id": f"improve-{step['id']}-mcp",
                    "type": "modify_step",
                    "step_id": step["id"],
                    "status": "pending",
                    "rationale": (
                        f"Step '{step['id']}' was an n8n service node, mapped to a placeholder HTTP call. "
                        "Upgrade to type:mcp using an appropriate Claude MCP server for reliable integration."
                    ),
                    "suggested_changes": {"type": "mcp", "url": None},
                }
            )

        if "code node lang=" in note:
            proposals.append(
                {
                    "proposal_id": str(uuid.uuid4())[:8],
                    "id": f"improve-{step['id']}-port",
                    "type": "modify_step",
                    "step_id": step["id"],
                    "status": "pending",
                    "rationale": (
                        f"Step '{step['id']}' contains inline n8n code that must be ported to a shell/Python script. "
                        f"See _n8n_note: {note[:120]}"
                    ),
                    "suggested_changes": {"script": f"steps/{step['id']}.sh"},
                }
            )

        if stype == "http":
            url = step.get("url", "")
            if "TODO" in str(url):
                proposals.append(
                    {
                        "proposal_id": str(uuid.uuid4())[:8],
                        "id": f"improve-{step['id']}-url",
                        "type": "modify_step",
                        "step_id": step["id"],
                        "status": "pending",
                        "rationale": (
                            f"Step '{step['id']}' has a placeholder URL. Replace with the real endpoint."
                        ),
                        "suggested_changes": {"url": "<REAL_ENDPOINT_HERE>"},
                    }
                )

        if stype == "branch" and step.get("_condition_expr"):
            proposals.append(
                {
                    "proposal_id": str(uuid.uuid4())[:8],
                    "id": f"improve-{step['id']}-condition",
                    "type": "modify_step",
                    "step_id": step["id"],
                    "status": "pending",
                    "rationale": (
                        f"Branch step '{step['id']}' auto-generated condition: "
                        f"{step['_condition_expr']!r}. Verify it matches original n8n logic."
                    ),
                    "suggested_changes": {
                        "script": step.get("script", f"steps/{step['id']}-condition.sh")
                    },
                }
            )

    return proposals


# ---------------------------------------------------------------------------
# Main conversion
# ---------------------------------------------------------------------------


def convert(n8n_export: dict[str, Any]) -> tuple[dict[str, Any], list[dict]]:
    """
    Convert an n8n workflow export to (workflow_manifest, improvement_proposals).
    """
    workflow_name = _slugify(n8n_export.get("name", "imported-workflow"))
    nodes: list[dict] = n8n_export.get("nodes", [])
    connections: dict = n8n_export.get("connections", {})

    action_nodes = [n for n in nodes if n.get("type") not in _SKIP_TYPES | _TRIGGER_TYPES]
    trigger_nodes = [n for n in nodes if n.get("type") in _TRIGGER_TYPES]
    triggers = _extract_triggers(trigger_nodes)

    # Build deduplicated step ID map: node name → final step id
    seen: dict[str, int] = {}
    id_map: dict[str, str] = {}
    for node in action_nodes:
        base = _node_id(node)
        if base in seen:
            seen[base] += 1
            id_map[node["name"]] = f"{base}-{seen[base]}"
        else:
            seen[base] = 0
            id_map[node["name"]] = base

    deps = _build_deps(action_nodes, connections, id_map)
    ordered = _topo_sort(list(deps.keys()), deps)

    steps: list[dict] = []
    for final_id in ordered:
        node = next((n for n in action_nodes if id_map[n["name"]] == final_id), None)
        if node is None:
            continue
        ntype = node.get("type", "")
        step_type = _NODE_TYPE_MAP.get(ntype, "shell")

        step: dict[str, Any] = {
            "id": final_id,
            "name": node.get("name", final_id),
            "type": step_type,
            "success_gate": "",
            "gate_type": "artifact",
            "requires_approval": False,
            "retry_limit": 0,
            "timeout_seconds": 300,
        }
        needs = deps.get(final_id, [])
        if needs:
            step["depends_on"] = needs

        if ntype == "n8n-nodes-base.httpRequest":
            _map_http_node(node, step)
        elif ntype == "n8n-nodes-base.wait":
            _map_wait_node(node, step)
        elif ntype == "n8n-nodes-base.merge":
            _map_merge_node(node, step)
        elif ntype == "n8n-nodes-base.if":
            _map_branch_node(node, step, connections, id_map)
        elif ntype == "n8n-nodes-base.switch":
            _map_switch_node(node, step, connections, id_map)
        elif "langchain" in ntype:
            _map_langchain_node(node, step)
        elif ntype == "n8n-nodes-base.code":
            _map_code_node(node, step)
        elif ntype in _SERVICE_TYPES:
            _map_service_node(node, step)
        else:
            # shell fallback
            params = node.get("parameters", {})
            cmd = params.get("command", "")
            if cmd:
                step["_n8n_command"] = cmd
            step.setdefault("script", f"steps/{final_id}.sh")

        if step_type == "shell" and "script" not in step:
            step["script"] = f"steps/{final_id}.sh"

        steps.append(step)

    proposals = _improvement_proposals(steps)

    # Strip internal _n8n_* keys before writing manifest
    clean_steps = [{k: v for k, v in s.items() if not k.startswith("_")} for s in steps]

    manifest: dict[str, Any] = {
        "schema_version": 4,
        "workflow_name": workflow_name,
        "version": 1,
        "goal": f"Imported from n8n: {n8n_export.get('name', 'workflow')}",
        "policy_pack": "strict-prod",
        "steps": clean_steps,
    }
    if triggers:
        manifest["triggers"] = triggers

    return manifest, proposals


# ---------------------------------------------------------------------------
# Scaffold output directory
# ---------------------------------------------------------------------------


def scaffold(manifest: dict[str, Any], proposals: list[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "workflow.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    for subdir in ("steps", "artifacts", "state", "logs", "audit/runs"):
        (output_dir / subdir).mkdir(parents=True, exist_ok=True)

    for step in manifest["steps"]:
        script_rel = step.get("script", "")
        if script_rel:
            sp = output_dir / script_rel
            sp.parent.mkdir(parents=True, exist_ok=True)
            if not sp.exists():
                sp.write_text(
                    f"#!/usr/bin/env bash\n# TODO: implement {step['id']}\necho done\n",
                    encoding="utf-8",
                )
                sp.chmod(0o755)

    if proposals:
        mutations_path = output_dir / "state" / "proposed-mutations.json"
        existing: list[dict] = []
        if mutations_path.exists():
            try:
                data = json.loads(mutations_path.read_text(encoding="utf-8"))
                # Support both legacy bare-list format and canonical {"mutations": [...]} envelope
                existing = data.get("mutations", data) if isinstance(data, dict) else data
            except (json.JSONDecodeError, OSError):
                pass
        existing.extend(proposals)
        mutations_path.write_text(
            json.dumps({"mutations": existing}, indent=2) + "\n", encoding="utf-8"
        )


def main(argv: list[str]) -> int:
    if not argv:
        print("Usage: import_n8n.py <n8n-export.json> [--output-dir DIR]", file=sys.stderr)
        return 1

    input_path = Path(argv[0])
    if not input_path.exists():
        print(f"[import-n8n] File not found: {input_path}", file=sys.stderr)
        return 1

    output_dir: Path | None = None
    for i, arg in enumerate(argv[1:], 1):
        if arg == "--output-dir" and i + 1 < len(argv):
            output_dir = Path(argv[i + 1])

    try:
        n8n_export = json.loads(input_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[import-n8n] Failed to read {input_path}: {exc}", file=sys.stderr)
        return 1

    manifest, proposals = convert(n8n_export)

    if output_dir is None:
        output_dir = Path(manifest["workflow_name"])

    scaffold(manifest, proposals, output_dir)

    n_steps = len(manifest["steps"])
    n_triggers = len(manifest.get("triggers", []))
    print(f"[import-n8n] '{n8n_export.get('name')}' → {output_dir}/")
    print(f"  {n_steps} step(s), {n_triggers} trigger(s), {len(proposals)} improvement proposal(s)")
    if proposals:
        print(f"  Review improvements: python3 run_workflow.py {output_dir} --list-mutations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

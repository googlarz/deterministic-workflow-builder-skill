# Security Policy

## Supported Versions

Security fixes are applied to the latest version on `main`. Older tags may not receive patches.

## Reporting a Vulnerability

Please avoid opening a public issue for a suspected security problem.

Instead, report it privately with:

- a clear description of the issue
- impact and affected files or commands
- reproduction steps or a proof of concept
- any suggested mitigation

If GitHub private vulnerability reporting is enabled for the repository, use that first. Otherwise, contact the maintainer directly and include `deterministic-workflow-builder security` in the subject line.

## Security Boundaries

This project aims to make workflow execution auditable and safer, but it still runs local commands by design. In particular:

- workflow step scripts can be dangerous if authored maliciously
- shell execution safety depends on policy configuration and command allowlists
- sidecars are advisory, not trusted decision-makers
- logs and artifacts should be reviewed before sharing outside trusted environments

Use the built-in `security_audit.py` tooling as a guardrail, not as a substitute for human review.

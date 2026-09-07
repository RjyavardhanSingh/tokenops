# Security Policy

## Supported versions

Security fixes are applied to the latest release on the `main` branch (PyPI
package `agent-tokenops`). Older 0.x releases are not patched separately while the
project is in alpha.

## Trust model

TokenOps is a **cost guardrail**, not a security boundary. It exists to stop your
own agents from spending more than you intended, whether through honest failure
(runaway loops, retries, prompts that balloon in cost) or because content an agent
reads redirects it into wasteful behavior (indirect prompt injection). Catching
that, before the spend happens rather than after the invoice, is what TokenOps is
designed to do.

It assumes the agents it governs are yours and are not actively trying to defeat
enforcement. Two consequences follow, and you should know which deployment you are
in:

- **Enforcement runs in-process.** Governance is applied by wrapping your model-call
  function. A call path that skips the wrapper is not counted. In single-process
  local use this is simply your own code, and not a concern.
- **Shared-file deployments expose the ledger.** In the multi-container setup, agent
  containers mount the same database that holds budgets, spend, and halt state. An
  agent that can write files could change those numbers, and TokenOps would not
  detect it.

If you run agent code you do not control, or agents that execute untrusted
instructions with file or shell access, treat TokenOps as a cost control and not a
security boundary.

**Hardening.** Run the shared control plane
([`agentplane-control-plane`](https://github.com/theagentplane/control-plane))
instead of giving agents a database path: agents then reach the ledger only over
HTTP, and the plane is the sole process that opens the database. Set API keys so the
plane rejects unknown callers. Multi-tenant authorization on the ledger routes is
still in progress; until it lands, treat any holder of a valid ingest key as trusted
for all runs.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security reports.

Use [GitHub Security Advisories](https://github.com/theagentplane/tokenops/security/advisories/new)
to send a private report. Include:

- A clear description of the issue and impact
- Steps to reproduce (or a proof of concept if safe to share privately)
- Affected versions / commit SHA if known

We will acknowledge receipt as soon as we can, assess severity, and coordinate a
fix and disclosure timeline with you. Please give us reasonable time to ship a
fix before any public discussion.

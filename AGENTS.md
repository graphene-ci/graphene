# AGENTS.md — graphene

Read the organization-level `AGENTS.md` (one directory up, org root) first;
its rules apply here in full.

## Scope of this repository

The runtime core: control plane, agent, and CLI. It consumes typed models
and contracts from `graphene-lang` and hosts plugin executors from
`graphene-plugins`. It never defines workflow schema itself.

## Rules

- The orchestration core is one implementation: server mode and CLI
  standalone mode (`run --local`) host the same code — never fork logic
  per mode.
- Teardown of ephemeral resources must be reachable from any state,
  including cancel and crash.
- Secrets never land in run state, artifacts, or logs; resolution happens
  as late as possible.
- RPC contracts are owned by the core via `graphene-lang` IDL; plugins
  implement them. No ad-hoc side channels.
- Stack/technology choices are recorded as ADRs in `graphene-docs` before
  implementation starts.

## Status

Bootstrap skeleton — structure is not yet established. When adding the first
real content, update this file and the README in the same PR.

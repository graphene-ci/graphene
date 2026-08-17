# AGENTS.md — graphene

The control plane server of graphene vision v3 (`../GRAPHENE.MD` at the
org root). Server only: no user-facing Go surface here.

## Before making changes

1. Read `../GRAPHENE.MD`. A change that contradicts the vision updates
   the vision first.
2. `make lint` and `make test` must be green before push.

## Code rules

- Go; code, names, and comments in English. Commits are Conventional
  Commits, no `Co-Authored-By`.
- Shared types, identifiers, wire conventions, and system resource flows
  come from the pipeline repository — never redefine them here.
- Side effects of the flows are behind the `Ops` interfaces from
  `pipeline/flow/*`; implementations live in `internal/`, every method
  idempotent.
- Secrets and large data never enter specs, logs, or Temporal history —
  references only.

## Boundaries

- `cmd/` — binary assembly only.
- `internal/` — everything else: `Ops` implementations, the server
  worker, API, secrets/tokens, managed execution path. Nothing importable
  from outside by design.

# AGENTS.md — graphene

Implementation of vision v3 (`../GRAPHENE.MD`). The previous layout
(k3s/CRD, operators) is retired and lives in branches; the rules below
describe the current one.

## Before making changes

1. Read `../GRAPHENE.MD` — the vision's decisions. A change that
   contradicts it updates the vision first, then the code.
2. `make lint` and `make test` must be green before push.

## Code rules

- Implementation language is Go; code, names, and comments are English.
  Commits are Conventional Commits, no `Co-Authored-By`.
- Identifiers are the `pkg/id` types, suffix `Id` (the var-naming
  exception is recorded in `.golangci.yaml` with its reason).
- Workflow code is deterministic; all external I/O is activities behind
  `Ops` interfaces, implemented in the server, every method idempotent.
- Secrets and large data never enter specs, logs, or Temporal history —
  references only (`pkg/ref`).
- System entities are built on `temporal-entity`; there is no home-grown
  entity machinery here.
- A test describes the required behavior first; the implementation
  follows.

## Package boundaries

- `pkg/id`, `pkg/ref` — vocabulary; import nothing from this repository.
- `pkg/wire` — cross-component conventions (queues, search attributes);
  pure functions only.
- `pkg/entity/*` — system entities: definition + types + `Ops`
  interface; `Ops` implementations live in the server, not here.
- `pkg/pipeline` — the user-facing library; imports neither
  `pkg/entity/*` nor server packages (enforced by review; later by an
  import-graph test).
- `internal/` (to come) — the server: `Ops` implementations, API, the
  managed execution path.
- `cmd/` (to come) — binary assembly only.

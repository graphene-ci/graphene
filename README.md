# graphene

The graphene control plane server — the single door of an installation.
The full vision lives in `../GRAPHENE.MD` (org root).

This repository contains ONLY the server. Everything user-facing and
everything shared lives in
[pipeline](https://github.com/graphene-ci/pipeline): the pipeline
author's library, the identifier/reference vocabulary, the wire
conventions, and the temporal flows of the system resources. The server:

- registers the system resource flows (`pipeline/flow/*`) on its worker
  and implements their `Ops` (clouds, agent registry, blob store);
- implements the server activity contract of the pipeline library
  (`pipeline/wire`): declare machine/artifact, delete by owner;
- will serve the API for CLI/UI, hold secrets and tokens, and run the
  managed execution path (on-demand run workers).

Temporal is an implementation detail of the server; nothing here is a
public Go surface — the outside sees the binary and the API.

## Layout

| Path | What it is |
|---|---|
| `cmd/graphene-server` | The server binary (wiring lands next) |
| `internal/` (to come) | `Ops` implementations, API, managed execution path |

## Build and check

```bash
make configure   # pinned tools into bin/, nothing global
make lint
make test
make build
```

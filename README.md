# graphene

Control plane for infrastructure and runs on top of Temporal as the
single durable core. The full vision lives in `../GRAPHENE.MD` (org
root); this repository is its implementation.

A run is an ordinary Temporal workflow written by the user. The system
adds what neither CI, nor Terraform, nor bare Temporal has: ownership of
resource lifetimes, guaranteed teardown, working with things it did not
create, and "one-shot means at most once".

## Layout

| Package | What it is |
|---|---|
| `pkg/id` | Identifier dictionary (suffix `Id`); literals by cast, external input through `Parse*` |
| `pkg/ref` | References: `SecretRef`, `BlobRef`, `OwnerRef` — a reference travels, never the value |
| `pkg/wire` | Cross-component conventions: queue names, search attributes |
| `pkg/entity/machine` | Machine system entity: created in a cloud (owned, destroyable) or recognized over ssh (not owned, nothing to destroy with); ready = agent connected |
| `pkg/entity/artifact` | Artifact system entity: a record about where bytes live; deleting the record deletes the bytes |
| `pkg/pipeline` | Pipeline author's library: `OnAgent` (converging, on a machine), `Action` (one-shot: MaximumAttempts=1, undeterminable outcome = `ErrUnknown`), references |

System entities are built on
[temporal-entity](https://github.com/graphene-ci/temporal-entity): one
long-running workflow per resource — commands as updates, reconcile
ticks, deletion with a guaranteed finalizer. Their workflows execute on
the server's worker; side effects sit behind `Ops` interfaces implemented
by the server.

The server, CLI, and the managed execution path land in this repository
as they are built (order per the vision: entities and the library first).

## Build and check

```bash
make configure   # pinned tools into bin/, nothing global
make lint        # golangci-lint, 0 issues
make test
```

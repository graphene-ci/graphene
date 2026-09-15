# graphene

The Graphene control plane and the `graphenectl` CLI. The server stores
resource records, starts and recovers their durable processes on Temporal, and
manages runs, sources, revisions, secrets, RBAC and connected agents.

An installation has a single external entry point: one listener serves the
Management and worker APIs, agent connections, the Temporal proxy, OTLP
ingestion, health probes and the container-registry proxy. The user-facing Go
SDK lives in [`pipeline`](https://github.com/graphene-ci/pipeline); the full
product model and guides are in the [`docs`](https://graphene-ci.github.io/docs/).

## Install

Releases: [github.com/graphene-ci/graphene/releases](https://github.com/graphene-ci/graphene/releases).

**Server** — pull the image from GHCR:

```bash
docker pull ghcr.io/graphene-ci/graphene-server:0.1.0   # or :latest
```

**`graphenectl`** — install with Go:

```bash
go install github.com/graphene-ci/graphene/cmd/graphenectl@latest   # or @v0.1.0
```

or download a binary from the release page (linux/darwin/windows ×
amd64/arm64), unpack it and put it on your `PATH`:

```bash
tar xzf graphenectl_0.1.0_linux_amd64.tar.gz
sudo install graphenectl /usr/local/bin/
graphenectl login --server <host:port>
```

## Public TLS and internal workers

Keep `GRAPHENE_SERVER_EXTERNAL` as `host:port` and set
`GRAPHENE_SERVER_EXTERNAL_TLS=true` when a TLS proxy terminates the public
endpoint. Bootstrap downloads use HTTPS; agents, machine executors, Docker
managed workers and source builds use TLS to that address. Machine executors
receive a minted run token on both launch and resurrection; a configured static
run token is only a fallback. Missing credentials fail before container startup.
Bootstrap retries binary downloads up to ten times with a 30-second network
timeout and a two-second delay. It installs a successful nonempty download
atomically; exhausted retries fail without leaving a partial executable.

Kubernetes managed workers can use `GRAPHENE_SERVER_EXTERNAL_INTERNAL`
with their own `GRAPHENE_SERVER_EXTERNAL_INTERNAL_TLS` setting. Without an
internal address they inherit the public address and transport. Both TLS
flags default to false for local plaintext installations. Internal loopback
telemetry stays plaintext. The registry proxy makes upstream upload locations
relative to the door, including when accessed through a port forward.

## Layout

The kind dictionary retires an unused brought kind by sending its deletion
signal. That activity returns after signal acceptance so the calling record
can finish its audit and enter deletion. It never waits for its own workflow
to close; ordinary resource teardown still waits for closure.

| Path | Purpose |
|---|---|
| `cmd/graphene-server` | build and run the server |
| `cmd/graphenectl` | the general-purpose operator CLI |
| `proto/management` | the public Management API |
| `internal/services` | Management, worker and agent API implementations |
| `internal/worker` | the Temporal worker and system-process registration |
| `internal/*flow`, `internal/ops` | record lifecycles and external effects |
| `internal/auth`, `internal/authz` | authentication, tokens and RBAC |
| `internal/infrastructure` | blob and secret stores and integrations |
| `deployments/` | the container build of the dev installation |

## Local development

```bash
make configure
make lint
make test
make build
```

The full dev contour comes up with `make compose-up` and down with
`make compose-down`. That is a development environment, not a production
deployment; its limits and configuration are described in the docs.

## Release

Cutting a release is a pushed semver tag (`vX.Y.Z`):

```bash
make ver v=0.1.0        # or: make bump TYPE=minor
```

The tag drives the release workflow: goreleaser publishes the `graphenectl`
binaries to a GitHub Release, and the server image is built (embedding the
latest released agent selected by the release workflow) and pushed to GHCR as `:X.Y.Z` and `:latest`.

Resource ownership transfer activities emit heartbeats while waiting for the
entity command, including during resource creation. Cancellation and the
activity deadline still bound the wait; command errors reach the caller.

S3 uploads use the remaining size of seekable inputs. Non-seekable inputs are
spooled to a temporary file, removed on success or failure, before upload. This
avoids the SDK's large unknown-size allocation for small concurrent artifacts.

### Long-lived agent connections

AgentAPI.Session is a bidirectional gRPC stream. The reverse proxy must allow
an unbounded request body duration; for Traefik, set the HTTPS entrypoint's
`transport.respondingTimeouts.readTimeout` to `0s`. Its default 60-second
request read deadline terminates sessions even while heartbeats are flowing.

The server stops awaiting a command result when the command's agent session
ends. The caller receives an error and its activity retry can issue a new
idempotent command after reconnection. It does not treat reconnect as success
or keep waiting for a response on the obsolete stream.

### Historical metrics

Both `graphenectl metrics run/<id>` and `graphenectl run/<id> metrics`
accept `--start` and `--end` as RFC3339 timestamps. The flags also apply to
raw PromQL queries. Omitted bounds retain the server defaults: end now,
start one hour before end.

Scoped metrics queries accept both UTF-8 attribute labels (`graphene.run`)
and OTLP/Prometheus-normalized labels (`graphene_run`) in the same store.
Every selector branch retains the namespace filter.

Metrics responses are limited to 8 MiB. Oversized or invalid backend JSON
returns an error without a partial snapshot. For a large run, request an
individual resource or fewer metric names; use an explicit interval for
historical runs. This bound also applies to raw PromQL queries.

VM bootstrap requires a working `runc` before starting the agent. Package-manager
failures retry up to five times; apt waits for package locks and retries downloads.
Each installation attempt is bounded to two minutes when `timeout` is available.
Exhausted installation fails bootstrap instead of advertising a machine that
cannot execute activities.

Kubernetes managed workers use a deterministic Deployment name with a readable
prefix and a hash of the complete namespace/run identity. Long IDs and normalized
names cannot share a worker merely because their prefixes match. Start/Ensure
find existing workers by both identity labels, including deployments created by
older server versions; a conflicting deployment name is an error.

### Ответы команд stand

`accept`, `extend`, `release` возвращают `{"count": N}` — число сохранённых
корней после команды. Полный список и сроки доступны в `get stand/<pipeline>`
(`state.holdings`). Ответ не содержит копию списка: кеш последних 100 ответов
переносится через Continue-as-New и должен оставаться небольшим. Изменение
ответа не удаляет holdings, не меняет TTL и не ограничивает срок хранения
конфигов. Само состояние stand по-прежнему переносится в Temporal; для очень
больших списков сохраняется лимит размера workflow payload.

Kubernetes managed workers preserve complete namespace/run IDs in Deployment and
Pod annotations. Label values that exceed Kubernetes limits are encoded; valid
legacy labels remain compatible. Worker discovery and cleanup verify the complete
identity, so long suite cell IDs retain their original Temporal queue.

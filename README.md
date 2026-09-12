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

Kubernetes managed workers can use `GRAPHENE_SERVER_EXTERNAL_INTERNAL`
with their own `GRAPHENE_SERVER_EXTERNAL_INTERNAL_TLS` setting. Without an
internal address they inherit the public address and transport. Both TLS
flags default to false for local plaintext installations. Internal loopback
telemetry stays plaintext. The registry proxy makes upstream upload locations
relative to the door, including when accessed through a port forward.

## Layout

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

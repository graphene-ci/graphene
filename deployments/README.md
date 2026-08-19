# Deployments

The compose file at the repository root is the POLYGON: a complete
demo/dev installation in one command. Production delivery is a
separate stage (helm, a real Temporal cluster, external S3 and
telemetry) and deliberately not this file.

`docker compose up -d` brings up: Temporal (dev server, search
attributes registered by its start flags), minio, a docker registry
stored in minio, the Victoria telemetry stack (victoriametrics :8428,
victorialogs :9428, victoriatraces :10428 — each with its own web UI),
and the server.

The server is ONE door on `:7233`, split by content on the same
listener: gRPC (agents, workers, the Temporal proxy, the worker and
management planes), the ConnectRPC browser surface (plain JSON POSTs to
`/graphene.management.v1.<Service>/<Method>` with a bearer token),
`/healthz` probes, and the `/v2` registry proxy. Temporal, minio, and
the registry stay internal to the compose network.

TLS terminates in front of the door; the proxy speaks unencrypted
HTTP/2 to it, which carries every protocol at once. With caddy:

```
graphene.example.com {
    reverse_proxy h2c://127.0.0.1:7233
}
```

and set `GRAPHENE_SERVER_EXTERNAL` to the proxy's address.

The whole server configuration is environment variables — the dotted
YAML path uppercased under the `GRAPHENE` prefix
(`server.external` → `GRAPHENE_SERVER_EXTERNAL`); a YAML file
(`GRAPHENE_SERVER_CONFIG`) works too and the environment overlays it.
Before exposing the door anywhere: replace the dev tokens and set the
external addresses to the host's real ones.

Blobs live in minio (`blobs.backend: s3`); a single-node file backend
(`file`) exists for development. The registry uses minio's S3 API as
its storage driver.

Telemetry: workers and agents export OTLP to the door; the server
stamps the caller's namespace and forwards each signal to its backend
(`otel.traces/logs/metrics` — OTLP/HTTP ingest URLs, no collector in
between). The Victoria stack is chosen for its STANDARD query
surfaces: PromQL (metrics), the Jaeger API (traces); logs are the one
signal without a de-facto standard — that access is isolated behind a
driver. Dimensions 3-5 of every entity live here, correlated by the
graphene.* attributes.

The managed contour launches run-worker containers on this host through
the mounted docker socket.

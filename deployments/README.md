# Deployments

`docker compose up -d` at the repository root brings up a whole
installation: Postgres, Temporal (with the graphene search attributes
registered by a one-shot container), minio, a docker registry stored in
minio, and the server.

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
(`file`) exists for development. The registry uses minio's S3 API as its
storage driver — one stateful store for everything but Postgres.

The managed contour launches run-worker containers on this host through
the mounted docker socket.

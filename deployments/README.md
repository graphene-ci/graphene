# Deployments

`docker compose up -d` at the repository root brings up a whole
installation: Postgres, Temporal (with the graphene search attributes
registered by a one-shot container), minio, a docker registry stored in
minio, and the server.

The server is the DOOR: gRPC `:7233` (agents, workers, the Temporal
proxy, the worker plane), HTTP `:7280` (probes, the registry proxy),
and ConnectRPC `:7281` (the management API for browsers — plain JSON
POSTs to `/graphene.management.v1.<Service>/<Method>` with a bearer
token). Temporal, minio, and the registry stay internal to the compose
network.

The whole server configuration is environment variables — the dotted
YAML path uppercased under the `GRAPHENE` prefix
(`server.external_grpc` → `GRAPHENE_SERVER_EXTERNAL_GRPC`); a YAML file
(`GRAPHENE_SERVER_CONFIG`) works too and the environment overlays it.
Before exposing the door anywhere: replace the dev tokens and set the
external addresses to the host's real ones.

Blobs live in minio (`blobs.backend: s3`); a single-node file backend
(`file`) exists for development. The registry uses minio's S3 API as its
storage driver — one stateful store for everything but Postgres.

The managed contour launches run-worker containers on this host through
the mounted docker socket.

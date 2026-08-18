# Deployments

`docker compose up -d` at the repository root brings up a whole
installation: Postgres, Temporal (with the graphene search attributes
registered by a one-shot container), a docker registry, and the server.

The server is the SINGLE DOOR: gRPC `:7233` (agents, workers, the
Temporal proxy) and HTTP `:7280` (runs, blobs, secrets, the registry
proxy). Temporal and the registry stay internal to the compose network.

The whole server configuration is environment variables — the dotted
YAML path uppercased under the `GRAPHENE` prefix
(`server.external_grpc` → `GRAPHENE_SERVER_EXTERNAL_GRPC`); a YAML file
(`GRAPHENE_SERVER_CONFIG`) works too and the environment overlays it.
Before exposing the door anywhere: replace the dev tokens and set the
external addresses to the host's real ones.

The managed contour launches run-worker containers on this host through
the mounted docker socket.

# The production image of the graphene control plane: one static binary
# on distroless. The managed contour drives the host's docker through the
# mounted socket — no docker client inside the image.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /out/graphene-server ./cmd/graphene-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/graphene-server /graphene-server
USER 65532:65532
ENTRYPOINT ["/graphene-server"]

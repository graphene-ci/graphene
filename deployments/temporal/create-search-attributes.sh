#!/bin/sh
# One-shot: registers the custom search attributes graphene lives on —
# the entity kind/phase and the ownership tree mirrors. Idempotent: an
# already-registered attribute is not an error.
set -u
ADDRESS="${TEMPORAL_ADDRESS:-temporal:7233}"
NAMESPACE="${TEMPORAL_NAMESPACE:-default}"

until temporal operator namespace describe --namespace "$NAMESPACE" --address "$ADDRESS" >/dev/null 2>&1; do
  echo "waiting for temporal at $ADDRESS..."
  sleep 2
done

create() {
  temporal operator search-attribute create \
    --address "$ADDRESS" --namespace "$NAMESPACE" \
    --name "$1" --type "$2" 2>&1 | grep -v "already exists" || true
}

create EntityKind Keyword
create EntityPhase Keyword
create EntityOwner Keyword
create EntityKeepUntil Datetime
echo "search attributes ready"

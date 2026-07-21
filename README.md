# graphene

The core of Graphene CI: control plane, agent, and CLI.

## Why this repository exists

Graphene CI is a CI/CD platform where **infrastructure is first-class**:
users declare machines (ephemeral cloud, existing SSH hosts, local
containers) and jobs over them in typed Pkl; the system materializes the
machines, executes the job DAG, and guarantees teardown. This repository
owns the runtime that makes a declared workflow actually run.

## Components

- **Control plane** — durable run orchestration (eval → plan → provision →
  jobs → teardown), machine lifecycle with guaranteed teardown, agent hub,
  run state, plugin management (resolve/pin/deliver), secret subsystem,
  trigger subsystem, first-class observability.
- **Agent** — self-contained binary installed on machines by provisioning;
  dials home (outbound only), executes actions via plugin executors, streams
  logs and outputs, verifies machine capabilities before jobs.
- **CLI** — local loop (`eval` / `validate` / `dry-run`) with no server;
  server loop (`run` / `watch` / `cancel`); standalone `run --local` that
  hosts the orchestration core embedded — same code, different hosting;
  plugin author dev-kit.

## Status

Bootstrap skeleton. No code yet.

## Getting started

```sh
make configure   # set up a working environment from scratch
make help        # list all targets
```

All tools and built binaries live in `bin/` — nothing is installed
system-wide.

import { createClient, type Interceptor, type Transport } from '@connectrpc/connect'

import { AgentsAPI } from './proto/management/v1/agents_pb.ts'
import { NamespacesAPI } from './proto/management/v1/namespaces_pb.ts'
import { ObserveAPI } from './proto/management/v1/observe_pb.ts'
import { RbacAPI } from './proto/management/v1/rbac_pb.ts'
import { ResourcesAPI } from './proto/management/v1/resources_pb.ts'
import { RevisionsAPI } from './proto/management/v1/revisions_pb.ts'
import { RunsAPI } from './proto/management/v1/runs_pb.ts'
import { SecretsAPI } from './proto/management/v1/secrets_pb.ts'
import { SourceAPI } from './proto/management/v1/source_pb.ts'

/** Header that scopes a call to one namespace. */
export const NAMESPACE_HEADER = 'x-graphene-namespace'

export interface Credentials {
  token: string
  namespace: string
}

/** Adds the bearer token and the namespace scope. Credentials are read per
 * request, so a token or namespace change needs no new transport. */
export function authInterceptor(credentials: () => Credentials): Interceptor {
  return (next) => (req) => {
    const { token, namespace } = credentials()
    if (token !== '') req.header.set('Authorization', `Bearer ${token}`)
    // A per-call header wins: system-namespace reads (the namespace
    // dictionary lives in graphene-system) pass their own scope.
    if (namespace !== '' && !req.header.has(NAMESPACE_HEADER)) {
      req.header.set(NAMESPACE_HEADER, namespace)
    }
    return next(req)
  }
}

/** Builds one client per Management API service over a single transport. */
export function createGrapheneClient(transport: Transport) {
  return {
    agents: createClient(AgentsAPI, transport),
    namespaces: createClient(NamespacesAPI, transport),
    observe: createClient(ObserveAPI, transport),
    rbac: createClient(RbacAPI, transport),
    resources: createClient(ResourcesAPI, transport),
    revisions: createClient(RevisionsAPI, transport),
    runs: createClient(RunsAPI, transport),
    secrets: createClient(SecretsAPI, transport),
    source: createClient(SourceAPI, transport),
  }
}

export type GrapheneClient = ReturnType<typeof createGrapheneClient>

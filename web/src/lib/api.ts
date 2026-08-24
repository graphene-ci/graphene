import { createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'

import { NamespacesAPI } from '@/proto/management/v1/namespaces_pb'
import { ObserveAPI } from '@/proto/management/v1/observe_pb'
import { ResourcesAPI } from '@/proto/management/v1/resources_pb'
import { RunsAPI } from '@/proto/management/v1/runs_pb'
import { SecretsAPI } from '@/proto/management/v1/secrets_pb'
import { VarsAPI } from '@/proto/management/v1/vars_pb'

// Same-origin by default (vite dev proxy / production reverse proxy);
// override with VITE_API_URL for a detached backend.
const baseUrl: string = import.meta.env.VITE_API_URL ?? window.location.origin

const transport = createConnectTransport({ baseUrl })

export const api = {
  namespaces: createClient(NamespacesAPI, transport),
  observe: createClient(ObserveAPI, transport),
  resources: createClient(ResourcesAPI, transport),
  runs: createClient(RunsAPI, transport),
  secrets: createClient(SecretsAPI, transport),
  vars: createClient(VarsAPI, transport),
}

export type Api = typeof api

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { createRouterTransport } from '@connectrpc/connect'

import {
  authInterceptor,
  type Credentials,
  createGrapheneClient,
  NAMESPACE_HEADER,
} from './index.ts'
import { NamespacesAPI } from './proto/management/v1/namespaces_pb.ts'

interface Seen {
  authorization: string | null
  namespace: string | null
}

// Serves NamespacesAPI in memory and reports the headers each call carried.
function serve(credentials: () => Credentials) {
  const seen: Seen[] = []
  const transport = createRouterTransport(
    ({ service }) => {
      service(NamespacesAPI, {
        serverInfo: (_req, ctx) => {
          seen.push({
            authorization: ctx.requestHeader.get('Authorization'),
            namespace: ctx.requestHeader.get(NAMESPACE_HEADER),
          })
          return { version: '1.2.3' }
        },
      })
    },
    { transport: { interceptors: [authInterceptor(credentials)] } },
  )
  return { client: createGrapheneClient(transport), seen }
}

describe('authInterceptor', { concurrency: true }, () => {
  const cases: { name: string; credentials: Credentials; callNamespace?: string; want: Seen }[] = [
    {
      name: 'token and namespace',
      credentials: { token: 't0k', namespace: 'default' },
      want: { authorization: 'Bearer t0k', namespace: 'default' },
    },
    {
      name: 'empty credentials send nothing',
      credentials: { token: '', namespace: '' },
      want: { authorization: null, namespace: null },
    },
    {
      name: 'per-call namespace wins',
      credentials: { token: 't0k', namespace: 'default' },
      callNamespace: 'graphene-system',
      want: { authorization: 'Bearer t0k', namespace: 'graphene-system' },
    },
  ]
  for (const c of cases) {
    test(c.name, async () => {
      const { client, seen } = serve(() => c.credentials)
      const headers =
        c.callNamespace === undefined ? undefined : { [NAMESPACE_HEADER]: c.callNamespace }
      const res = await client.namespaces.serverInfo({}, { headers })
      assert.equal(res.version, '1.2.3')
      assert.deepEqual(seen, [c.want])
    })
  }

  test('credentials are read per request', async () => {
    let current: Credentials = { token: 'first', namespace: 'a' }
    const { client, seen } = serve(() => current)
    await client.namespaces.serverInfo({})
    current = { token: 'second', namespace: 'b' }
    await client.namespaces.serverInfo({})
    assert.deepEqual(seen, [
      { authorization: 'Bearer first', namespace: 'a' },
      { authorization: 'Bearer second', namespace: 'b' },
    ])
  })
})

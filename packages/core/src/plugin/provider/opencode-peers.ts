export * as OpencodePeersPlugin from "./opencode-peers.js"

import { define } from "@opencode/plugin/effect/global"
import { Duration, Effect, Schedule, Schema, Scope } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { credentialHeaders, methodID, oauth, OpencodePlugin } from "./opencode.js"

// The opencode-cloud peer directory: one list of servers per OpenCode Console account.
const defaultDirectory = "https://opencode-peers.anomalyco.workers.dev"

const Directory = Schema.Struct({
  instances: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      version: Schema.optional(Schema.String),
      address: Schema.Struct({ relay: Schema.NullOr(Schema.String), addresses: Schema.Array(Schema.String) }),
      updated: Schema.Number,
    }),
  ),
})

/**
 * Server-wide phase of the Console plugin: announces this server to the signed-in account's peer directory and keeps
 * the account's other servers in the peer list.
 */
export const Plugin = define<HttpClient.HttpClient | Scope.Scope>({
  id: OpencodePlugin.id,
  effect: Effect.fn("OpencodePeersPlugin")(function* (ctx) {
    const http = yield* HttpClient.HttpClient
    const base = (process.env.OPENCODE_PEERS_URL || defaultDirectory).replace(/\/+$/, "")
    const listed = new Set<string>()

    const headers = Effect.gen(function* () {
      const stored = yield* ctx.credential.active("opencode")
      if (stored?.value.type !== "oauth" && stored?.value.type !== "key") return undefined
      const value =
        stored.value.type === "oauth" &&
        stored.value.methodID === methodID &&
        stored.value.expires <= Date.now() + Duration.toMillis(Duration.minutes(5))
          ? yield* oauth(http)
              .refresh(stored.value)
              .pipe(Effect.tap((refreshed) => ctx.credential.update(stored.id, refreshed)))
          : stored.value
      const accountID = value.metadata?.accountID
      return { ...credentialHeaders(value), ...(typeof accountID === "string" ? { "x-account-id": accountID } : {}) }
    })
    const send = (request: HttpClientRequest.HttpClientRequest, auth: Record<string, string>) =>
      http.execute(HttpClientRequest.setHeaders(request, auth)).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk))

    const sync = Effect.gen(function* () {
      const self = yield* ctx.peer.self()
      const auth = yield* headers
      const instances =
        self && auth
          ? yield* Effect.gen(function* () {
              yield* send(
                HttpClientRequest.put(`${base}/v1/instances/${self.id}`).pipe(
                  HttpClientRequest.bodyJsonUnsafe({ name: self.name, version: self.version, address: self.address }),
                ),
                auth,
              )
              const response = yield* send(HttpClientRequest.get(`${base}/v1/instances`), auth)
              return (yield* HttpClientResponse.schemaBodyJson(Directory)(response)).instances
            })
          : []
      const others = instances.filter((instance) => instance.id !== self?.id)
      yield* Effect.forEach(others, (instance) => ctx.peer.add(instance))
      const current = new Set(others.map((instance) => instance.id))
      yield* Effect.forEach(
        [...listed].filter((id) => !current.has(id)),
        (id) => ctx.peer.remove(id),
      )
      listed.clear()
      current.forEach((id) => listed.add(id))
    })

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const self = yield* ctx.peer.self()
        const auth = yield* headers
        if (self && auth) yield* send(HttpClientRequest.delete(`${base}/v1/instances/${self.id}`), auth)
      }).pipe(Effect.timeout("3 seconds"), Effect.ignore),
    )
    yield* sync.pipe(
      Effect.catch((error) => Effect.logWarning("peer directory sync failed", { error: String(error) })),
      Effect.repeat(Schedule.spaced("60 seconds")),
      Effect.forkScoped,
    )
  }),
})

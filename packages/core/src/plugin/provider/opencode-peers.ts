export * as OpencodePeersPlugin from "./opencode-peers.js"

import { define } from "@opencode/plugin/effect/global"
import { Effect, Schedule, Schema, Scope } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { credentialHeaders, methodID, oauth, OpencodePlugin } from "./opencode.js"

const Directory = Schema.Struct({
  instances: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      version: Schema.optional(Schema.String),
      kind: Schema.Literals(["server", "client"]),
      address: Schema.optional(
        Schema.Struct({ relay: Schema.NullOr(Schema.String), addresses: Schema.Array(Schema.String) }),
      ),
      updated: Schema.Number,
    }),
  ),
})

/** Server-wide phase of the Console plugin: syncs the peer list with the signed-in account's peer directory. */
export const Plugin = define<HttpClient.HttpClient | Scope.Scope>({
  id: OpencodePlugin.id,
  effect: Effect.fn("OpencodePeersPlugin")(function* (ctx) {
    const http = yield* HttpClient.HttpClient
    const base = process.env.OPENCODE_PEERS_URL || "https://opencode-peers.anomalyco.workers.dev"
    const listed = new Set<string>()

    const headers = Effect.gen(function* () {
      const stored = yield* ctx.credential.active("opencode")
      if (stored?.value.type !== "oauth" && stored?.value.type !== "key") return undefined
      const value =
        stored.value.type === "oauth" &&
        stored.value.methodID === methodID &&
        stored.value.expires < Date.now() + 300_000
          ? yield* oauth(http)
              .refresh(stored.value)
              .pipe(Effect.tap((refreshed) => ctx.credential.update(stored.id, refreshed)))
          : stored.value
      const accountID = value.metadata?.accountID
      return { ...credentialHeaders(value), ...(typeof accountID === "string" ? { "x-account-id": accountID } : {}) }
    })
    const send = (request: HttpClientRequest.HttpClientRequest, auth: Record<string, string>) =>
      http.execute(HttpClientRequest.setHeaders(request, auth)).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk))

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const self = yield* ctx.peer.self()
        const auth = yield* headers
        if (self && auth) yield* send(HttpClientRequest.delete(`${base}/v1/instances/${self.id}`), auth)
      }).pipe(Effect.timeout("3 seconds"), Effect.ignore),
    )
    // Announce this server, then replace this plugin's peers with the account's other servers.
    yield* Effect.gen(function* () {
      const self = yield* ctx.peer.self()
      const auth = yield* headers
      const instances =
        !self || !auth
          ? []
          : yield* Effect.gen(function* () {
              yield* send(
                HttpClientRequest.put(`${base}/v1/instances/${self.id}`).pipe(HttpClientRequest.bodyJsonUnsafe(self)),
                auth,
              )
              const response = yield* send(HttpClientRequest.get(`${base}/v1/instances`), auth)
              return (yield* HttpClientResponse.schemaBodyJson(Directory)(response)).instances
            })
      const others = instances.filter((instance) => instance.id !== self?.id)
      yield* Effect.forEach(others, ctx.peer.add)
      const current = new Set(others.map((instance) => instance.id))
      yield* Effect.forEach(
        [...listed].filter((id) => !current.has(id)),
        ctx.peer.remove,
      )
      listed.clear()
      current.forEach((id) => listed.add(id))
    }).pipe(
      Effect.catch((error) => Effect.logWarning("peer directory sync failed", { error: String(error) })),
      Effect.repeat(Schedule.spaced("15 seconds")),
      Effect.forkScoped,
    )
  }),
})

export * as ServerPeers from "./peers"

import { ConfigDiscovery } from "@opencode/core/config/discovery"
import type { PeerProvider } from "@opencode/plugin/peer"
import { Global } from "@opencode/util/global"
import { Effect, FileSystem, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpServerRequest, HttpServerResponse } from "effect/http"
import { Socket } from "effect/socket"
import { parse } from "jsonc-parser"
import path from "path"

const GlobalConfig = Schema.Struct({
  peers: Schema.optional(
    Schema.Array(
      Schema.Struct({
        package: Schema.String,
        options: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
      }),
    ),
  ),
})

const PEER_PATH = /^\/api\/peers\/([^/]+)(\/.*)$/

export function matches(pathname: string) {
  return pathname === "/api/peers" || PEER_PATH.test(pathname)
}

const forward = Effect.fnUntraced(function* (from: Socket.Socket, to: Socket.Socket) {
  const reader = yield* from.reader
  const writer = yield* to.writer
  while (true) yield* writer.writeAll(yield* reader.pull)
})

/**
 * Starts the providers listed under `peers` in the global config for the lifetime of the current scope.
 * Project config cannot add providers.
 */
export const start = Effect.fn("ServerPeers.start")(function* (server: {
  url: string
  headers: Record<string, string>
}) {
  const global = yield* Global.Service
  const fs = yield* FileSystem.FileSystem
  const http = yield* HttpClient.HttpClient
  const configs = yield* Effect.forEach(ConfigDiscovery.names, (name) =>
    Effect.gen(function* () {
      const file = path.join(global.config, name)
      if (!(yield* fs.exists(file))) return {}
      return yield* Schema.decodeUnknownEffect(GlobalConfig)(parse(yield* fs.readFileString(file)))
    }),
  )
  const providers = yield* Effect.forEach(configs.findLast((config) => config.peers)?.peers ?? [], (entry) =>
    Effect.acquireRelease(
      Effect.tryPromise(async () => {
        const module: { default: PeerProvider } = await import(entry.package)
        return module.default({ options: entry.options ?? {}, server })
      }),
      (peers) => Effect.promise(() => peers.close()),
    ),
  )

  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const url = new URL(request.url, "http://localhost")
    if (url.pathname === "/api/peers") return HttpServerResponse.jsonUnsafe(providers.flatMap((p) => p.list()))

    const [, encoded, rest] = PEER_PATH.exec(url.pathname) ?? []
    const id = decodeURIComponent(encoded)
    const provider = providers.find((p) => p.list().some((peer) => peer.id === id))
    if (!provider) return HttpServerResponse.empty({ status: 404 })

    const target = new URL(yield* Effect.tryPromise(() => provider.connect(id)))
    target.pathname = rest
    target.search = url.search
    // Local credentials never leave this server.
    target.searchParams.delete("auth_token")

    if (request.headers.upgrade?.toLowerCase() === "websocket") {
      target.protocol = target.protocol === "https:" ? "wss:" : "ws:"
      const remote = yield* Socket.makeWebSocket(target.href).pipe(
        Effect.provide(Socket.layerWebSocketConstructorGlobal),
      )
      const client = yield* request.upgrade
      yield* Effect.raceFirst(forward(client, remote), forward(remote, client)).pipe(Effect.ignore)
      return HttpServerResponse.empty()
    }

    const response = yield* http.execute(
      HttpServerRequest.toClientRequest(request).pipe(
        HttpClientRequest.setUrl(target),
        HttpClientRequest.removeHeader("authorization"),
        HttpClientRequest.removeHeader("cookie"),
        HttpClientRequest.removeHeader("host"),
      ),
    )
    return HttpServerResponse.fromClientResponse(response)
  }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 502 })))
})

export * as ServerPeers from "./peers"

import type { Peers } from "@opencode/plugin/peer"
import { Effect } from "effect"
import { HttpClient, HttpClientRequest, HttpServerRequest, HttpServerResponse } from "effect/http"
import { Socket } from "effect/socket"

const PEER_PATH = /^\/api\/peers\/([^/]+)(\/.*)$/

export function matches(pathname: string) {
  return pathname === "/api/peers" || PEER_PATH.test(pathname)
}

const forward = Effect.fnUntraced(function* (from: Socket.Socket, to: Socket.Socket) {
  const reader = yield* from.reader
  const writer = yield* to.writer
  while (true) yield* writer.writeAll(yield* reader.pull)
})

/** Serves `/api/peers` from the peers that server plugins provide. */
export const make = Effect.fn("ServerPeers.make")(function* (providers: readonly Peers[]) {
  const http = yield* HttpClient.HttpClient

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

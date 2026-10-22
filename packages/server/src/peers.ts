export * as Peers from "./peers"

import type { BiStream, Connection } from "@number0/iroh/index.js"
import { Peer } from "@opencode/core/peer"
import type { PeerEntry } from "@opencode/plugin/effect/global"
import { Global } from "@opencode/util/global"
import { Context, Effect, FiberSet, Layer, Schema, Scope, Semaphore } from "effect"
import { NetAddress } from "effect/net"
import { Socket } from "effect/socket"
import { randomBytes } from "node:crypto"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { hostname } from "node:os"
import path from "node:path"
import { ServerAuth } from "./auth"

const ALPN = Array.from(new TextEncoder().encode("opencode/peer/1"))
const HELLO = Array.from(new TextEncoder().encode("opencode/peer/1 hello"))
const CHUNK = 64 * 1024

/** A loopback URL that reaches the peer, and the token its server accepts as the Basic password for user "opencode". */
interface Gateway {
  readonly url: string
  readonly token: string
}

export interface Options {
  /** Name shown to the account's other servers; defaults to the hostname. */
  readonly name?: string
  readonly target: { readonly hostname: string; readonly port: number }
  readonly version?: string
}

export class Unavailable extends Schema.TaggedError<Unavailable>()("Peers.Unavailable", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export interface Interface {
  /** The peer list that server-wide plugins maintain, without this server. */
  readonly list: () => Effect.Effect<ReadonlyArray<PeerEntry>, Unavailable>
  readonly connect: (id: string) => Effect.Effect<Gateway, Unavailable>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Peers") {}

const unavailable = (message: string) =>
  Service.of({
    list: () => Effect.fail(new Unavailable({ message })),
    connect: () => Effect.fail(new Unavailable({ message })),
  })

export const disabled = Layer.succeed(Service, unavailable("Peer connections are not enabled on this server"))

export const layer = (options: Options) =>
  Layer.effect(
    Service,
    make(options).pipe(
      Effect.catch((error) =>
        Effect.logWarning("peer connections unavailable", { error: error.message }).pipe(
          Effect.as(unavailable(error.message)),
        ),
      ),
    ),
  )

const make = Effect.fnUntraced(function* (options: Options) {
  // Loaded lazily so runtimes that never enable peers do not load the native binding.
  const { Endpoint, EndpointAddr, EndpointId, RelayMode } = yield* Effect.tryPromise({
    try: () => import("@number0/iroh/index.js"),
    catch: (cause) => new Unavailable({ message: "Failed to load the iroh binding", cause }),
  })
  const { makeNet } = yield* Effect.promise(() => import("@effect/platform-node/NodeSocket"))
  const NodeSocketServer = yield* Effect.promise(() => import("@effect/platform-node/NodeSocketServer"))
  const global = yield* Global.Service
  const registry = yield* Peer.Service
  const auth = yield* ServerAuth.Config

  const file = path.join(global.data, "peer.key")
  const secret = yield* Effect.tryPromise({
    try: async () => {
      const existing = await readFile(file).catch(() => undefined)
      if (existing?.length === 32) return existing
      const created = randomBytes(32)
      await mkdir(path.dirname(file), { recursive: true })
      await writeFile(file, created, { mode: 0o600 })
      return created
    },
    catch: (cause) => new Unavailable({ message: "Failed to load the peer identity", cause }),
  })
  const endpoint = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () => {
        // The minimal preset publishes nothing to public discovery; the directory carries addresses instead.
        const builder = Endpoint.builder()
        builder.applyMinimal()
        builder.secretKey(Array.from(secret))
        builder.alpns([ALPN])
        builder.relayMode(RelayMode.defaultMode())
        return builder.bind()
      },
      catch: (cause) => new Unavailable({ message: "Failed to start the peer endpoint", cause }),
    }),
    (endpoint) => Effect.promise(() => endpoint.close()),
  )
  const self = endpoint.id().toString()

  yield* registry.setSelf(
    Effect.gen(function* () {
      // Waits for a home relay on first use only; direct addresses are still announced without one.
      yield* Effect.promise(() => endpoint.online()).pipe(Effect.timeout("10 seconds"), Effect.ignore)
      const current = endpoint.addr()
      // Wildcard sockets are not dialable; loopback forms make same-host peers work immediately.
      const bound = endpoint
        .boundSockets()
        .map((item) => item.replace(/^0\.0\.0\.0:/, "127.0.0.1:").replace(/^\[::\]:/, "[::1]:"))
      return {
        id: self,
        name: options.name || hostname(),
        version: options.version,
        address: { relay: current.relayUrl(), addresses: [...new Set([...current.directAddresses(), ...bound])] },
      }
    }),
  )
  const peers = registry.list().pipe(Effect.map((list) => list.filter((peer) => peer.id !== self)))

  // Inbound: only servers in the peer list get a session token and a pipe to this server.
  const serve = Effect.fn("Peers.serve")(
    function* (connection: Connection) {
      const remote = connection.remoteId().toString()
      if (!(yield* peers).some((peer) => peer.id === remote)) return
      const token = ServerAuth.issueSession(auth)
      if (token === undefined) return
      const control = yield* Effect.tryPromise(() => connection.acceptBi())
      yield* Effect.tryPromise(() => control.recv.readToEnd(256))
      yield* Effect.tryPromise(() => control.send.writeAll(Array.from(new TextEncoder().encode(token))))
      yield* Effect.tryPromise(() => control.send.finish())
      while (true) {
        const stream = yield* Effect.tryPromise(() => connection.acceptBi())
        const local = yield* makeNet({ host: options.target.hostname, port: options.target.port })
        yield* pipe(local, stream).pipe(Effect.ignore, Effect.forkScoped)
      }
    },
    Effect.scoped,
    (effect, connection) => effect.pipe(Effect.ignore, Effect.ensuring(Effect.sync(() => connection.close(0n, [])))),
  )
  const accepted = yield* FiberSet.make()
  yield* Effect.gen(function* () {
    while (true) {
      const pending = yield* Effect.tryPromise(() => endpoint.acceptNext()).pipe(Effect.orElseSucceed(() => null))
      if (pending === null) return
      yield* FiberSet.run(
        accepted,
        Effect.tryPromise(async () => (await pending.accept()).connect()).pipe(Effect.flatMap(serve), Effect.ignore),
      )
    }
  }).pipe(Effect.forkScoped)

  // Outbound: one loopback listener per peer for the server's lifetime. Each TCP connection becomes a bi-stream on the
  // peer's iroh connection, which is re-dialed after it drops so clients keep their URL and token.
  const layerScope = yield* Scope.Scope
  const gateways = new Map<string, Gateway & { connection: Connection }>()
  const lock = yield* Semaphore.make(1)
  yield* Effect.addFinalizer(() => Effect.sync(() => gateways.forEach((gateway) => gateway.connection.close(0n, []))))

  const dial = Effect.fn("Peers.dial")(function* (id: string) {
    const instance = (yield* peers).find((peer) => peer.id === id)
    if (!instance) return yield* new Unavailable({ message: `Peer not found: ${id}` })
    // iroh's TLS handshake proves the remote holds the dialed endpoint ID's key.
    const connection = yield* Effect.tryPromise({
      try: async () =>
        endpoint.connect(
          new EndpointAddr(EndpointId.fromString(id), instance.address.relay, [...instance.address.addresses]),
          ALPN,
        ),
      catch: (cause) => new Unavailable({ message: `Could not reach peer ${instance.name}`, cause }),
    })
    const token = yield* Effect.gen(function* () {
      const control = yield* Effect.tryPromise(() => connection.openBi())
      yield* Effect.tryPromise(() => control.send.writeAll(HELLO))
      yield* Effect.tryPromise(() => control.send.finish())
      const reply = yield* Effect.tryPromise(() => control.recv.readToEnd(4096))
      if (reply.length === 0) return yield* Effect.fail("empty token")
      return new TextDecoder().decode(Uint8Array.from(reply))
    }).pipe(
      Effect.mapError((cause) => new Unavailable({ message: `Peer ${instance.name} refused the connection`, cause })),
      Effect.onError(() => Effect.sync(() => connection.close(0n, []))),
    )
    return { connection, token }
  })

  // The first token stays valid across re-dials (it lasts until the peer's password changes or it expires).
  const refresh = Effect.fn("Peers.refresh")(function* (id: string, gateway: Gateway & { connection: Connection }) {
    if (gateway.connection.closeReason() === null) return gateway.connection
    gateway.connection = (yield* dial(id)).connection
    return gateway.connection
  })

  const open = Effect.fn("Peers.open")(function* (id: string) {
    const dialed = yield* dial(id)
    const server = yield* NodeSocketServer.make({ host: "127.0.0.1", port: 0 }).pipe(
      Scope.provide(layerScope),
      Effect.mapError((cause) => new Unavailable({ message: "Failed to open a local peer listener", cause })),
      Effect.onError(() => Effect.sync(() => dialed.connection.close(0n, []))),
    )
    const port = NetAddress.isInetAddress(server.address) ? server.address.port : 0
    const gateway = { url: `http://127.0.0.1:${port}`, token: dialed.token, connection: dialed.connection }
    yield* server
      .run((socket) =>
        lock
          .withPermits(1)(refresh(id, gateway))
          .pipe(
            Effect.flatMap((connection) => Effect.tryPromise(() => connection.openBi())),
            Effect.flatMap((stream) => pipe(socket, stream)),
            Effect.ignore,
          ),
      )
      .pipe(Effect.ignore, Effect.forkIn(layerScope))
    return gateway
  })

  return Service.of({
    list: () => peers,
    connect: Effect.fn("Peers.connect")(
      function* (id) {
        const existing = gateways.get(id)
        if (existing) {
          yield* refresh(id, existing)
          return { url: existing.url, token: existing.token }
        }
        const gateway = yield* open(id)
        gateways.set(id, gateway)
        return { url: gateway.url, token: gateway.token }
      },
      (effect) => lock.withPermits(1)(effect),
    ),
  })
})

const isEnd = (error: unknown) => Socket.isSocketError(error) && error.reason._tag === "SocketCloseError"

/** Copy raw bytes between a TCP socket and an iroh bi-stream until both directions end. */
const pipe = Effect.fn("Peers.pipe")(
  function* (socket: Socket.Socket, stream: BiStream) {
    const pull = yield* Socket.readerBytes(socket)
    const upload = Effect.gen(function* () {
      while (true) {
        for (const chunk of yield* pull) {
          // The binding only accepts number[] buffers.
          yield* Effect.tryPromise(() => stream.send.writeAll(Array.from(chunk)))
        }
      }
    }).pipe(Effect.catchIf(isEnd, () => Effect.tryPromise(() => stream.send.finish())))
    // Releasing the writer's scope half-closes the TCP socket once the peer finishes.
    const download = Effect.gen(function* () {
      const writer = yield* socket.writer
      while (true) {
        const bytes = yield* Effect.tryPromise(() => stream.recv.read(CHUNK))
        if (bytes.length === 0) return
        yield* writer.write(Uint8Array.from(bytes))
      }
    }).pipe(Effect.scoped)
    yield* Effect.all([upload, download], { concurrency: 2, discard: true })
  },
  Effect.scoped,
  (effect, _socket, stream) =>
    effect.pipe(
      Effect.onError(() =>
        Effect.promise(async () => {
          await stream.send.reset(0n).catch(() => {})
          await stream.recv.stop(0n).catch(() => {})
        }),
      ),
    ),
)

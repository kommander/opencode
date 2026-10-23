export * as Peers from "./peers"

import type { BiStream, Connection } from "@opencode-ai/iroh"
import { Peer } from "@opencode/core/peer"
import type { PeerEntry, PeerRelay } from "@opencode/plugin/effect/global"
import { Global } from "@opencode/util/global"
import {
  Context,
  Deferred,
  Duration,
  Effect,
  FiberSet,
  Layer,
  Option,
  Schedule,
  Schema,
  Scope,
  Semaphore,
} from "effect"
import { NetAddress } from "effect/net"
import { Socket } from "effect/socket"
import { randomBytes } from "node:crypto"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { hostname } from "node:os"
import path from "node:path"
import { ServerAuth } from "./auth"

const ALPN = "opencode/peer/1"
const HELLO = new TextEncoder().encode("opencode/peer/1 hello")
const CHUNK = 64 * 1024
const BLOCK_MS = 60 * 60 * 1000

/** A loopback URL that reaches the peer, and the token its server accepts as the Basic password for user "opencode". */
interface Gateway {
  readonly url: string
  readonly token: string
}

/** A gateway with its current iroh connection, and the error of its latest failed re-dial. */
interface Dialed extends Gateway {
  connection: Connection
  failure?: Unavailable
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
  /** Servers in the peer list, and whether this server holds a live connection to each. */
  readonly list: () => Effect.Effect<ReadonlyArray<PeerEntry & { readonly connected: boolean }>, Unavailable>
  readonly connect: (id: string) => Effect.Effect<Gateway, Unavailable>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Peers") {}

const unavailable = (message: string) =>
  Service.of({
    list: () => Effect.fail(new Unavailable({ message })),
    connect: () => Effect.fail(new Unavailable({ message })),
  })

const disabledService = unavailable("Peer connections are not enabled on this server")
export const disabled = Layer.succeed(Service, disabledService)

// Peer connections stay off until a server-wide plugin provides the relays they use.
export const layer = (options: Options) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const registry = yield* Peer.Service
      const ready = yield* Deferred.make<Interface>()
      yield* registry.relay.pipe(
        Effect.flatMap((relay) => make(options, relay)),
        Effect.catch((error) =>
          Effect.logWarning("peer connections unavailable", { error: error.message }).pipe(
            Effect.as(unavailable(error.message)),
          ),
        ),
        Effect.flatMap((service) => Deferred.succeed(ready, service)),
        Effect.forkScoped,
      )
      const current = Deferred.poll(ready).pipe(
        Effect.flatMap(Option.match({ onNone: () => Effect.succeed(disabledService), onSome: (service) => service })),
      )
      return Service.of({
        list: () => current.pipe(Effect.flatMap((service) => service.list())),
        connect: (id) => current.pipe(Effect.flatMap((service) => service.connect(id))),
      })
    }),
  )

const make = Effect.fnUntraced(function* (options: Options, relay: PeerRelay) {
  // Loaded lazily so runtimes that never enable peers do not load the native binding.
  const { bind } = yield* Effect.tryPromise({
    try: () => import("@opencode-ai/iroh"),
    catch: (cause) => new Unavailable({ message: "Failed to load the iroh binding", cause }),
  })
  const { makeNet, NetSocket } = yield* Effect.promise(() => import("@effect/platform-node/NodeSocket"))
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
      // Publishes nothing to public discovery; the directory carries addresses instead.
      // A dead peer's connection closes within ~10 s, so gateways re-dial instead of hanging.
      try: () => bind({ secretKey: secret, alpn: ALPN, idleTimeoutMs: 10_000, relayUrls: [...relay.urls] }),
      catch: (cause) => new Unavailable({ message: "Failed to start the peer endpoint", cause }),
    }),
    (endpoint) => Effect.promise(() => endpoint.close()),
  )
  const self = endpoint.id()

  // Announced without waiting for a home relay: the relays admit this endpoint only with a token, which plugins
  // issue only for an existing directory entry.
  yield* registry.setSelf(
    Effect.sync(() => {
      const current = endpoint.addr()
      // Wildcard sockets are not dialable; loopback forms make same-host peers work immediately.
      const bound = endpoint
        .boundSockets()
        .map((item) => item.replace(/^0\.0\.0\.0:/, "127.0.0.1:").replace(/^\[::\]:/, "[::1]:"))
      return {
        id: self,
        kind: "server" as const,
        name: options.name || hostname(),
        // Runs from source report the placeholder version "local", which reads like "this machine" in peer lists.
        version: options.version === "local" ? "dev" : options.version,
        address: { relay: current.relay, addresses: [...new Set([...current.addresses, ...bound])] },
      }
    }),
  )
  // Plugins that started before this endpoint had nothing to announce yet.
  yield* registry.refresh().pipe(Effect.forkScoped)
  // The relays admit this endpoint only with a current token: renew it at 80% of its lifetime. Failures retry from 2
  // seconds backing off to a minute, so a server that started before its network was up comes online quickly.
  yield* relay.token.pipe(
    Effect.tapError((error) => Effect.logWarning("peer relay token unavailable", { error: String(error) })),
    Effect.retry(Schedule.min([Schedule.exponential("2 seconds"), Schedule.spaced("60 seconds")])),
    Effect.tap((token) => Effect.promise(() => endpoint.setRelayToken(token.token))),
    Effect.map((token) => Duration.millis(Math.max(60_000, (token.expires - Date.now()) * 0.8))),
    Effect.flatMap(Effect.sleep),
    Effect.forever,
    Effect.forkScoped,
  )
  // Announce the relay URL as soon as the first token brings the endpoint online, not at the next plugin heartbeat.
  yield* Effect.promise(() => endpoint.online()).pipe(Effect.andThen(registry.refresh()), Effect.forkScoped)
  const peers = registry.list().pipe(Effect.map((list) => list.filter((peer) => peer.id !== self)))
  // Only servers can be dialed; clients connect in.
  const servers = peers.pipe(Effect.map((list) => list.filter((peer) => peer.kind === "server")))

  // An unknown caller triggers at most one peer list refresh every 5 seconds. Three unknown attempts in a row block
  // its key from triggering refreshes for an hour; a blocked key is still admitted once it is in the peer list.
  const failures = new Map<string, { readonly count: number; readonly at: number }>()
  const refreshed = { at: 0 }
  const known = (remote: string) => peers.pipe(Effect.map((list) => list.some((peer) => peer.id === remote)))
  const admit = Effect.fn("Peers.admit")(function* (remote: string) {
    const now = Date.now()
    const previous = failures.get(remote)
    const recent = previous && now - previous.at < BLOCK_MS ? previous.count : 0
    if (!(yield* known(remote)) && recent < 3 && now - refreshed.at >= 5_000) {
      refreshed.at = now
      yield* registry.refresh()
    }
    if (yield* known(remote)) {
      failures.delete(remote)
      return true
    }
    if (recent >= 3) return false
    failures.forEach((entry, id) => now - entry.at >= BLOCK_MS && failures.delete(id))
    failures.set(remote, { count: recent + 1, at: now })
    return false
  })

  // Inbound: only peers in the peer list get a session token and a pipe to this server.
  const serve = Effect.fn("Peers.serve")(
    function* (connection: Connection) {
      if (!(yield* admit(connection.remoteId()))) return
      const token = ServerAuth.issueSession(auth)
      if (token === undefined) return
      const control = yield* Effect.tryPromise(() => connection.acceptBi())
      yield* Effect.tryPromise(() => control.recv.readToEnd(256))
      yield* Effect.tryPromise(() => control.send.writeAll(new TextEncoder().encode(token)))
      yield* Effect.tryPromise(() => control.send.finish())
      while (true) {
        const stream = yield* Effect.tryPromise(() => connection.acceptBi())
        const local = yield* makeNet({ host: options.target.hostname, port: options.target.port })
        yield* pipe(local, stream).pipe(Effect.ignore, Effect.forkScoped)
      }
    },
    Effect.scoped,
    (effect, connection) => effect.pipe(Effect.ignore, Effect.ensuring(Effect.sync(() => connection.close()))),
  )
  const accepted = yield* FiberSet.make()
  yield* Effect.gen(function* () {
    while (true) {
      const pending = yield* Effect.tryPromise(() => endpoint.accept()).pipe(Effect.orElseSucceed(() => null))
      if (pending === null) return
      yield* FiberSet.run(
        accepted,
        Effect.tryPromise(() => pending.accept()).pipe(Effect.flatMap(serve), Effect.ignore),
      )
    }
  }).pipe(Effect.forkScoped)

  // Outbound: one loopback listener per peer for the server's lifetime. Each TCP connection becomes a bi-stream on the
  // peer's iroh connection, which is re-dialed after it drops so clients keep their URL and token.
  const layerScope = yield* Scope.Scope
  const gateways = new Map<string, Dialed>()
  // Dials are serialized per peer, so an unreachable peer never delays requests to the others.
  const locks = new Map<string, Semaphore.Semaphore>()
  const locked = <A, E, R>(id: string, effect: Effect.Effect<A, E, R>) => {
    const lock = locks.get(id) ?? Semaphore.makeUnsafe(1)
    locks.set(id, lock)
    return lock.withPermits(1)(effect)
  }
  yield* Effect.addFinalizer(() => Effect.sync(() => gateways.forEach((gateway) => gateway.connection.close())))

  const dial = Effect.fn("Peers.dial")(function* (id: string) {
    const instance = (yield* servers).find((peer) => peer.id === id)
    if (!instance) return yield* new Unavailable({ message: `Peer not found: ${id}` })
    // iroh's TLS handshake proves the remote holds the dialed endpoint ID's key.
    const connection = yield* Effect.tryPromise({
      try: () =>
        endpoint.connect({
          id,
          relay: instance.address?.relay ?? null,
          addresses: [...(instance.address?.addresses ?? [])],
        }),
      catch: (cause) => new Unavailable({ message: `Could not reach peer ${instance.name}`, cause }),
    })
    const token = yield* Effect.gen(function* () {
      const control = yield* Effect.tryPromise(() => connection.openBi())
      yield* Effect.tryPromise(() => control.send.writeAll(HELLO))
      yield* Effect.tryPromise(() => control.send.finish())
      const reply = yield* Effect.tryPromise(() => control.recv.readToEnd(4096))
      if (reply.length === 0) return yield* Effect.fail("empty token")
      return new TextDecoder().decode(reply)
    }).pipe(
      Effect.mapError((cause) => new Unavailable({ message: `Peer ${instance.name} refused the connection`, cause })),
      Effect.onError(() => Effect.sync(() => connection.close())),
    )
    return { connection, token }
  })

  // The first token stays valid across re-dials (it lasts until the peer's password changes or it expires). Callers
  // pass the failure they saw before queueing; one that queued behind a failed re-dial gets its error instead of
  // repeating it.
  const refresh = Effect.fn("Peers.refresh")(function* (id: string, gateway: Dialed, seen: Unavailable | undefined) {
    if (gateway.connection.closeReason() === null) return gateway.connection
    if (gateway.failure && gateway.failure !== seen) return yield* gateway.failure
    // The peer may have left or restarted on a new address since the list was last refreshed.
    yield* registry.refresh()
    const dialed = yield* dial(id).pipe(Effect.tapError((error) => Effect.sync(() => (gateway.failure = error))))
    gateway.connection = dialed.connection
    return gateway.connection
  })

  const open = Effect.fn("Peers.open")(function* (id: string) {
    const dialed = yield* dial(id)
    const server = yield* NodeSocketServer.make({ host: "127.0.0.1", port: 0 }).pipe(
      Scope.provide(layerScope),
      Effect.mapError((cause) => new Unavailable({ message: "Failed to open a local peer listener", cause })),
      Effect.onError(() => Effect.sync(() => dialed.connection.close())),
    )
    const port = NetAddress.isInetAddress(server.address) ? server.address.port : 0
    const gateway: Dialed = { url: `http://127.0.0.1:${port}`, token: dialed.token, connection: dialed.connection }
    yield* server
      .run((socket) =>
        locked(id, refresh(id, gateway, gateway.failure)).pipe(
          Effect.flatMap((connection) => Effect.tryPromise(() => connection.openBi())),
          Effect.flatMap((stream) => pipe(socket, stream)),
          // The socket server only closes sockets the handler read or wrote, so close it when the peer is unreachable.
          Effect.tapCause(() => Effect.serviceOption(NetSocket).pipe(Effect.map(Option.map((conn) => conn.destroy())))),
          Effect.ignore,
        ),
      )
      .pipe(Effect.ignore, Effect.forkIn(layerScope))
    return gateway
  })

  return Service.of({
    list: () =>
      registry.refresh().pipe(
        Effect.andThen(servers),
        Effect.map((list) =>
          list.map((peer) => ({ ...peer, connected: gateways.get(peer.id)?.connection.closeReason() === null })),
        ),
      ),
    connect: Effect.fn("Peers.connect")(
      function* (id) {
        const existing = gateways.get(id)
        if (existing) {
          yield* refresh(id, existing, existing.failure)
          return { url: existing.url, token: existing.token }
        }
        const gateway = yield* open(id)
        gateways.set(id, gateway)
        return { url: gateway.url, token: gateway.token }
      },
      (effect, id) => locked(id, effect),
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
        for (const chunk of yield* pull) yield* Effect.tryPromise(() => stream.send.writeAll(chunk))
      }
    }).pipe(Effect.catchIf(isEnd, () => Effect.tryPromise(() => stream.send.finish())))
    // Releasing the writer's scope half-closes the TCP socket once the peer finishes.
    const download = Effect.gen(function* () {
      const writer = yield* socket.writer
      while (true) {
        const bytes = yield* Effect.tryPromise(() => stream.recv.read(CHUNK))
        if (bytes.length === 0) return
        yield* writer.write(bytes)
      }
    }).pipe(Effect.scoped)
    yield* Effect.all([upload, download], { concurrency: 2, discard: true })
  },
  Effect.scoped,
  (effect, _socket, stream) =>
    effect.pipe(
      Effect.onError(() =>
        Effect.promise(async () => {
          await stream.send.reset().catch(() => {})
          await stream.recv.stop().catch(() => {})
        }),
      ),
    ),
)

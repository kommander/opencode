export * as Peer from "./peer.js"

import type { PeerEntry, PeerRelay } from "@opencode/plugin/effect/global"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Context, Deferred, Effect, Layer, Scope } from "effect"

type Self = Omit<PeerEntry, "updated">

/** Process-wide peer list. The server's peer endpoint provides this server's entry; server-wide plugins add peers. */
export interface Interface {
  readonly self: () => Effect.Effect<Self | undefined>
  readonly setSelf: (self: Effect.Effect<Self>) => Effect.Effect<void>
  readonly add: (owner: string, peer: PeerEntry) => Effect.Effect<void>
  readonly remove: (owner: string, id: string) => Effect.Effect<void>
  readonly clear: (owner: string) => Effect.Effect<void>
  readonly list: () => Effect.Effect<PeerEntry[]>
  readonly onRefresh: (sync: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>
  /** Provides the relays peer connections use; the first plugin to provide them wins. */
  readonly setRelay: (relay: PeerRelay) => Effect.Effect<void>
  /** Waits until a plugin provides relays. */
  readonly relay: Effect.Effect<PeerRelay>
  /** Runs every registered plugin refresh, giving up on each after a few seconds. */
  readonly refresh: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Peer") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const relay = yield* Deferred.make<PeerRelay>()
    const peers = new Map<string, { readonly owner: string; readonly peer: PeerEntry }>()
    let self: Effect.Effect<Self> | undefined
    const syncs = new Set<Effect.Effect<void>>()
    const remove = (owner: string, id: string) => {
      if (peers.get(id)?.owner === owner) peers.delete(id)
    }
    return Service.of({
      self: () => self ?? Effect.succeed(undefined),
      setSelf: (value) => Effect.sync(() => void (self = value)),
      add: (owner, peer) => Effect.sync(() => void peers.set(peer.id, { owner, peer })),
      remove: (owner, id) => Effect.sync(() => remove(owner, id)),
      clear: (owner) => Effect.sync(() => [...peers.keys()].forEach((id) => remove(owner, id))),
      list: () => Effect.sync(() => [...peers.values()].map((item) => item.peer)),
      onRefresh: (sync) =>
        Effect.acquireRelease(
          Effect.sync(() => void syncs.add(sync)),
          () => Effect.sync(() => void syncs.delete(sync)),
        ),
      setRelay: (value) => Deferred.succeed(relay, value).pipe(Effect.asVoid),
      relay: Deferred.await(relay),
      refresh: () =>
        Effect.forEach([...syncs], (sync) => sync.pipe(Effect.timeout("5 seconds"), Effect.ignore), {
          concurrency: "unbounded",
          discard: true,
        }),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

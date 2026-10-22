export * as Peer from "./peer.js"

import type { PeerEntry, PeerSelf } from "@opencode/plugin/effect/global"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer, Scope } from "effect"

/**
 * Process-wide peer registry. The server's peer endpoint provides this server's identity; server-wide plugins add
 * the peers it may connect to and accept connections from, each entry owned by the plugin that added it.
 */
export interface Interface {
  readonly self: () => Effect.Effect<PeerSelf | undefined>
  /** Provides this server's identity for as long as the calling scope is open. */
  readonly provideSelf: (self: Effect.Effect<PeerSelf>) => Effect.Effect<void, never, Scope.Scope>
  readonly add: (owner: string, peer: PeerEntry) => Effect.Effect<void>
  readonly remove: (owner: string, id: string) => Effect.Effect<void>
  /** Removes every entry the owner added. */
  readonly clear: (owner: string) => Effect.Effect<void>
  /** Every peer, with the most recently announced entry winning when several owners add the same ID. */
  readonly list: () => Effect.Effect<PeerEntry[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Peer") {}

const layer = Layer.sync(Service, () => {
  const entries = new Map<string, Map<string, PeerEntry>>()
  let self: Effect.Effect<PeerSelf> | undefined
  return Service.of({
    self: () => self ?? Effect.succeed(undefined),
    provideSelf: (value) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          self = value
        }),
        () =>
          Effect.sync(() => {
            if (self === value) self = undefined
          }),
      ),
    add: (owner, peer) =>
      Effect.sync(() => {
        const owned = entries.get(owner) ?? new Map<string, PeerEntry>()
        owned.set(peer.id, peer)
        entries.set(owner, owned)
      }),
    remove: (owner, id) => Effect.sync(() => void entries.get(owner)?.delete(id)),
    clear: (owner) => Effect.sync(() => void entries.delete(owner)),
    list: () =>
      Effect.sync(() => {
        const merged = new Map<string, PeerEntry>()
        entries.forEach((owned) =>
          owned.forEach((peer) => {
            const existing = merged.get(peer.id)
            if (!existing || existing.updated < peer.updated) merged.set(peer.id, peer)
          }),
        )
        return [...merged.values()]
      }),
  })
})

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

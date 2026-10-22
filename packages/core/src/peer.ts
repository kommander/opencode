export * as Peer from "./peer.js"

import type { PeerEntry } from "@opencode/plugin/effect/global"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer } from "effect"

type Self = Omit<PeerEntry, "updated">

/** Process-wide peer list. The server's peer endpoint provides this server's entry; server-wide plugins add peers. */
export interface Interface {
  readonly self: () => Effect.Effect<Self | undefined>
  readonly setSelf: (self: Effect.Effect<Self>) => Effect.Effect<void>
  readonly add: (owner: string, peer: PeerEntry) => Effect.Effect<void>
  readonly remove: (owner: string, id: string) => Effect.Effect<void>
  readonly clear: (owner: string) => Effect.Effect<void>
  readonly list: () => Effect.Effect<PeerEntry[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Peer") {}

const layer = Layer.sync(Service, () => {
  const peers = new Map<string, { readonly owner: string; readonly peer: PeerEntry }>()
  let self: Effect.Effect<Self> | undefined
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
  })
})

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

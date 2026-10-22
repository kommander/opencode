import type { Credential } from "@opencode/schema/credential"
import type { Effect, Scope } from "effect"
import type { App } from "../app.js"
import type { PluginOptions } from "../options.js"

/** Context of a plugin's server-wide phase, which runs once per server process instead of once per location. */
export interface GlobalContext {
  readonly app: App
  readonly options: PluginOptions
  readonly credential: GlobalCredentialDomain
  readonly peer: PeerDomain
}

export interface GlobalCredentialDomain {
  /** Returns the integration's selected stored credential. */
  readonly active: (integrationID: string) => Effect.Effect<StoredCredential | undefined>
  /** Replaces a stored credential's value, for example after refreshing an OAuth token. */
  readonly update: (id: string, value: Credential.Value) => Effect.Effect<void>
}

export interface StoredCredential {
  readonly id: string
  readonly value: Credential.Value
}

/** Address another server dials this one at over iroh. */
export interface PeerAddress {
  readonly relay: string | null
  readonly addresses: ReadonlyArray<string>
}

/** This server, as other servers should see it. */
export interface PeerSelf {
  readonly id: string
  readonly name: string
  readonly version?: string
  readonly address: PeerAddress
}

export interface PeerEntry extends PeerSelf {
  /** When the peer last announced itself, in epoch milliseconds. */
  readonly updated: number
}

/**
 * Peers this server may connect to and accept connections from. Entries belong to the plugin that added them and
 * are removed when its scope closes.
 */
export interface PeerDomain {
  /** This server's peer identity and address, once its peer endpoint is running. */
  readonly self: () => Effect.Effect<PeerSelf | undefined>
  /** Adds a peer, or replaces this plugin's entry with the same ID. */
  readonly add: (peer: PeerEntry) => Effect.Effect<void>
  readonly remove: (id: string) => Effect.Effect<void>
}

export interface GlobalPlugin<R = Scope.Scope> {
  readonly id: string
  readonly effect: (context: GlobalContext) => Effect.Effect<void, never, R>
}

export function define<R = Scope.Scope>(plugin: GlobalPlugin<R>) {
  return plugin
}

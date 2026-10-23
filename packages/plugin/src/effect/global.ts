import type { Credential } from "@opencode/schema/credential"
import type { Effect, Scope } from "effect"

/** A server this server may connect to and accept connections from, or a client it accepts connections from. */
export interface PeerEntry {
  readonly id: string
  readonly name: string
  readonly version?: string
  readonly kind: "server" | "client"
  /** Where to dial a server; clients have none. */
  readonly address?: { readonly relay: string | null; readonly addresses: ReadonlyArray<string> }
  /** When the peer last announced itself, in epoch milliseconds. */
  readonly updated: number
}

/** The relays a plugin provides for peer connections, and how to get the bearer token they require. */
export interface PeerRelay {
  readonly urls: ReadonlyArray<string>
  /** Fetches a fresh token; the server renews it before `expires` (epoch milliseconds). */
  readonly token: Effect.Effect<{ readonly token: string; readonly expires: number }, unknown>
}

/** Context of a built-in plugin's server-wide phase, which runs once per server process. */
export interface GlobalContext {
  readonly credential: {
    /** The integration's selected stored credential. */
    readonly active: (
      integrationID: string,
    ) => Effect.Effect<{ readonly id: string; readonly value: Credential.Value } | undefined>
    readonly update: (id: string, value: Credential.Value) => Effect.Effect<void>
  }
  /** Peer list entries belong to the plugin that added them and are removed when its scope closes. */
  readonly peer: {
    /** This server's own entry, once its peer endpoint runs. */
    readonly self: () => Effect.Effect<Omit<PeerEntry, "updated"> | undefined>
    readonly add: (peer: PeerEntry) => Effect.Effect<void>
    readonly remove: (id: string) => Effect.Effect<void>
    /** Turns on peer connections through these relays. Without a relay, the server starts no peer endpoint. */
    readonly relay: (relay: PeerRelay) => Effect.Effect<void>
    /** Registers how this plugin refreshes its peers when the server needs a current list. */
    readonly onRefresh: (sync: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>
  }
}

export interface GlobalPlugin<R = Scope.Scope> {
  readonly id: string
  readonly effect: (context: GlobalContext) => Effect.Effect<void, never, R>
}

export function define<R = Scope.Scope>(plugin: GlobalPlugin<R>) {
  return plugin
}

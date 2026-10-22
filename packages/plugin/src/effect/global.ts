import type { Credential } from "@opencode/schema/credential"
import type { Effect, Scope } from "effect"

/** A server this server may connect to, and accept connections from, over iroh. */
export interface PeerEntry {
  readonly id: string
  readonly name: string
  readonly version?: string
  readonly address: { readonly relay: string | null; readonly addresses: ReadonlyArray<string> }
  /** When the peer last announced itself, in epoch milliseconds. */
  readonly updated: number
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
  }
}

export interface GlobalPlugin<R = Scope.Scope> {
  readonly id: string
  readonly effect: (context: GlobalContext) => Effect.Effect<void, never, R>
}

export function define<R = Scope.Scope>(plugin: GlobalPlugin<R>) {
  return plugin
}

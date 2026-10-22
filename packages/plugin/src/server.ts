export * as ServerPlugin from "./server.js"

import type { PluginOptions } from "./options.js"
import type { Peers } from "./peer.js"

export type { PeerInfo, Peers } from "./peer.js"

/**
 * Experimental server plugins. A server plugin is trusted code that runs once per server process for the process
 * lifetime, independent of any Location. Only `server.plugins` in the global config loads server plugins; project
 * config cannot.
 */
export interface Context {
  readonly options: PluginOptions
  /** Reaches this server over loopback; `headers` carry its credentials. */
  readonly server: { readonly url: string; readonly headers: Record<string, string> }
  readonly peers: {
    /** Lists the peers at `/api/peers` and proxies `/api/peers/:id/*` to them. */
    readonly provide: (peers: Peers) => void
  }
}

export type Cleanup = () => Promise<void> | void

export interface Plugin {
  readonly id: string
  readonly setup: (context: Context) => Promise<Cleanup | void> | Cleanup | void
}

export function define(plugin: Plugin) {
  return plugin
}

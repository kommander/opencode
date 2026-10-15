/**
 * Experimental peer providers. A provider is trusted server code: `opencode serve` imports each module listed under
 * `peers` in the global config and calls its default export once. The server lists the peers at `/api/peers` and
 * proxies `/api/peers/:id/*` to the URL returned by `connect`. Inbound peer traffic is the provider's job: it reaches
 * its own server through `server.url` with `server.headers`.
 */
export type PeerProvider = (input: {
  options: Record<string, unknown>
  server: { url: string; headers: Record<string, string> }
}) => Promise<Peers>

export interface Peers {
  list(): PeerInfo[]
  /** Returns a base URL, normally on loopback, that reaches the peer's OpenCode server. */
  connect(peerID: string): Promise<string>
  close(): Promise<void>
}

export interface PeerInfo {
  id: string
  name: string
  status: "online" | "offline"
}

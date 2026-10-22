/**
 * Experimental peers, provided by server plugins through `context.peers.provide`. The server lists the peers at
 * `/api/peers` and proxies `/api/peers/:id/*` to the URL returned by `connect`. Inbound peer traffic is the plugin's
 * job: it reaches its own server through `context.server.url` with `context.server.headers`.
 */
export interface Peers {
  list(): PeerInfo[]
  /** Returns a base URL, normally on loopback, that reaches the peer's OpenCode server. */
  connect(peerID: string): Promise<string>
}

export interface PeerInfo {
  id: string
  name: string
  status: "online" | "offline"
}

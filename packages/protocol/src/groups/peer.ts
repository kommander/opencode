import { Schema } from "effect"
import { HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/http-api"
import { ServiceUnavailableError } from "../errors.js"

export const PeerInfo = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  /** When the peer last announced itself, in epoch milliseconds. */
  updated: Schema.Number,
  /** Whether this server holds a live connection to the peer. */
  connected: Schema.Boolean,
}).annotate({ identifier: "PeerInfo" })
export type PeerInfo = typeof PeerInfo.Type

export const PeerConnection = Schema.Struct({
  url: Schema.String,
  token: Schema.String,
}).annotate({ identifier: "PeerConnection" })
export type PeerConnection = typeof PeerConnection.Type

export const PeerGroup = HttpApiGroup.make("server.peer")
  .add(
    HttpApiEndpoint.get("peer.list", "/api/peers", {
      success: Schema.Struct({ data: Schema.Array(PeerInfo) }),
      error: ServiceUnavailableError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "peer.list",
        summary: "List peers",
        description: "List the other OpenCode servers this server can connect to.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("peer.connect", "/api/peers/:id/connect", {
      params: { id: Schema.String },
      success: PeerConnection,
      error: ServiceUnavailableError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "peer.connect",
        summary: "Connect to peer",
        description:
          "Open a peer-to-peer connection to another server and return a local loopback URL that reaches it, with a token the peer accepts as the Basic password for user `opencode`.",
      }),
    ),
  )
  .annotateMerge(OpenApi.annotations({ title: "peer" }))

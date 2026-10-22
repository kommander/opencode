import { Schema } from "effect"
import { HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/http-api"
import { PeerNotFoundError, ServiceUnavailableError } from "../errors.js"

export const PeerInfo = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  online: Schema.Boolean,
  /** When the peer last announced itself, in epoch milliseconds. */
  updated: Schema.Number,
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
        description: "List the signed-in account's other OpenCode servers known to the peer directory.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("peer.connect", "/api/peers/:id/connect", {
      params: { id: Schema.String },
      success: PeerConnection,
      error: [PeerNotFoundError, ServiceUnavailableError],
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

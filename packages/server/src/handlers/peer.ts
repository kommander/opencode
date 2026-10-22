import { ServiceUnavailableError } from "@opencode/protocol/errors"
import { Effect } from "effect"
import { HttpApiBuilder } from "effect/http-api"
import { Api } from "../api"
import { Peers } from "../peers"

export const PeerHandler = HttpApiBuilder.group(Api, "server.peer", (handlers) =>
  Effect.gen(function* () {
    const peers = yield* Peers.Service

    return handlers
      .handle("peer.list", () =>
        peers.list().pipe(
          Effect.map((data) => ({ data })),
          Effect.mapError((error) => new ServiceUnavailableError({ message: error.message, service: "peers" })),
        ),
      )
      .handle("peer.connect", (ctx) =>
        peers
          .connect(ctx.params.id)
          .pipe(Effect.mapError((error) => new ServiceUnavailableError({ message: error.message, service: "peers" }))),
      )
  }),
)

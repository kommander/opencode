export * as PluginGlobal from "./global.js"

import type { GlobalContext, GlobalPlugin } from "@opencode/plugin/effect/global"
import { Integration } from "@opencode/schema/integration"
import { Effect, Layer, Scope } from "effect"
import { FetchHttpClient, HttpClient } from "effect/http"
import { App } from "../app.js"
import { Credential } from "../credential.js"
import { Peer } from "../peer.js"
import { OpencodePeersPlugin } from "./provider/opencode-peers.js"

// Built-in plugins with a server-wide phase. Package plugins cannot declare one yet.
const builtins: ReadonlyArray<GlobalPlugin<HttpClient.HttpClient | Scope.Scope>> = [OpencodePeersPlugin.Plugin]

/** Runs the built-in server-wide plugin phases, once per server process, until the layer's scope closes. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const app = yield* App.Metadata
    const credentials = yield* Credential.Service
    const peers = yield* Peer.Service
    yield* Effect.forEach(builtins, (plugin) => {
      const context: GlobalContext = {
        app,
        options: {},
        credential: {
          // Listings order the integration's selected credential last.
          active: (integrationID) =>
            credentials.list(Integration.ID.make(integrationID)).pipe(
              Effect.map((list) => list.at(-1)),
              Effect.map((item) => item && { id: item.id, value: item.value }),
            ),
          update: (id, value) => credentials.update(Credential.ID.make(id), { value }),
        },
        peer: {
          self: peers.self,
          add: (peer) => peers.add(plugin.id, peer),
          remove: (id) => peers.remove(plugin.id, id),
        },
      }
      return Effect.gen(function* () {
        yield* Effect.addFinalizer(() => peers.clear(plugin.id))
        yield* plugin.effect(context)
        yield* Effect.never
      }).pipe(
        Effect.scoped,
        Effect.provide(FetchHttpClient.layer),
        Effect.catchCause((cause) => Effect.logError("server-wide plugin failed", { plugin: plugin.id, cause })),
        Effect.forkScoped,
      )
    })
  }),
)

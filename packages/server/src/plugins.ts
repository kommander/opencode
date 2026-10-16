export * as ServerPlugins from "./plugins"

import { ConfigDiscovery } from "@opencode/core/config/discovery"
import type { ServerPlugin } from "@opencode/plugin/server"
import { ConfigPlugin } from "@opencode/schema/config/plugin"
import { Global } from "@opencode/util/global"
import { Effect, FileSystem, Schema } from "effect"
import { parse } from "jsonc-parser"
import path from "path"

const GlobalConfig = Schema.Struct({
  server: Schema.optional(Schema.Struct({ plugins: Schema.optional(ConfigPlugin.Plugins) })),
})

const Module = Schema.Struct({
  default: Schema.Struct({
    id: Schema.String,
    setup: Schema.declare<ServerPlugin.Plugin["setup"]>(
      (input): input is ServerPlugin.Plugin["setup"] => typeof input === "function",
    ),
  }),
})

/**
 * Starts the server plugins listed under `server.plugins` in the global config for the lifetime of the current scope.
 * Only the global config file is read, so project config cannot load server code.
 */
export const start = Effect.fn("ServerPlugins.start")(function* (server: ServerPlugin.Context["server"]) {
  const global = yield* Global.Service
  const fs = yield* FileSystem.FileSystem
  const configs = yield* Effect.forEach(ConfigDiscovery.names, (name) =>
    Effect.gen(function* () {
      const file = path.join(global.config, name)
      if (!(yield* fs.exists(file))) return {}
      return yield* Schema.decodeUnknownEffect(GlobalConfig)(parse(yield* fs.readFileString(file)))
    }),
  )
  const peers: ServerPlugin.Peers[] = []
  yield* Effect.forEach(configs.findLast((config) => config.server?.plugins)?.server?.plugins ?? [], (entry) =>
    Effect.gen(function* () {
      const target = typeof entry === "string" ? entry : entry.package
      const plugin = (yield* Schema.decodeUnknownEffect(Module)(yield* Effect.tryPromise(() => import(target))))
        .default
      yield* Effect.log({ msg: "starting server plugin", id: plugin.id, target })
      yield* Effect.acquireRelease(
        Effect.tryPromise(async () =>
          plugin.setup({
            options: typeof entry === "string" ? {} : (entry.options ?? {}),
            server,
            peers: { provide: (provided) => void peers.push(provided) },
          }),
        ),
        (cleanup) => Effect.promise(async () => cleanup?.()),
      )
    }),
  )
  return { peers }
})

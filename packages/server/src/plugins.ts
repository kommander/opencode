export * as ServerPlugins from "./plugins"

import { ConfigDiscovery } from "@opencode/core/config/discovery"
import type { ServerPlugin } from "@opencode/plugin/server"
import { ConfigPlugin } from "@opencode/schema/config/plugin"
import { Global } from "@opencode/util/global"
import { Effect, FileSystem, Schema } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/http"
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
  const routes = new Map<string, (request: Request) => Promise<Response>>()
  yield* Effect.forEach(configs.findLast((config) => config.server?.plugins)?.server?.plugins ?? [], (entry) =>
    Effect.gen(function* () {
      const target = typeof entry === "string" ? entry : entry.package
      const plugin = (yield* Schema.decodeUnknownEffect(Module)(yield* Effect.tryPromise(() => import(target)))).default
      yield* Effect.log({ msg: "starting server plugin", id: plugin.id, target })
      yield* Effect.acquireRelease(
        Effect.tryPromise(async () =>
          plugin.setup({
            options: typeof entry === "string" ? {} : (entry.options ?? {}),
            server,
            peers: { provide: (provided) => void peers.push(provided) },
            routes: { serve: (fetch) => void routes.set(plugin.id, fetch) },
          }),
        ),
        (cleanup) => Effect.promise(async () => cleanup?.()),
      )
    }),
  )
  return { peers, routes }
})

const PLUGIN_PATH = /^\/api\/plugins\/([^/]+)(\/.*)?$/

export function matches(pathname: string) {
  return PLUGIN_PATH.test(pathname)
}

/** Serves `/api/plugins/:id/*` from the routes server plugins registered. */
export const serve = (routes: ReadonlyMap<string, (request: Request) => Promise<Response>>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const [, id, rest] = PLUGIN_PATH.exec(new URL(request.url, "http://localhost").pathname) ?? []
    const fetch = routes.get(decodeURIComponent(id))
    if (!fetch) return HttpServerResponse.empty({ status: 404 })
    const incoming = yield* HttpServerRequest.toWeb(request)
    const target = new URL(incoming.url)
    target.pathname = rest ?? "/"
    return HttpServerResponse.fromWeb(yield* Effect.tryPromise(() => fetch(new Request(target, incoming))))
  }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 500 })))

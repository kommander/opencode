import { describe, expect } from "bun:test"
import { Duration, Effect, Layer, LayerMap, Queue, Stream } from "effect"
import { App } from "@opencode/core/app"
import { ConfigPluginSource } from "@opencode/core/config/plugin/source"
import { Form } from "@opencode/core/form"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Instance } from "@opencode/core/instance"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-services"
import { Plugin } from "@opencode/core/plugin"
import { Rpc } from "@opencode/core/rpc"
import { Session } from "@opencode/core/session"
import { SessionAttention } from "@opencode/schema/session-attention"
import { SdkPlugins } from "@opencode/core/plugin/sdk"
import { AbsolutePath } from "@opencode/core/schema"
import { Tool } from "@opencode/core/tool"
import { TodoTool } from "@opencode/core/tool/plugin/todo"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { Npm } from "@opencode/util/npm"
import { Bus } from "../../src/bus"
import { Database } from "../../src/database/database"
import { tempGlobalLayer } from "../fixture/global"
import { offlineModels } from "../fixture/models"
import { tmpdirScoped } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"
import { toolDefinitions } from "../lib/tool"

const unexpected = (pkg: string) =>
  Effect.fail(new Npm.InstallFailedError({ add: [pkg], dir: "", cause: new Error(`Unexpected Npm call: ${pkg}`) }))

const instances = Layer.effect(
  LocationServiceMap.Service,
  Effect.gen(function* () {
    const map = yield* LayerMap.make(
      (ref: Location.Ref) => Instance.layer(ref, { discovery: false, replacements: bindings }),
      { idleTimeToLive: Duration.infinity },
    )
    const bindings: LayerNode.Replacements = [
      App.node.replace(App.configured({ channel: "dev" })),
      Global.node.replace(tempGlobalLayer),
      offlineModels,
      ConfigPluginSource.node.replace(
        Layer.succeed(
          ConfigPluginSource.Service,
          ConfigPluginSource.Service.of({ operations: () => Effect.succeed([]), changes: () => Stream.never }),
        ),
      ),
      Npm.node.replace(
        Layer.succeed(
          Npm.Service,
          Npm.Service.of({
            add: unexpected,
            resolve: (name) => Effect.succeed({ directory: "", name }),
            check: () => Effect.succeed(false),
            update: unexpected,
            which: () => Effect.undefined,
          }),
        ),
      ),
      LocationServiceMap.node.replace(Layer.succeed(LocationServiceMap.Service, map)),
      Instance.node.replace(
        Layer.succeed(Instance.Service, { provide: (session) => Effect.provide(map.get(session.location)) }),
      ),
    ]
    return map
  }),
)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SdkPlugins.node, LocationServiceMap.node, Session.node]),
    [Global.node.replace(tempGlobalLayer), offlineModels, LocationServiceMap.node.replace(instances)],
  ),
)

describe("PluginInternal flags", () => {
  it.live("publishes an attention invalidation when admitted user input reopens completed work", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const location = Location.Ref.make({ directory: AbsolutePath.make(directory.path) })
      const locations = yield* LocationServiceMap.Service
      yield* Effect.gen(function* () {
        const plugins = yield* Plugin.Service
        yield* plugins.awaitActivation
        const rpc = yield* Rpc.Service
        const client = rpc.client(SessionAttention.Definition)
        const sessions = yield* Session.Service
        const { id: sessionID } = yield* sessions.create({ location })
        expect(yield* client.complete({ sessionID })).toBe("completed")
        const events = yield* client.events.subscribe("changed").pipe(Stream.toQueue({ capacity: 8 }))
        yield* sessions.prompt({ sessionID, text: "One more thing", resume: false })
        expect((yield* Queue.take(events)).data.sessionID).toBe(sessionID)
        expect(yield* client.get({ sessionID })).toBe("cleared")
        const forms = yield* Form.Service
        const form = yield* forms.create({ sessionID, title: "Choose", fields: [{ key: "answer", type: "string" }] })
        yield* Queue.take(events)
        expect(yield* client.get({ sessionID })).toBe("needs_form")
        yield* forms.cancel(form.id)
        yield* Queue.take(events)
        expect(yield* client.get({ sessionID })).toBe("cleared")
      }).pipe(Effect.scoped, Effect.provide(locations.get(location)))
    }),
  )
  it.live("turns a flagged built-in tool on for a dev-channel server with no config", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const locations = yield* LocationServiceMap.Service
      const names = yield* Effect.gen(function* () {
        yield* (yield* Plugin.Service).awaitActivation
        return (yield* toolDefinitions(yield* Tool.Service)).map((definition) => definition.name)
      }).pipe(
        Effect.scoped,
        Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(directory.path) }))),
      )

      expect(names).toContain(TodoTool.name)
    }),
  )
})

import { expect } from "bun:test"
import { Bus } from "@opencode/core/bus"
import { Flag } from "@opencode/core/flag"
import { Location } from "@opencode/core/location"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { Tool } from "@opencode/core/tool"
import { SessionSearchTools } from "@opencode/core/tool/plugin/session-search"
import { Effect } from "effect"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { executeTool, toolIdentity } from "./lib/tool"
import { PluginTestLayer } from "./plugin/fixture"

const it = testEffect(PluginTestLayer)

const setup = Effect.gen(function* () {
  const plugins = yield* Plugin.Service
  const host = yield* PluginHost.make(plugins, "test")
  yield* SessionSearchTools.Plugin.effect(host).pipe(
    Effect.provide(Flag.testLayer([], { overrides: "session_search" })),
  )
  const registry = yield* Tool.Service
  const sessions = yield* Session.Service
  const location = yield* Location.Service
  const local = Location.Ref.make({ directory: location.directory })
  const caller = yield* sessions.create({ location: local })
  const call = (name: string, input: Record<string, unknown>) =>
    executeTool(registry, {
      ...toolIdentity,
      sessionID: caller.id,
      call: {
        type: "tool-call",
        id: `call-${name}`,
        name: "execute",
        input: { code: `return await tools.sessions.${name}(${JSON.stringify(input)})` },
      },
    })
  const run = (name: string, input: Record<string, unknown>) =>
    call(name, input).pipe(
      Effect.map((result) => JSON.parse(result.content?.[0]?.type === "text" ? result.content[0].text : "")),
    )
  return { sessions, local, caller, call, run }
})

const message = (sessionID: Session.ID, text: string) =>
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const inboxID = SessionMessage.ID.create()
    yield* bus.publish(SessionEvent.InboxEnqueued, {
      sessionID,
      inboxID,
      item: { type: "user", payload: { text }, delivery: "steer" },
    })
    yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID })
    return inboxID
  })

it.live("opens a literal content hit and pages through the surrounding messages", () =>
  Effect.gen(function* () {
    const fixture = yield* setup
    const target = yield* fixture.sessions.create({ location: fixture.local, title: "Release notes" })
    const query = "release.100%_ready"
    const before = "release.10000ready"
    const match = `Status: ${query} for review`
    const after = "releaseX100%_ready"
    yield* message(target.id, before)
    const hit = yield* message(target.id, match)
    yield* message(target.id, after)

    const hits: { messageID: string; cursor?: string }[] = []
    let cursor: string | undefined
    do {
      const page = yield* fixture.run(
        "search",
        cursor ? { cursor, limit: 1 } : { sessionID: target.id, query, limit: 1 },
      )
      for (const group of page.sessions) {
        expect(group).toMatchObject({ sessionID: target.id, title: target.title })
        hits.push(...group.hits)
      }
      cursor = page.nextCursor
    } while (cursor)
    expect(hits).toMatchObject([{ messageID: hit }])
    const inspect = (cursor?: string) => fixture.run("inspect", { sessionID: target.id, cursor, limit: 1 })
    const page = yield* inspect(hits[0]?.cursor)
    expect(page).toMatchObject({ session: { sessionID: target.id }, messages: [{ id: hit, text: match }] })
    expect(page.previousCursor).toEqual(expect.any(String))
    expect((yield* inspect(page.previousCursor)).messages).toMatchObject([{ text: before }])
    expect((yield* inspect(page.nextCursor)).messages).toMatchObject([{ text: after }])
  }),
)

it.live("only lists sessions at the caller's location and does not reveal a foreign session by ID", () =>
  Effect.gen(function* () {
    const fixture = yield* setup
    const peer = yield* fixture.sessions.create({ location: fixture.local, title: "Same directory" })
    const other = yield* tmpdirScoped()
    const foreign = yield* fixture.sessions.create({
      location: Location.Ref.make({ directory: AbsolutePath.make(other.path) }),
      title: "Private notes",
    })
    const ids: string[] = []
    let cursor: string | undefined
    do {
      const page = yield* fixture.run("search", { limit: 1, cursor })
      expect(page.sessions.length).toBeLessThanOrEqual(1)
      for (const session of page.sessions) ids.push(session.sessionID)
      cursor = page.nextCursor
    } while (cursor)
    expect(ids.sort()).toEqual([fixture.caller.id, peer.id].sort())

    const input = { sessionID: foreign.id }
    for (const name of ["search", "inspect"]) {
      const result = yield* fixture.call(name, input)
      expect(result.metadata?.error).toBe(true)
      expect(JSON.stringify(result)).not.toContain("Private notes")
    }
  }),
)

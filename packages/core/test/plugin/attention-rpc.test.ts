import { expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Bus } from "@opencode/core/bus"
import { Form } from "@opencode/core/form"
import { Location } from "@opencode/core/location"
import { Permission } from "@opencode/core/permission"
import { register } from "@opencode/core/plugin/attention-rpc"
import { Project } from "@opencode/core/project"
import { Rpc } from "@opencode/core/rpc"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionAttention } from "@opencode/schema/session-attention"
import { Workspace } from "@opencode/schema/workspace"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { location } from "../fixture/location"
import { testEffect } from "../lib/effect"
import { globalProjectNode } from "../lib/project"

const here = location({ directory: AbsolutePath.make("/project") })
const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Session.node, Form.node, Permission.node, Rpc.node]), [
    Location.node.replace(Layer.succeed(Location.Service, here)),
    Project.node.replace(globalProjectNode),
    Bus.node.replace(Bus.configured({ persist: true })),
    SessionExecution.node.replace(SessionExecution.noopLayer),
  ]),
)

const setup = Effect.gen(function* () {
  const rpc = yield* Rpc.Service
  const states = new Map<Session.ID, SessionAttention.Status>()
  yield* register(
    { rpc, location: here },
    (id) => Effect.succeed(states.get(id) ?? "unclassified"),
    (id) =>
      Effect.sync(() => {
        states.set(id, "completed")
      }),
  )
  return { client: rpc.client(SessionAttention.Definition), states }
})

it.effect("reads a page of all local sessions without treating archiving as completion", () =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    const { client, states } = yield* setup
    const ordinary = yield* session.create({ location: here })
    const archived = yield* session.create({ location: here })
    yield* session.archive(archived.id)
    states.set(ordinary.id, "cleared")
    const workspaceID = Workspace.ID.create()
    const other = yield* session.create({ location: { directory: AbsolutePath.make("/other") } })
    const workspace = yield* session.create({ location: { directory: here.directory, workspaceID } })
    for (const id of [Session.ID.create(), other.id, workspace.id]) {
      expect(yield* client.get({ sessionID: id })).toBeNull()
      expect(yield* client.complete({ sessionID: id })).toBeNull()
      expect(states.has(id)).toBe(false)
    }
    for (const filter of [
      { directory: other.location.directory, workspaceID: null },
      { directory: here.directory, workspaceID },
      {},
    ]) {
      const cursor = Session.ListCursor.make({
        ...filter,
        anchor: { direction: "next", id: ordinary.id, time: Number.MAX_SAFE_INTEGER },
      })
      expect(yield* client.list({ cursor })).toEqual({ data: [] })
    }
    const first = yield* client.list({ limit: 1 })
    expect(first.data).toHaveLength(1)
    const second = yield* client.list({ limit: 1, cursor: first.next })
    expect(second.data).toHaveLength(1)
    expect([...first.data, ...second.data]).toEqual(
      expect.arrayContaining([
        { sessionID: ordinary.id, state: "cleared" },
        { sessionID: archived.id, state: "unclassified" },
      ]),
    )
    expect(yield* client.list({ limit: 1, cursor: second.next })).toEqual({ data: [] })
    const empty = yield* client.list({ limit: 1, needsUser: true })
    expect(empty).toEqual({ data: [], next: first.next })
    const [row] = second.data
    states.set(row.sessionID, "needs_reply")
    expect((yield* client.list({ limit: 1, cursor: empty.next, needsUser: true })).data).toEqual([
      { sessionID: row.sessionID, state: "needs_reply" },
    ])
    for (const limit of [0, 1.5, 101])
      expect(yield* client.list({ limit }).pipe(Effect.flip)).toMatchObject({ type: "rpc.invalid_input" })
  }),
)

it.effect("live permission and form requests overlay persisted state, including on completion", () =>
  Effect.gen(function* () {
    const session = yield* Session.Service
    const forms = yield* Form.Service
    const permissions = yield* Permission.Service
    const { client, states } = yield* setup
    const { id: sessionID } = yield* session.create({
      location: here,
      permissions: [{ action: "read", resource: "*", effect: "ask" }],
    })
    states.set(sessionID, "needs_reply")
    const check = Effect.fnUntraced(function* (state: SessionAttention.Status) {
      expect(yield* client.get({ sessionID })).toBe(state)
      expect((yield* client.list({})).data).toEqual([{ sessionID, state }])
      expect((yield* client.list({ needsUser: true })).data).toEqual(
        state === "completed" ? [] : [{ sessionID, state }],
      )
    })
    yield* check("needs_reply")
    const form = yield* forms.create({ sessionID, title: "Input", fields: [{ key: "name", type: "string" }] })
    yield* check("needs_form")
    const permission = yield* permissions.ask({ sessionID, action: "read", resources: ["file"] })
    yield* check("needs_permission")
    expect(yield* client.complete({ sessionID })).toBe("needs_permission")
    yield* permissions.reply({ requestID: permission.id, reply: "once" })
    yield* check("needs_form")
    yield* forms.cancel(form.id)
    yield* check("completed")
  }),
)

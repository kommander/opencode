import type { Context } from "@opencode/plugin/effect/plugin"
import { SessionAttention } from "@opencode/schema/session-attention"
import { Effect, Option } from "effect"
import { Form } from "../form.js"
import { Permission } from "../permission.js"
import { Session } from "../session.js"

export const register = Effect.fn("SessionAttention.register")(function* (
  ctx: Pick<Context, "location"> & { readonly rpc: Pick<Context["rpc"], "register"> },
  read: (id: Session.ID) => Effect.Effect<SessionAttention.Status>,
  complete: (id: Session.ID) => Effect.Effect<void>,
) {
  const sessions = yield* Session.Service
  const permissions = yield* Permission.Service
  const forms = yield* Form.Service
  const local = Effect.fnUntraced(function* (id: Session.ID) {
    const session = yield* sessions.get(id).pipe(Effect.option)
    return (
      Option.isSome(session) &&
      session.value.location.directory === ctx.location.directory &&
      session.value.location.workspaceID === ctx.location.workspaceID
    )
  })

  // Snapshot the live requests once per call, not once per row in a session page.
  const resolver = Effect.gen(function* () {
    const permission = new Set((yield* permissions.list()).map((request) => request.sessionID))
    const form = new Set((yield* forms.list()).map((request) => request.sessionID))
    return (id: Session.ID) => {
      if (permission.has(id)) return Effect.succeed("needs_permission" as const)
      if (form.has(id)) return Effect.succeed("needs_form" as const)
      return read(id)
    }
  })

  return yield* ctx.rpc
    .register(SessionAttention.Definition, {
      list: Effect.fnUntraced(function* (input) {
        const workspaceID = ctx.location.workspaceID ?? null
        // Session.list honors cursor filters instead of first-page filters. Never forward an unscoped cursor.
        if (input.cursor !== undefined) {
          const cursor = yield* Session.ListCursor.parse(input.cursor).pipe(Effect.option)
          if (
            !Option.exists(
              cursor,
              (page) =>
                "directory" in page && page.directory === ctx.location.directory && page.workspaceID === workspaceID,
            )
          )
            return { data: [] }
        }
        const page = yield* sessions
          .list({ cursor: input.cursor, limit: input.limit, directory: ctx.location.directory, workspaceID })
          .pipe(Effect.orDie)
        const resolve = yield* resolver
        const data = yield* Effect.forEach(page.data, (row) =>
          resolve(row.id).pipe(Effect.map((state) => ({ sessionID: row.id, state }))),
        )
        return {
          data: input.needsUser ? data.filter((row) => row.state.startsWith("needs_")) : data,
          next: page.cursor.next,
        }
      }),
      get: Effect.fnUntraced(function* ({ sessionID }) {
        if (!(yield* local(sessionID))) return null
        const resolve = yield* resolver
        return yield* resolve(sessionID)
      }),
      complete: Effect.fnUntraced(function* ({ sessionID }) {
        if (!(yield* local(sessionID))) return null
        yield* complete(sessionID)
        const resolve = yield* resolver
        return yield* resolve(sessionID)
      }),
    })
    .pipe(Effect.orDie)
})

export * as SessionSearchTools from "./session-search.js"

import { ToolFailure } from "@opencode/ai/effect"
import type { Context } from "@opencode/plugin/effect/plugin"
import { optional, PositiveInt } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { DateTime, Effect, Schema } from "effect"
import { Config } from "../../config.js"
import { ConfigEntryObserver } from "../../config/plugin/entry-observer.js"
import { Flag } from "../../flag.js"
import { Permission } from "../../permission.js"
import { SessionStore } from "../../session/store.js"

const Page = { limit: optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))), cursor: optional(Schema.String) }
const Search = Schema.Struct({
  ...Page,
  query: optional(Schema.NonEmptyString),
  sessionID: optional(Session.ID),
  includeMessages: optional(Schema.Boolean),
  includeToolOutput: optional(Schema.Boolean),
})
const Inspect = Schema.Struct({ ...Page, sessionID: Session.ID })
const Summary = Schema.Struct({ sessionID: Session.ID, title: Schema.String })
const Message = Schema.Struct({ id: SessionMessage.ID, type: Schema.String, time: Schema.Number, text: Schema.String })
const Hit = Schema.Struct({
  messageID: SessionMessage.ID,
  type: Schema.String,
  time: Schema.Number,
  cursor: optional(Schema.String),
  message: optional(Message),
})
const Group = Schema.Struct({ ...Summary.fields, hits: Schema.Array(Hit) })

// Tokens carry positions, not authority. Reapply the invocation's location and session filter on every page.
const SearchPosition = Schema.fromJsonString(
  Schema.Struct({
    id: Session.ID,
    time: Schema.Finite,
    after: optional(SessionMessage.ID),
    query: Search.fields.query,
    sessionID: Search.fields.sessionID,
  }),
)
const InspectPosition = Schema.fromJsonString(
  Schema.Struct({ sessionID: Session.ID, id: SessionMessage.ID, direction: Schema.Literals(["previous", "next"]) }),
)
const searchCursor = Schema.encodeSync(SearchPosition)
const inspectCursor = Schema.encodeSync(InspectPosition)
const invalid = () => new ToolFailure({ message: "Invalid session history cursor" })
const summary = (info: Session.Info) => ({ sessionID: info.id, title: info.title ?? "Untitled session" })

// Project transcript fields explicitly; attachment bytes, provider replay data and arbitrary metadata are not transcript text.
function transcript(message: SessionMessage.MediaInfo, output: boolean): string {
  switch (message.type) {
    case "assistant":
      return message.content
        .map((part) => {
          if (part.type !== "tool") return part.text
          const input = JSON.stringify(part.state.input, (key, value: unknown) =>
            /^(?:base64|data)$/i.test(key) || (typeof value === "string" && value.startsWith("data:"))
              ? "[omitted]"
              : value,
          )
          return [
            part.name,
            input,
            ...(output && "content" in part.state
              ? (part.state.content ?? []).flatMap((item) => (item.type === "text" ? [item.text] : []))
              : []),
            output && part.state.status === "error" ? part.state.error.message : "",
          ]
            .filter(Boolean)
            .join("\n")
        })
        .join("\n")
    case "user":
    case "synthetic":
    case "system":
    case "skill":
      return message.text
    case "shell":
      return [message.command, output && message.output?.output].filter(Boolean).join("\n")
    case "compaction":
      return message.status === "failed" ? message.error.message : message.summary
    case "agent-switched":
      return message.agent
    case "model-switched":
      return `${message.model.providerID}/${message.model.id}`
    case "location-switched":
      return message.location.directory
    case "idle":
      return message.error?.message ?? message.outcome
  }
}

function view(message: SessionMessage.MediaInfo, text: string) {
  return {
    id: message.id,
    type: message.type,
    time: DateTime.toEpochMillis(message.time.created),
    text: text.length <= 8000 ? text : `${text.slice(0, 8000)}…`,
  }
}

export const Plugin = {
  id: "opencode.tool.session-search",
  effect: Effect.fn("SessionSearchTools.Plugin")(function* (ctx: Context) {
    const store = yield* SessionStore.Service
    const permission = yield* Permission.Service
    const flags = yield* Flag.Service
    const config = yield* Config.Service
    const loaded = yield* ConfigEntryObserver.observe(config, ctx.event, ctx.tool.reload())
    const enabled = () => flags.resolve(loaded.entries).session_search.enabled
    const local = (info: Session.Info) =>
      info.location.directory === ctx.location.directory && info.location.workspaceID === ctx.location.workspaceID
    const get = (id: Session.ID) =>
      store
        .get(id)
        .pipe(
          Effect.flatMap((info) =>
            info && local(info)
              ? Effect.succeed(info)
              : Effect.fail(new ToolFailure({ message: "Session not found in this location" })),
          ),
        )
    const failure = (error: { readonly message: string }) => new ToolFailure({ message: error.message })

    const search = Effect.fn("SessionSearchTools.search")(function* (
      input: typeof Search.Type,
      pos?: typeof SearchPosition.Type,
    ) {
      const limit = input.limit ?? 20
      const query = input.query?.toLowerCase()
      const sessions: (typeof Group.Type)[] = []
      const result = (nextCursor?: string) => ({ sessions, nextCursor })
      const batch = 32
      const candidates = input.sessionID
        ? [yield* get(input.sessionID)]
        : yield* store.list({
            directory: ctx.location.directory,
            workspaceID: ctx.location.workspaceID ?? null,
            anchor: pos && { id: pos.id, time: pos.time, direction: "next" },
            limit: batch + 1,
          })
      if (!input.sessionID && pos?.after) candidates.unshift(yield* get(pos.id))
      let remaining = limit
      let budget = 128
      for (const [index, info] of candidates.slice(0, batch).entries()) {
        if (!local(info)) continue
        const position = {
          id: info.id,
          query: input.query,
          sessionID: input.sessionID,
          time: info.id === pos?.id ? pos.time : DateTime.toEpochMillis(info.time.updated),
        }
        const after = info.id === pos?.id ? pos.after : undefined
        const hits: (typeof Hit.Type)[] = []
        const group = { ...summary(info), hits }
        if (query) {
          const rows = yield* store.messagesWithRefs({
            sessionID: info.id,
            order: "asc",
            limit: budget + 1,
            cursor: after && { id: after, direction: "next" },
          })
          let previous = after
          for (const [i, row] of rows.entries()) {
            const text = transcript(row, true)
            if (text.toLowerCase().includes(query)) {
              hits.push({
                messageID: row.id,
                type: row.type,
                time: DateTime.toEpochMillis(row.time.created),
                cursor: previous && inspectCursor({ sessionID: info.id, id: previous, direction: "next" }),
                message: input.includeMessages
                  ? view(row, input.includeToolOutput ? text : transcript(row, false))
                  : undefined,
              })
              remaining--
            }
            previous = row.id
            budget--
            if ((budget === 0 || remaining === 0) && i < rows.length - 1) {
              if (hits.length || (!after && group.title.toLowerCase().includes(query))) sessions.push(group)
              return result(searchCursor({ ...position, after: row.id }))
            }
          }
        }
        if (hits.length || (!after && (!query || group.title.toLowerCase().includes(query)))) {
          sessions.push(group)
          if (!hits.length) remaining--
        }
        if (remaining === 0 || budget === 0 || index === batch - 1)
          return result(index < candidates.length - 1 ? searchCursor(position) : undefined)
      }
      return result()
    })

    yield* ctx.tool
      .transform((editor) => {
        if (!enabled()) return
        editor.namespace({
          name: "sessions",
          description: "Search and inspect session history in the current location.",
        })
        editor.add({
          name: "search",
          options: { namespace: "sessions" },
          input: Search,
          description:
            "List sessions, or search titles, messages, commands and tool calls for literal text (case-insensitive). Continue with nextCursor even after an empty page; the cursor keeps the query and session filter. includeMessages adds transcript excerpts to hits; includeToolOutput includes result text in excerpts.",
          output: Schema.Struct({ sessions: Schema.Array(Group), nextCursor: optional(Schema.String) }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if (!enabled()) return yield* Effect.fail(new ToolFailure({ message: "Session history tools disabled" }))
              const pos =
                input.cursor === undefined
                  ? undefined
                  : yield* Schema.decodeUnknownEffect(SearchPosition)(input.cursor).pipe(Effect.mapError(invalid))
              if (
                pos &&
                ((input.sessionID !== undefined && input.sessionID !== pos.sessionID) ||
                  (input.query !== undefined && input.query !== pos.query))
              )
                return yield* Effect.fail(invalid())
              const sessionID = input.sessionID ?? pos?.sessionID
              yield* permission.assert({
                action: "sessions_search",
                resources: [sessionID ?? ctx.location.directory],
                save: ["*"],
                ...Permission.invocation(context),
              })
              const output = yield* search({ ...input, query: input.query ?? pos?.query, sessionID }, pos)
              return { output, content: JSON.stringify(output) }
            }).pipe(Effect.mapError(failure)),
        })
        editor.add({
          name: "inspect",
          options: { namespace: "sessions" },
          input: Inspect,
          description:
            "Read a chronological transcript page. Pass the hit's cursor to start at the matching message (omit for the first message). Use previousCursor or nextCursor to navigate.",
          output: Schema.Struct({
            session: Summary,
            messages: Schema.Array(Message),
            previousCursor: optional(Schema.String),
            nextCursor: optional(Schema.String),
          }),
          execute: (input, context) =>
            Effect.gen(function* () {
              if (!enabled()) return yield* Effect.fail(new ToolFailure({ message: "Session history tools disabled" }))
              yield* permission.assert({
                action: "sessions_inspect",
                resources: [input.sessionID],
                save: ["*"],
                ...Permission.invocation(context),
              })
              const info = yield* get(input.sessionID)
              const pos =
                input.cursor === undefined
                  ? undefined
                  : yield* Schema.decodeUnknownEffect(InspectPosition)(input.cursor).pipe(Effect.mapError(invalid))
              if (pos && pos.sessionID !== info.id) return yield* Effect.fail(invalid())
              const limit = input.limit ?? 20
              const rows = yield* store.messagesWithRefs({
                sessionID: info.id,
                order: "asc",
                limit: limit + 1,
                cursor: pos,
              })
              const backward = pos?.direction === "previous"
              const page = backward ? rows.slice(-limit) : rows.slice(0, limit)
              const first = page[0]
              const last = page.at(-1)
              const output = {
                session: summary(info),
                messages: page.map((row) => view(row, transcript(row, true))),
                previousCursor:
                  first && (backward ? rows.length > limit : pos)
                    ? inspectCursor({ sessionID: info.id, id: first.id, direction: "previous" })
                    : undefined,
                nextCursor:
                  last && (backward || rows.length > limit)
                    ? inspectCursor({ sessionID: info.id, id: last.id, direction: "next" })
                    : undefined,
              }
              return { output, content: JSON.stringify(output) }
            }).pipe(Effect.mapError(failure)),
        })
      })
      .pipe(Effect.orDie)
  }),
}

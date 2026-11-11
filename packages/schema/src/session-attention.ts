export * as SessionAttention from "./session-attention.js"

import { Schema } from "effect"
import { Rpc } from "./rpc.js"
import { optional } from "./schema.js"
import { Session } from "./session.js"

export const Status = Schema.Literals([
  "unclassified",
  "cleared",
  "needs_reply",
  "needs_form",
  "needs_permission",
  "completed",
]).annotate({ identifier: "SessionAttention.Status" })
export type Status = typeof Status.Type

const session = Schema.toStandardSchemaV1(Schema.Struct({ sessionID: Session.ID }))
const status = Schema.toStandardSchemaV1(Schema.NullOr(Status))

/** Experimental: may change without compatibility guarantees. `changed` invalidates the effective state. */
export const Definition = Rpc.define({
  id: "experimental.session-attention",
  methods: {
    list: {
      input: Schema.toStandardSchemaV1(
        Schema.Struct({
          cursor: optional(Session.ListCursor),
          limit: optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
          /** Filter this source page to sessions needing user action. Follow `next` even when `data` is empty. */
          needsUser: optional(Schema.Literal(true)),
        }),
      ),
      output: Schema.toStandardSchemaV1(
        Schema.Struct({
          data: Schema.Array(Schema.Struct({ sessionID: Session.ID, state: Status })),
          next: optional(Session.ListCursor),
        }),
      ),
    },
    get: { input: session, output: status },
    complete: { input: session, output: status },
  },
  events: { changed: { schema: session } },
})

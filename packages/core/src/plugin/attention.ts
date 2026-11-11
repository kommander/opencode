export * as AttentionPlugin from "./attention.js"

import { define } from "@opencode/plugin/effect/plugin"
import type { StorageDomain } from "@opencode/plugin/effect/storage"
import { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import type { SessionAttention } from "@opencode/schema/session-attention"
import { Effect, FiberMap, Schema, ScopedRef, Stream } from "effect"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { Flag } from "../flag.js"
import { Form } from "../form.js"
import { Permission } from "../permission.js"
import { classify } from "./attention-classifier.js"
import { register } from "./attention-rpc.js"

type Disposition = Exclude<SessionAttention.Status, "needs_form" | "needs_permission">
const stored = Schema.is(Schema.Literals(["cleared", "completed", "needs_reply"]))

// Only the prose verdict and explicit dispositions survive a restart. Structured requests
// belong to Form and Permission; the RPC overlays their current snapshots at read time.
export const track = Effect.fn("Attention.track")(function* <R>(
  storage: Pick<StorageDomain, "get" | "set" | "remove">,
  classify: (
    id: Session.ID,
    messageID: SessionMessage.ID,
  ) => Effect.Effect<Exclude<Disposition, "completed">, never, R>,
  changed: (id: Session.ID) => Effect.Effect<void>,
) {
  const jobs = yield* FiberMap.make<Session.ID>()
  const locks = yield* KeyedMutex.make<Session.ID>()
  const read = Effect.fn("Attention.read")(function* (id: Session.ID): Effect.fn.Return<Disposition> {
    const value = yield* storage.get(id)
    return stored(value) ? value : "unclassified"
  })
  const write = Effect.fn("Attention.write")(function* (id: Session.ID, state: Disposition) {
    if ((yield* read(id)) === state) return
    yield* state === "unclassified" ? storage.remove(id) : storage.set(id, state)
    yield* changed(id)
  })
  const reset = Effect.fn("Attention.reset")(function* (id: Session.ID, state: Disposition, reopen = false) {
    yield* FiberMap.remove(jobs, id)
    yield* locks.withLock(id)(
      Effect.gen(function* () {
        if (!reopen && (yield* read(id)) === "completed") return
        yield* write(id, state)
      }),
    )
  })
  return {
    read,
    complete: (id: Session.ID) => reset(id, "completed"),
    user: (id: Session.ID) => reset(id, "cleared", true),
    started: (id: Session.ID) => reset(id, "cleared"),
    unknown: (id: Session.ID) => reset(id, "unclassified"),
    remove: (id: Session.ID) =>
      FiberMap.remove(jobs, id).pipe(Effect.andThen(storage.remove(id)), Effect.andThen(changed(id))),
    cancel: (id: Session.ID) => FiberMap.remove(jobs, id),
    succeeded: Effect.fn("Attention.succeeded")(function* (id: Session.ID, messageID?: SessionMessage.ID) {
      if ((yield* read(id)) === "completed") return
      yield* reset(id, "unclassified")
      if (!messageID) return
      yield* FiberMap.run(
        jobs,
        id,
        Effect.gen(function* () {
          const state = yield* classify(id, messageID)
          yield* locks.withLock(id)(
            Effect.gen(function* () {
              if ((yield* read(id)) === "completed") return
              yield* write(id, state)
            }),
          )
        }),
      )
    }),
  }
})

export const Plugin = define({
  id: "opencode.session-attention",
  effect: Effect.fn(function* (ctx) {
    const flags = yield* Flag.Service
    const activate = Effect.gen(function* () {
      const steps = new Map<Session.ID, SessionMessage.ID>()
      const tracker = yield* track(ctx.storage, classify, changed)
      const rpc = yield* register(ctx, tracker.read, tracker.complete).pipe(Effect.orDie)
      function changed(sessionID: Session.ID): Effect.Effect<void> {
        return rpc.events
          .emit("changed", { sessionID })
          .pipe(Effect.ignoreCause({ log: "Warn", message: "Could not publish session attention change" }))
      }
      // Replied is published before settlement (and assert interruption publishes no reply).
      // Invalidate only after the authoritative pending snapshot has changed.
      const forms = yield* Form.Service
      const permissions = yield* Permission.Service
      yield* forms.pendingChanges.pipe(
        Stream.filter(Schema.is(Session.ID)),
        Stream.runForEach(changed),
        Effect.forkScoped({ startImmediately: true }),
      )
      yield* permissions.pendingChanges.pipe(Stream.runForEach(changed), Effect.forkScoped({ startImmediately: true }))
      yield* ctx.event
        .subscribe({
          types: [
            "session.inbox.enqueued",
            "session.step.ended",
            "session.step.failed",
            "session.execution.started",
            "session.execution.succeeded",
            "session.execution.failed",
            "session.execution.interrupted",
            "session.deleted",
            "session.moved",
          ],
        })
        .pipe(
          Stream.runForEach((event) => {
            const id = event.data.sessionID
            switch (event.type) {
              case "session.step.ended":
                if (event.data.finish === "stop") steps.set(id, event.data.assistantMessageID)
                else steps.delete(id)
                return Effect.void
              case "session.step.failed":
                steps.delete(id)
                return Effect.void
              case "session.inbox.enqueued":
                if (event.data.item.type !== "user") return Effect.void
                steps.delete(id)
                return tracker.user(id)
              case "session.execution.started":
                steps.delete(id)
                return tracker.started(id)
              case "session.execution.succeeded": {
                const message = steps.get(id)
                steps.delete(id)
                return tracker.succeeded(id, message)
              }
              case "session.execution.failed":
              case "session.execution.interrupted":
                steps.delete(id)
                return tracker.unknown(id).pipe(Effect.andThen(changed(id)))
              case "session.deleted":
                steps.delete(id)
                return tracker.remove(id)
              case "session.moved":
                steps.delete(id)
                return tracker.cancel(id).pipe(Effect.andThen(changed(id)))
            }
          }),
          Effect.forkScoped({ startImmediately: true }),
        )
    })
    // Built-ins are not reactivated by configuration changes. Closing the scope removes
    // the RPC and cancels outstanding inference when the feature is switched off.
    const active = yield* ScopedRef.make(() => false)
    const refresh = Effect.gen(function* () {
      const enabled = yield* flags.enabled("session_attention")
      if (enabled === (yield* ScopedRef.get(active))) return
      yield* ScopedRef.set(active, enabled ? activate.pipe(Effect.as(true)) : Effect.succeed(false))
    })
    yield* ctx.event.subscribe({ types: ["config.updated"] }).pipe(
      Stream.runForEach(() => refresh),
      Effect.forkScoped({ startImmediately: true }),
    )
    yield* refresh
  }),
})

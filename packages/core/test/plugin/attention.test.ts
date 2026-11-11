import { expect } from "bun:test"
import { KV } from "@opencode/core/kv"
import { AttentionPlugin } from "@opencode/core/plugin/attention"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Deferred, Effect } from "effect"
import { testEffect } from "../lib/effect"

const it = testEffect(LayerNode.compile(KV.node))

it.effect("completion clears a reply request and only new user work reopens it", () =>
  Effect.gen(function* () {
    const kv = yield* KV.Service
    const id = Session.ID.create()
    const classified = yield* Deferred.make<void>()
    const tracker = yield* AttentionPlugin.track(
      kv,
      () => Effect.succeed("needs_reply"),
      () => Deferred.succeed(classified, undefined).pipe(Effect.asVoid),
    )
    expect(yield* tracker.read(id)).toBe("unclassified")
    yield* tracker.succeeded(id, SessionMessage.ID.create())
    yield* Deferred.await(classified)
    expect(yield* tracker.read(id)).toBe("needs_reply")
    yield* tracker.complete(id)
    yield* tracker.started(id)
    yield* tracker.succeeded(id, SessionMessage.ID.create())
    expect(yield* tracker.read(id)).toBe("completed")
    yield* tracker.user(id)
    expect(yield* tracker.read(id)).toBe("cleared")
    // A control-only drain has no final reply to send to a classifier.
    yield* tracker.succeeded(id)
    expect(yield* tracker.read(id)).toBe("unclassified")
  }),
)

it.effect("new input interrupts an in-flight verdict without bringing back the old request", () =>
  Effect.gen(function* () {
    const kv = yield* KV.Service
    const id = Session.ID.create()
    const began = yield* Deferred.make<void>()
    const cancelled = yield* Deferred.make<void>()
    const tracker = yield* AttentionPlugin.track(
      kv,
      () =>
        Deferred.succeed(began, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined)),
        ),
      () => Effect.void,
    )
    yield* tracker.succeeded(id, SessionMessage.ID.create())
    yield* Deferred.await(began)
    yield* tracker.user(id)
    yield* Deferred.await(cancelled)
    expect(yield* tracker.read(id)).toBe("cleared")
  }),
)

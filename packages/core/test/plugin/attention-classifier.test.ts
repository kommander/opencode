import { expect } from "bun:test"
import { Config } from "@opencode/core/config"
import { Generate } from "@opencode/core/generate"
import { classify } from "@opencode/core/plugin/attention-classifier"
import { Session } from "@opencode/core/session"
import { Document } from "@opencode/schema/config"
import { Model } from "@opencode/schema/model"
import { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Layer, Schema } from "effect"
import { it } from "../lib/effect"

const sessionID = Session.ID.create()
const messageID = SessionMessage.ID.create()
const document = Schema.decodeUnknownSync(Document)
const entries = [
  document({ type: "document", info: { experimental: { attention_model: "test/chat#fast" } } }),
  document({ type: "document", info: { experimental: { part_rows: true } } }),
]
const assistant = Schema.decodeUnknownSync(SessionMessage.Assistant)({
  id: messageID,
  type: "assistant",
  agent: "build",
  model: { providerID: "other", id: "other" },
  time: { created: 1, completed: 2 },
  finish: "stop",
  content: [
    { type: "text", text: "before-tool" },
    {
      type: "tool",
      id: "call",
      name: "read",
      time: { created: 1 },
      state: { status: "completed", input: {}, content: [{ type: "text", text: "tool-secret" }] },
    },
    { type: "reasoning", text: "reasoning-secret" },
    { type: "text", text: "progress-secret", native: { phase: "commentary" } },
    { type: "text", text: `omit-prefix${"x".repeat(4500)}Which option?` },
  ],
})

function run(step: SessionMessage.Info | undefined, output: Effect.Effect<string, Generate.Error>, config = entries) {
  const reads: unknown[] = []
  const calls: Generate.TextInput[] = []
  return classify(sessionID, messageID).pipe(
    Effect.provide([
      Config.testLayer(config),
      Layer.mock(Session.Service, {
        message: (input) => {
          reads.push(input)
          return Effect.succeed(step)
        },
        revert: {
          stage: () => Effect.die("unused"),
          clear: () => Effect.die("unused"),
          commit: () => Effect.die("unused"),
        },
      }),
      Layer.mock(Generate.Service, {
        text: (input) => {
          calls.push(input)
          return output
        },
      }),
    ]),
    Effect.map((result) => ({ result, reads, calls })),
  )
}

it.effect("sends only the bounded final text to the explicit model", () =>
  Effect.gen(function* () {
    const { result, reads, calls } = yield* run(assistant, Effect.succeed(" needs_reply\n"))
    expect(reads).toEqual([{ sessionID, messageID }])
    expect(calls).toHaveLength(1)
    expect(calls[0]?.model).toEqual(Model.Ref.parse("test/chat#fast"))
    expect(calls[0]?.prompt.split("\n").at(-1)).toBe(JSON.stringify("Which option?".padStart(4000, "x")))
    expect(result).toBe("needs_reply")
  }),
)

it.effect("does not generate without an explicit model or if the indicated step has no final answer", () =>
  Effect.gen(function* () {
    const reply = Effect.succeed("cleared")
    expect(yield* run(assistant, reply, [])).toEqual({ result: "unclassified", calls: [], reads: [] })
    for (const step of [
      undefined,
      Schema.decodeUnknownSync(SessionMessage.User)({
        type: "user",
        id: "msg_new",
        time: { created: 3 },
        text: "new task",
      }),
      { ...assistant, finish: "length" as const },
      { ...assistant, content: assistant.content.slice(0, -1) },
    ]) {
      const outcome = yield* run(step, reply)
      expect(outcome.calls).toEqual([])
      expect(outcome.result).toBe("unclassified")
    }
  }),
)

it.effect("rejects unusable output and generation failures rather than treating them as cleared", () =>
  Effect.gen(function* () {
    expect((yield* run(assistant, Effect.succeed("cleared"))).result).toBe("cleared")
    expect((yield* run(assistant, Effect.succeed("probably cleared"))).result).toBe("unclassified")
    expect((yield* run(assistant, Effect.fail(new Generate.UnavailableError({ message: "offline" })))).result).toBe(
      "unclassified",
    )
  }),
)

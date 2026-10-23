import { intro, select, spinner } from "@clack/prompts"
import { ConfigProvider, Effect, Option } from "effect"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { handlePromptErrors, prompt, requireInteractive } from "../../ui/prompt"
import { createClient, request } from "./auth/shared"
import tui from "./default"

export default Runtime.handler(
  Commands.commands.peer,
  Effect.fn("cli.peer")(function* (input) {
    const gateway = yield* choose(input).pipe(handlePromptErrors)
    if (!gateway) return
    yield* tui({
      server: Option.some(gateway.url),
      standalone: false,
      directory: Option.none(),
      continue: false,
      session: Option.none(),
      prompt: Option.none(),
      auto: false,
      yolo: false,
      dangerouslySkipPermissions: false,
    }).pipe(
      // The TUI reads the --server password from OPENCODE_PASSWORD.
      Effect.provide(
        ConfigProvider.layerAdd(ConfigProvider.fromEnv({ env: { OPENCODE_PASSWORD: gateway.token } }), {
          asPrimary: true,
        }),
      ),
    )
  }),
)

const choose = Effect.fn("cli.peer.choose")(function* (input: Runtime.Input<typeof Commands.commands.peer>) {
  yield* requireInteractive("opencode peer needs an interactive terminal")
  intro("Connect to another OpenCode server")
  const client = yield* createClient({ server: Option.getOrUndefined(input.server), standalone: input.standalone })
  const peers = yield* request((signal) => client.peer.list({ signal }))
  if (peers.length === 0) return yield* Effect.fail(new Error("No other OpenCode servers found"))
  const id = yield* prompt<string>(() =>
    select({
      message: "Select a server",
      options: peers.map((peer) => ({
        value: peer.id,
        label: peer.name,
        hint: [
          peer.id.slice(0, 8),
          peer.version,
          peer.connected ? "connected" : `seen ${Math.round((Date.now() - peer.updated) / 60_000)}m ago`,
        ]
          .filter(Boolean)
          .join(" · "),
      })),
    }),
  )
  const progress = spinner()
  progress.start("Connecting...")
  return yield* request((signal) => client.peer.connect({ id }, { signal })).pipe(
    Effect.tap(() => Effect.sync(() => progress.stop("Connected"))),
    Effect.tapCause(() => Effect.sync(() => progress.stop("Failed to connect", 1))),
  )
})

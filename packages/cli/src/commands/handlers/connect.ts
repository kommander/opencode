import { intro, select, spinner } from "@clack/prompts"
import { Effect, Option } from "effect"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { handlePromptErrors, prompt, requireInteractive } from "../../ui/prompt"
import { createClient, request } from "./auth/shared"
import { tui } from "./default"

export default Runtime.handler(
  Commands.commands.connect,
  Effect.fn("cli.connect")(function* (input) {
    const gateway = yield* choose(input).pipe(handlePromptErrors)
    if (!gateway) return
    yield* tui(
      {
        server: Option.some(gateway.url),
        standalone: false,
        directory: Option.none(),
        continue: false,
        session: Option.none(),
        prompt: Option.none(),
        auto: false,
        yolo: false,
        dangerouslySkipPermissions: false,
      },
      gateway.token,
    )
  }),
)

// The local server owns the peer list and the iroh gateway; the TUI then talks to the gateway like any server.
const choose = Effect.fn("cli.connect.choose")(function* (input: Runtime.Input<typeof Commands.commands.connect>) {
  yield* requireInteractive("opencode connect needs an interactive terminal")
  intro("Connect to another OpenCode server")
  const client = yield* createClient({ server: Option.getOrUndefined(input.server), standalone: input.standalone })
  const peers = yield* request((signal) => client.peer.list({ signal }))
  if (peers.length === 0)
    return yield* Effect.fail(new Error("No other OpenCode servers are signed in to your OpenCode Console account"))
  const id = yield* prompt<string>(() =>
    select({
      message: "Select a server",
      options: peers.map((peer) => ({
        value: peer.id,
        label: peer.name,
        hint: [peer.id.slice(0, 8), peer.version, peer.online ? "online" : `seen ${ago(peer.updated)}`]
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

function ago(time: number) {
  const minutes = Math.round((Date.now() - time) / 60_000)
  if (minutes < 60) return `${minutes}m ago`
  if (minutes < 48 * 60) return `${Math.round(minutes / 60)}h ago`
  return `${Math.round(minutes / 1440)}d ago`
}

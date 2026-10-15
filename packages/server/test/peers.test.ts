import { expect } from "bun:test"
import { Effect } from "effect"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { HttpServer } from "effect/http"
import { it } from "../../core/test/lib/effect"
import { ServerProcess } from "../src/process"
import { received } from "./fixture/peer-provider"

const start = (password: string, config: string) =>
  ServerProcess.start<never, never>({
    hostname: "127.0.0.1",
    port: 0,
    password,
    app: { version: password },
    database: { path: ":memory:" },
    models: { fetch: false },
    config: { directory: config, project: false, claude: false, content: "{}" },
    fs: { filewatcher: false, fff: false },
  })

it.live(
  "proxies HTTP, SSE and WebSockets to provider peers without forwarding local credentials",
  () =>
    Effect.gen(function* () {
      const remote = yield* start("remote", process.env.XDG_CONFIG_HOME!)
      const config = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-peers-")))
      yield* Effect.addFinalizer(() => Effect.promise(() => fs.rm(config, { recursive: true, force: true })))
      yield* Effect.promise(() =>
        Bun.write(
          path.join(config, "opencode.jsonc"),
          JSON.stringify({
            peers: [
              {
                package: import.meta.dir + "/fixture/peer-provider.ts",
                options: {
                  target: HttpServer.formatAddress(remote.address),
                  authorization: `Basic ${btoa("opencode:remote")}`,
                },
              },
            ],
          }),
        ),
      )
      const local = yield* start("local", config)
      const base = HttpServer.formatAddress(local.address)
      const token = btoa("opencode:local")
      const request = (path: string, init: RequestInit = {}) =>
        fetch(base + path, { ...init, headers: { authorization: `Basic ${token}`, ...init.headers } })

      yield* Effect.promise(async () => {
        expect((await fetch(base + "/api/peers")).status).toBe(401)
        expect(await (await request("/api/peers")).json()).toEqual([{ id: "remote", name: "Remote", status: "online" }])
        expect((await request("/api/peers/missing/api/info")).status).toBe(404)

        const info = await request("/api/peers/remote/api/info", { headers: { cookie: "local=secret" } })
        expect(await info.json()).toMatchObject({ version: "remote" })
        expect(received.at(-1)?.has("authorization")).toBe(false)
        expect(received.at(-1)?.has("cookie")).toBe(false)

        const stream = await request("/api/peers/remote/api/event")
        const events = stream.body!.getReader()
        await readUntil(events, "server.connected")
        await events.cancel()

        const directory = process.env.XDG_CONFIG_HOME!
        const query = "?location[directory]=" + encodeURIComponent(directory)
        const created = await request("/api/peers/remote/api/pty" + query, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            command: "/bin/sh",
            args: [
              "-c",
              "stty -echo; printf 'ready\\n'; while IFS= read -r line; do printf 'reply:%s\\n' \"$line\"; done",
            ],
            cwd: directory,
          }),
        })
        const pty = (await created.json()) as { data: { id: string } }
        const url = new URL(base + `/api/peers/remote/api/pty/${pty.data.id}/connect` + query + "&auth_token=" + token)
        url.protocol = "ws:"
        const socket = new WebSocket(url)
        socket.binaryType = "arraybuffer"
        const output = new ReadableStream<Uint8Array>({
          start(stream) {
            socket.addEventListener("message", (event) =>
              stream.enqueue(
                typeof event.data === "string" ? new TextEncoder().encode(event.data) : new Uint8Array(event.data),
              ),
            )
            socket.addEventListener("close", () => stream.close())
          },
        }).getReader()
        await readUntil(output, "ready")
        socket.send("text\n")
        await readUntil(output, "reply:text")
        socket.close()
      })
    }),
  30_000,
)

async function readUntil(reader: ReadableStreamDefaultReader<Uint8Array>, text: string) {
  let result = ""
  while (!result.includes(text)) {
    const item = await reader.read()
    if (item.done) throw new Error(`Stream closed before ${text}`)
    result += new TextDecoder().decode(item.value)
  }
}

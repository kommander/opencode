import { ServerPlugin } from "@opencode/plugin/server"

export const received: Headers[] = []

// Test-only server plugin: one peer behind a loopback proxy that adds the peer server's credentials.
export default ServerPlugin.define({
  id: "test-peers",
  setup: (context) => {
    context.routes.serve(async (request) =>
      Response.json({ method: request.method, path: new URL(request.url).pathname, body: await request.text() }),
    )
    const target = String(context.options.target)
    const authorization = String(context.options.authorization)
    const proxy = Bun.serve<{ remote: WebSocket; pending: (string | Buffer)[] }>({
      hostname: "127.0.0.1",
      port: 0,
      fetch(request, server) {
        received.push(request.headers)
        const url = new URL(request.url)
        const upstream = target + url.pathname + url.search
        if (request.headers.get("upgrade") === "websocket") {
          // @ts-expect-error Bun's WebSocket accepts request headers.
          const remote = new WebSocket(upstream.replace("http", "ws"), { headers: { authorization } })
          remote.binaryType = "arraybuffer"
          return server.upgrade(request, { data: { remote, pending: [] } })
            ? undefined
            : new Response(null, { status: 400 })
        }
        const headers = new Headers(request.headers)
        headers.set("authorization", authorization)
        return fetch(upstream, { method: request.method, headers, body: request.body })
      },
      websocket: {
        open(socket) {
          const remote = socket.data.remote
          remote.addEventListener("open", () => socket.data.pending.splice(0).forEach((data) => remote.send(data)))
          remote.addEventListener("message", (event) =>
            socket.send(typeof event.data === "string" ? event.data : new Uint8Array(event.data)),
          )
          remote.addEventListener("close", () => socket.close())
        },
        message(socket, data) {
          const remote = socket.data.remote
          if (remote.readyState === WebSocket.OPEN) remote.send(data)
          else socket.data.pending.push(data)
        },
        close(socket) {
          socket.data.remote.close()
        },
      },
    })
    context.peers.provide({
      list: () => [{ id: "remote", name: "Remote", status: "online" }],
      connect: async () => proxy.url.origin,
    })
    return async () => {
      await proxy.stop(true)
    }
  },
})

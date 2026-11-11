# Session search

A private, opt-in Effect server plugin for browsing session history. It requires an OpenCode host with **`ctx.session.message.list`**; installing the example does not upgrade an older host.

## Run locally

1. From the repository root, run `bun install --frozen-lockfile`. The example is a workspace package for development; it is **not** a built-in or enabled by default. No build or publish step is needed.
2. Add the package directory to your project's `opencode.jsonc` (use an absolute path if the project is elsewhere):

   ```jsonc
   {
     "$schema": "https://opencode.ai/config.json",
     "plugins": ["./examples/session-search"],
   }
   ```

3. Open the configured project with that host. In a development checkout containing the host change, `bun run dev --standalone` runs against a private server. Ask the agent to search earlier sessions for a phrase, then open a match. Remove the config entry to disable the plugin.

To verify without a model, run `bun run --cwd packages/core test test/plugin/session-search-example.test.ts` from the repository root. The test loads this package through project config and calls the tools against isolated session history.

## Tools

| Code Mode tool           | Input                                                                                 | Output                                                  |
| ------------------------ | ------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `tools.sessions.search`  | `query?`, `sessionID?`, `cursor?`, `limit?`, `includeMessages?`, `includeToolOutput?` | `sessions: [{ sessionID, title, hits }]`, `nextCursor?` |
| `tools.sessions.inspect` | `sessionID`, `cursor?`, `limit?`                                                      | `session`, `messages`, `previousCursor?`, `nextCursor?` |

Without a query, search lists sessions. A query is case-insensitive **literal text** (not a regex). A matching title can produce a group with no hits. Each message hit has `messageID`, `type`, `time` (Unix milliseconds), and an inspection cursor unless it's the first message. `includeMessages` adds a projected `{ id, type, time, text }` to each hit. Tool and shell result text is searchable even when excluded from the excerpt; set `includeToolOutput: true` to show it. Inspection includes that text by default. Each projected message is clipped after 8,000 characters.

Both tools default to `limit: 20` (range 1–100). Query pages scan **one session and up to 64 messages** per call. Continue with `nextCursor` even after an empty page. The plugin remembers the query, session filter, and projection options for the last 512 search continuations. After eviction or plugin reload, start a new search. Like the underlying host pages, cursors browse live history, not a snapshot.

```js
let page = await tools.sessions.search({ query: "release.100%", includeMessages: true })
for (;;) {
  const group = page.sessions.find((s) => s.hits.length)
  if (group) {
    return await tools.sessions.inspect({ sessionID: group.sessionID, cursor: group.hits[0].cursor, limit: 5 })
  }
  if (!page.nextCursor) return "No matching message"
  page = await tools.sessions.search({ cursor: page.nextCursor })
}
```

Inspection returns messages in chronological order in both directions. To move backward or forward, call `inspect` with the same `sessionID` and the returned `previousCursor` or `nextCursor`.

## Access and projection

The plugin reads through `ctx.session.list`, `get`, and `message.list`. It limits results to the plugin location's directory **and** workspace, including direct reads by ID. Every call checks `ctx.permission.assert` for the invoking session and agent. The actions are `sessions_search` (resource: filtered session ID or current directory) and `sessions_inspect` (resource: target session ID).

The tools return transcript text, not raw records. Attachments, non-text tool results, provider replay fields, reasoning, and arbitrary metadata are not indexed or returned. For tool calls, only the name, selected textual arguments (`command`, `code`, `path`, `filePath`, `pattern`, `query`), and text results/errors are projected. Visible transcript text can still contain sensitive information; use permissions accordingly.

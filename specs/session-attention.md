# Experimental session attention

Attention answers **does this session need a person?**, not **is this session running?** A running tool can be waiting for approval. A successful execution can end with a question. Neither `session.active`, the last execution `outcome`, nor the viewed watermark answers the attention question. Reading, viewing, or archiving a session does not complete its work.

The built-in server plugin is gated by `experimental.session_attention`. It exposes a typed, **Location-scoped** plugin RPC; no extra HTTP routes or changes to `Session.Info`. Each session (including a child or fork) has its own disposition, not its parent's. Clients can aggregate sessions into families if needed.

```ts
import { SessionAttention } from "@opencode/schema/session-attention"

const attention = client.rpc(SessionAttention.Definition)
const page = await attention.list({ needsUser: true, limit: 50 })
// Follow `next` even after an empty page; the limit bounds source sessions examined.
const state = await attention.get({ sessionID })
await attention.complete({ sessionID })
```

`list` returns `{data: [{sessionID, state}], next?}`; omit `needsUser` for a bulk read of all states. This is not a server-wide inbox. `get` and `complete` return the effective state, or `null` if the session isn't in this Location. Subscribe to `changed` and refresh the affected session; it is an invalidation, not a durable event. Subscribe before the initial snapshot and refetch on reconnect. As with other plugin RPCs, a cold Location can return `rpc.unavailable` until activation.

| State | When it applies |
| --- | --- |
| `needs_permission` | At least one outstanding permission request for this session. Highest priority. |
| `needs_form` | At least one pending session-owned form (including the question tool), with no pending permission. |
| `completed` | Explicit `complete`, **not** successful execution, archive, or a model's guess. New user input reopens it; automatic work does not. |
| `needs_reply` | The configured classifier found the final assistant text asks for information, a decision, or action needed to proceed. An optional follow-up offer is not a request. |
| `cleared` | A user prompt was admitted, a new execution started, or the last final reply was classified as not requiring a response. |
| `unclassified` | No observation yet, no qualifying final reply, interrupted/failed execution, or missing, inconclusive or failed classifier. This is not a negative verdict. |

`needsUser: true` selects only the three `needs_*` states. Pending requests come from the live Permission and Form owners, never a persisted counter or inferred prose. Resolving one reveals the next request or the underlying disposition. `complete` suppresses inferred attention, **not** an actual form or permission: answer or reject those through their existing endpoints. On reopen a pending request still takes precedence.

Prose inference is opt-in. Set `experimental.attention_model` to a **chat** model (e.g. `provider/model#variant`). With no selection there is no model call. After a successful busy period the plugin considers the last observed, normally finished step, sends only the last 4,000 characters of its final visible text, and stores the label, not the excerpt. Generation runs outside the session and does not keep it busy. A new prompt, execution, completion, move or deletion cancels outstanding classification. No transcript backfill: the plugin observes live events while enabled; a missed turn is not replayed or billed later. Structured requests are still available from authoritative snapshots after reconnect or plugin reload.

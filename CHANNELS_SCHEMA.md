# Claude Code Experimental Channels – Exact Wire Schema

**Status**: [DOCUMENTED] from official Claude Code documentation  
**Version**: Claude Code v2.1.80+ (introduced); current version supports channels as documented below  
**Authentication**: Anthropic console API key or claude.ai account; not available on Bedrock/Vertex/Foundry  

---

## 1. Capability Declaration

**[DOCUMENTED]** — from `/en/channels-reference.md`

When an MCP server wants to be a channel, it declares the capability in the `Server` constructor's `capabilities` object:

```typescript
{
  capabilities: {
    experimental: {
      'claude/channel': {}
    }
  }
}
```

### Schema Details

- **Key**: `experimental['claude/channel']` (exact string)
- **Value**: Always an empty object `{}`
- **Location**: Under `capabilities.experimental`, NOT top-level `capabilities`
- **Presence effect**: Registers a notification listener so Claude Code accepts `notifications/claude/channel` events
- **Required**: Yes, for any channel to function

### Optional: Permission Relay

To opt in to relay of permission prompts (tool approval dialogs), add:

```typescript
{
  capabilities: {
    experimental: {
      'claude/channel': {},
      'claude/channel/permission': {}  // opt in to permission relay
    },
    tools: {}  // required if exposing tools
  }
}
```

- **Key**: `experimental['claude/channel/permission']` (exact string)
- **Value**: Always an empty object `{}`
- **Requirement**: Only declare if your channel authenticates senders (has a gating mechanism); see Security section
- **Effect**: Enables Claude Code to forward `notifications/claude/channel/permission_request` to your server

### Optional: Reply Tool Discovery

To expose tools Claude can call (like `reply`):

```typescript
{
  capabilities: {
    experimental: { 'claude/channel': {} },
    tools: {}  // standard MCP capability; enables tool discovery
  }
}
```

- **Key**: `tools` (standard MCP, not channel-specific)
- **Value**: Always an empty object `{}`
- **Effect**: Triggers Claude Code to call `ListToolsRequestSchema` at connection to discover available tools

---

## 2. Launch and Registration

### Flag Syntax

**[DOCUMENTED]** — from `/en/channels.md` and `/en/channels-reference.md`

Custom channels run with the `--dangerously-load-development-channels` flag during research preview:

```bash
claude --dangerously-load-development-channels server:webhook
claude --dangerously-load-development-channels plugin:yourplugin@yourmarketplace
```

### Approved channels use `--channels`:

```bash
claude --channels plugin:telegram@claude-plugins-official
claude --channels plugin:discord@claude-plugins-official
claude --channels plugin:fakechat@claude-plugins-official
```

### Schema Details

- **Flag format**: `--dangerously-load-development-channels` (exact, with hyphens)
- **Entry format**: `server:<name>` or `plugin:<name>@<marketplace>`
  - `server:<name>`: bare MCP server registered in `.mcp.json` by name
  - `plugin:<name>@<marketplace>`: installed plugin name and marketplace
- **Multiple entries**: space-separated, e.g. `--dangerously-load-development-channels server:webhook plugin:custom@mymarketplace`
- **Bypass behavior**: Skips the approved allowlist for these entries only; `channelsEnabled` org policy still applies
- **Scope**: Development flag is independent of `--channels` entries; combining both doesn't extend the development bypass to `--channels` entries

### MCP Configuration

Channels run as MCP stdio servers, registered in `.mcp.json`:

```json
{
  "mcpServers": {
    "webhook": {
      "command": "bun",
      "args": ["./webhook.ts"]
    }
  }
}
```

- **Transport**: stdio (Claude Code spawns the server as a subprocess and communicates over stdin/stdout)
- **Subprocess**: Started automatically when Claude Code initializes; no manual server startup needed
- **Lifecycle**: Runs for the duration of the Claude Code session

---

## 3. Push / Wake Notification

### The Notification Method

**[DOCUMENTED]** — from `/en/channels-reference.md`, "Notification format" section

The MCP **server** (not Claude) sends a JSON-RPC **notification** to Claude Code to push an event into the session:

```typescript
await mcp.notification({
  method: 'notifications/claude/channel',
  params: {
    content: 'build failed on main: https://ci.example.com/run/1234',
    meta: { severity: 'high', run_id: '1234' }
  }
})
```

### Exact Wire Schema

**Method name**: `notifications/claude/channel` (exact string)

**Params object**:

```typescript
{
  content: string;              // required: the event body
  meta?: Record<string, string>;  // optional: contextual attributes
}
```

**Field details**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `content` | `string` | Yes | The event body. Delivered as the text content of the `<channel>` XML tag. |
| `meta` | `Record<string, string>` | No | Each key-value pair becomes an XML attribute on the `<channel>` tag. All values must be strings. Keys must be valid identifiers (letters, digits, underscores only); keys with hyphens or special characters are silently dropped. |

### Delivery Guarantee

- **Acknowledgment**: `await mcp.notification()` resolves when the JSON-RPC message is written to the transport, NOT when Claude has processed it
- **Delivery**: Events queue into the session and process in order
- **Handling missing channel**: If the session hasn't loaded your server as a channel (not passed in `--channels` or `--dangerously-load-development-channels`), OR if `channelsEnabled` org policy is false, events are silently dropped with no error returned to your server
- **Batching**: If multiple notifications arrive while Claude is busy, they're delivered together on the next turn

### Event as Claude Sees It

Claude receives the notification as an XML block injected into the conversation context:

```xml
<channel source="webhook" severity="high" run_id="1234">
build failed on main: https://ci.example.com/run/1234
</channel>
```

- **`source` attribute**: Set automatically from your MCP server's configured name
- **Other attributes**: Pulled from `meta` keys, in order
- **Content**: Exact `content` string from params, no escaping or modification
- **Instructions**: Your `instructions` string from the `Server` constructor is added to Claude's system prompt to explain how to handle these events

---

## 4. Reply Path (Two-Way Channels)

### Tool Registration and Discovery

**[DOCUMENTED]** — from `/en/channels-reference.md`, "Expose a reply tool" section

One-way channels push events only. Two-way channels expose tools Claude can call to send messages back.

Your server implements two standard MCP request handlers:

#### ListToolsRequestSchema

Claude Code calls this at connection to discover what tools your server offers:

```typescript
import { ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [{
    name: 'reply',
    description: 'Send a message back over this channel',
    inputSchema: {
      type: 'object',
      properties: {
        chat_id: { type: 'string', description: 'The conversation to reply in' },
        text: { type: 'string', description: 'The message to send' },
      },
      required: ['chat_id', 'text'],
    },
  }],
}))
```

**Schema**: Standard MCP `Tool` objects. No channel-specific extensions.

#### CallToolRequestSchema

Claude calls this when it wants to invoke a tool:

```typescript
import { CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  if (req.params.name === 'reply') {
    const { chat_id, text } = req.params.arguments as { chat_id: string; text: string }
    // send the reply through your platform API
    return { content: [{ type: 'text', text: 'sent' }] }
  }
  throw new Error(`unknown tool: ${req.params.name}`)
})
```

**Request shape**:
```typescript
{
  params: {
    name: string;
    arguments: Record<string, unknown>;
  }
}
```

**Response shape**:
```typescript
{
  content: Array<{
    type: 'text' | 'image' | 'resource';  // standard MCP types
    text?: string;
    // ... other fields depend on type
  }>;
}
```

### How It Works

1. Claude reads an inbound `<channel>` event containing a `chat_id` attribute
2. Claude processes the event and calls the `reply` tool, passing the `chat_id` and response text
3. Your tool handler receives the tool call and sends the reply through your platform's API
4. Claude sees the tool result ("sent" or equivalent) and continues

There is no backward channel notification; the tool call and its result are the complete protocol.

---

## 5. Permission Relay (Optional, Two-Way Only)

### Permission Request Notification

**[DOCUMENTED]** — from `/en/channels-reference.md`, "Relay permission prompts" section

When Claude tries to call a tool that needs approval (like `Bash` or `Write`), Claude Code sends a notification to your channel server if it declared `experimental['claude/channel/permission']`:

**Method name**: `notifications/claude/channel/permission_request` (exact string)

**Params object**:

```typescript
{
  request_id: string;     // five lowercase letters, a-z excluding 'l'
  tool_name: string;      // name of the tool Claude wants to run, e.g. "Bash", "Write"
  description: string;    // human-readable summary of what this call does
  input_preview: string;  // tool arguments as JSON, truncated to ~200 characters
}
```

**Field details**:

| Field | Type | Description |
|-------|------|-------------|
| `request_id` | `string` | Five lowercase letters drawn from `a-z` but excluding `l` (so it never reads as `1` or `I` on a phone). The local terminal dialog does NOT display this ID. Your inbound handler must extract and echo it back in the verdict. This is the only way Claude Code learns which remote verdict corresponds to which request. |
| `tool_name` | `string` | Name of the tool, e.g. "Bash", "Write", "Edit". |
| `description` | `string` | Human-readable text matching the local terminal dialog. For Bash, it's Claude's description of the command or the command itself if no description was given. |
| `input_preview` | `string` | The tool's arguments as a JSON string, truncated to ~200 characters. For Bash this is the command; for Write it's the file path and content prefix. You decide what to show in your remote prompt; you can omit it if there's no room. |

### Permission Request Handling

Your server registers a notification handler (using Zod for validation):

```typescript
import { z } from 'zod'

const PermissionRequestSchema = z.object({
  method: z.literal('notifications/claude/channel/permission_request'),
  params: z.object({
    request_id: z.string(),
    tool_name: z.string(),
    description: z.string(),
    input_preview: z.string(),
  }),
})

mcp.setNotificationHandler(PermissionRequestSchema, async ({ params }) => {
  // Format and send to your remote channel (Telegram, Discord, iMessage, etc.)
  send(`Claude wants to run ${params.tool_name}: ${params.description}\n\nReply "yes ${params.request_id}" or "no ${params.request_id}"`)
})
```

### Permission Verdict Notification

Your inbound handler receives the remote response and sends back a verdict notification:

**Method name**: `notifications/claude/channel/permission` (exact string)

**Params object**:

```typescript
{
  request_id: string;    // five-letter ID echoed from the request
  behavior: 'allow' | 'deny';
}
```

**Implementation pattern** (from Telegram source code, [FROM PRIOR-ART CODE]):

```typescript
const PERMISSION_REPLY_RE = /^\s*(y|yes|n|no)\s+([a-km-z]{5})\s*$/i

async function onInbound(message: PlatformMessage) {
  if (!allowed.has(message.sender.id)) return  // gate first
  
  const m = PERMISSION_REPLY_RE.exec(message.text)
  if (m) {
    // m[1] is the verdict word (y/yes/n/no), m[2] is the request_id
    await mcp.notification({
      method: 'notifications/claude/channel/permission',
      params: {
        request_id: m[2].toLowerCase(),  // normalize in case of autocorrect caps
        behavior: m[1].toLowerCase().startsWith('y') ? 'allow' : 'deny',
      },
    })
    return  // handled as verdict, don't forward as chat
  }
  
  // not a verdict: forward as normal chat event
  await mcp.notification({
    method: 'notifications/claude/channel',
    params: { content: message.text, meta: { chat_id: String(message.chat.id) } },
  })
}
```

### Verdict Behavior

- **Allow**: Claude proceeds with the tool call
- **Deny**: Tool call is rejected, same as answering "No" in the local dialog
- **ID mismatch**: If the server emits a verdict with a request_id that doesn't match an open request, Claude Code drops it silently
- **Format mismatch**: If the inbound text doesn't match the regex pattern, it falls through as a normal chat message and never becomes a verdict
- **First answer wins**: The local terminal dialog stays open. If the user at the terminal answers before the remote verdict arrives, that answer is applied and any pending remote response is dropped (the request_id is no longer open)
- **No acknowledgment**: Sending a verdict does not receive an acknowledgment; the server has no way to know whether Claude Code accepted it or dropped it due to ID mismatch

---

## 6. Version and Feature Timeline

**[DOCUMENTED]** — from `/en/changelog.md` and `/en/channels.md`

- **v2.1.80 (May 2026)**: Channels research preview introduced; `--dangerously-load-development-channels` flag available
- **v2.1.81+**: Permission relay via `notifications/claude/channel/permission_request` added
- **Current**: Channels remain in research preview; all features documented above are current
- **Flag status**: `--dangerously-load-development-channels` still required for custom channels during research preview; approved plugins use `--channels`

### Gating

- **Organization policy**: `channelsEnabled` (default: false for Team/Enterprise, true for Console with API key; admin-controlled via managed settings)
- **Allowlist**: During research preview, channels must be on the approved allowlist or run with `--dangerously-load-development-channels`
- **Approved list**: Anthropic-maintained by default; organizations can replace with `allowedChannelPlugins` in managed settings
- **Feature changes**: The flag syntax and protocol contract may change based on feedback, per the research preview notice

---

## Summary Table

| Component | Method/Key | Exact Syntax | Source |
|-----------|-----------|--------------|--------|
| **Capability** | Channel declaration | `capabilities.experimental['claude/channel']: {}` | [DOCUMENTED] |
| **Capability** | Permission relay | `capabilities.experimental['claude/channel/permission']: {}` | [DOCUMENTED] |
| **Capability** | Tool discovery | `capabilities.tools: {}` | [DOCUMENTED] |
| **Launch** | Development flag | `--dangerously-load-development-channels` | [DOCUMENTED] |
| **Launch** | Approved flag | `--channels` | [DOCUMENTED] |
| **Launch** | Entry format | `server:<name>` or `plugin:<name>@<marketplace>` | [DOCUMENTED] |
| **Notification** | Push event | `notifications/claude/channel` | [DOCUMENTED] |
| **Notification** | Push params | `{ content: string; meta?: Record<string, string> }` | [DOCUMENTED] |
| **Notification** | Permission request | `notifications/claude/channel/permission_request` | [DOCUMENTED] |
| **Notification** | Permission verdict | `notifications/claude/channel/permission` | [DOCUMENTED] |
| **Tool** | Discovery | Standard MCP `ListToolsRequestSchema` | [DOCUMENTED] |
| **Tool** | Invocation | Standard MCP `CallToolRequestSchema` | [DOCUMENTED] |
| **ID format** | Request ID | Five letters `[a-km-z]{5}` (case-insensitive) | [FROM PRIOR-ART CODE] / [DOCUMENTED] |

---

## What Cannot Be Determined Without Live Capture

None. The schema is fully documented in official sources and confirmed by reference implementations.

---

## References

1. **Official channels documentation**: https://code.claude.com/docs/en/channels.md
2. **Channels reference (wire schema)**: https://code.claude.com/docs/en/channels-reference.md
3. **Claude Code changelog**: https://code.claude.com/docs/en/changelog.md
4. **Telegram plugin reference implementation**: https://github.com/anthropics/claude-plugins-official/tree/main/external_plugins/telegram/server.ts
5. **Discord plugin reference implementation**: https://github.com/anthropics/claude-plugins-official/tree/main/external_plugins/discord
6. **Webhook receiver example** (official docs walkthrough): https://code.claude.com/docs/en/channels-reference.md#example-build-a-webhook-receiver


# Claude Harness

A local Anthropic-compatible API server that lets [Claude Code](https://code.claude.com)
run against a custom model backend instead of Anthropic's API.

## How it works

```
Claude Code --(ANTHROPIC_BASE_URL)--> localhost:8080 --queue--> responder agent
                                                        <--response--
```

1. `server.mjs` implements `POST /v1/messages` (Anthropic format) and `GET /v1/models`.
   Incoming requests are written to `queue/<uuid>.json`; it then waits for
   `queue/<uuid>.response.json` and streams the response back as SSE.
2. A responder agent polls `queue/`, reads each request, and writes back a
   properly formatted Anthropic message object (text and/or `tool_use` blocks).
3. Configure Claude Code:
   ```sh
   export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
   export ANTHROPIC_API_KEY=any-value
   ```

## Response contract

The responder writes a plain (non-streaming) Anthropic message:

```json
{
  "id": "msg_abc123",
  "type": "message",
  "role": "assistant",
  "model": "custom",
  "stop_reason": "end_turn",
  "content": [{"type": "text", "text": "..."}],
  "usage": {"input_tokens": 0, "output_tokens": 0}
}
```

For tool calls, include
`{"type":"tool_use","id":"toolu_xyz","name":"<tool>","input":{...}}`
in `content` and set `stop_reason` to `"tool_use"`. Write the file atomically
(temp file + rename).

## Status

Experimental. Claude Code 2.x validates model IDs client-side, so a custom
model name needs to be registered via `GET /v1/models` (implemented) — still
a work in progress.

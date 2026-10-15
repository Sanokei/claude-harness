// Minimal Anthropic-compatible API server for the Claude Code harness.
// Claude Code -> POST /v1/messages -> we queue it -> a responder agent
// writes <id>.response.json -> we stream it back as SSE.
//
// The responder writes a plain (non-streaming) Anthropic message object:
// {
//   "id": "msg_...", "type": "message", "role": "assistant",
//   "model": "...", "stop_reason": "end_turn" | "tool_use" | ...,
//   "content": [ {"type":"text","text":"..."},
//                {"type":"tool_use","id":"toolu_...","name":"...","input":{...}} ],
//   "usage": {"input_tokens": 0, "output_tokens": 0}
// }
// This server converts it to the SSE event stream Claude Code expects.

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const PORT = 8080;
const QUEUE = new URL('./queue/', import.meta.url).pathname;

function sendSSE(res, message) {
  const write = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };
  const model = message.model || 'muse-spark';
  write('message_start', {
    type: 'message_start',
    message: { id: message.id, type: 'message', role: 'assistant',
      content: [], model, stop_reason: null, stop_sequence: null,
      usage: { input_tokens: 0, output_tokens: 0 } },
  });
  message.content.forEach((block, index) => {
    if (block.type === 'text') {
      write('content_block_start', { type: 'content_block_start', index,
        content_block: { type: 'text', text: '' } });
      // split into a couple of deltas to look like streaming
      const text = block.text || '';
      const mid = Math.ceil(text.length / 2);
      for (const chunk of [text.slice(0, mid), text.slice(mid)]) {
        if (chunk) write('content_block_delta', { type: 'content_block_delta',
          index, delta: { type: 'text_delta', text: chunk } });
      }
      write('content_block_stop', { type: 'content_block_stop', index });
    } else if (block.type === 'tool_use') {
      write('content_block_start', { type: 'content_block_start', index,
        content_block: { type: 'tool_use', id: block.id, name: block.name, input: {} } });
      const inputJson = JSON.stringify(block.input || {});
      write('content_block_delta', { type: 'content_block_delta', index,
        delta: { type: 'input_json_delta', partial_json: inputJson } });
      write('content_block_stop', { type: 'content_block_stop', index });
    }
  });
  write('message_delta', { type: 'message_delta',
    delta: { stop_reason: message.stop_reason || 'end_turn', stop_sequence: null },
    usage: { output_tokens: 10 } });
  write('message_stop', { type: 'message_stop' });
  res.end();
}

function waitForResponse(id, timeoutMs = 600000) {
  return new Promise((resolve, reject) => {
    const file = path.join(QUEUE, id + '.response.json');
    const start = Date.now();
    const timer = setInterval(() => {
      if (fs.existsSync(file)) {
        clearInterval(timer);
        try {
          const data = JSON.parse(fs.readFileSync(file, 'utf8'));
          fs.unlinkSync(file);
          const req = path.join(QUEUE, id + '.json');
          if (fs.existsSync(req)) fs.unlinkSync(req);
          resolve(data);
        } catch (e) { reject(e); }
      } else if (Date.now() - start > timeoutMs) {
        clearInterval(timer);
        reject(new Error('responder timeout'));
      }
    }, 1000);
  });
}

const server = http.createServer((req, res) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  if (req.method === 'GET' && req.url.startsWith('/v1/models')) {
    // Model list for client-side validation. Advertise a broad set.
    const models = ['claude-opus-5-5','claude-sonnet-5-5','claude-haiku-5-5',
      'claude-opus-4-1','claude-sonnet-4-5','muse-spark'].map(id => ({
        id, type: 'model', display_name: id, created_at: '2026-01-01T00:00:00Z',
      }));
    const parts = req.url.split('/');
    const single = parts.length === 4 ? decodeURIComponent(parts[3]).split('?')[0] : null;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    if (single) {
      const m = models.find(x => x.id === single) || { id: single, type: 'model', display_name: single };
      res.end(JSON.stringify(m));
    } else {
      res.end(JSON.stringify({ data: models, has_more: false }));
    }
    return;
  }
  if (req.method === 'POST' && req.url === '/v1/messages') {
    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', async () => {
      const id = crypto.randomUUID();
      try {
        const parsed = JSON.parse(body);
        fs.writeFileSync(path.join(QUEUE, id + '.json'), JSON.stringify(parsed));
        console.log(`[${new Date().toISOString()}] queued ${id} (model=${parsed.model}, msgs=${(parsed.messages||[]).length}, tools=${(parsed.tools||[]).length})`);
        const message = await waitForResponse(id);
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        });
        sendSSE(res, message);
        console.log(`[${new Date().toISOString()}] responded ${id} stop=${message.stop_reason}`);
      } catch (e) {
        console.error('error:', e.message);
        if (!res.headersSent) res.writeHead(500, { 'Content-Type': 'application/json' });
        try { res.end(JSON.stringify({ type: 'error', error: { message: String(e.message || e) } })); } catch {}
      }
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`harness API listening on http://127.0.0.1:${PORT}`);
});

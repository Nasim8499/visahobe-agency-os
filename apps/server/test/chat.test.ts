/**
 * Real-time chat tests.
 *
 * These run against a real HTTP listener and a real upstream model server that
 * speaks Server-Sent Events, so they verify genuine incremental streaming
 * (tokens arriving over time) and genuine failover — not a mocked promise.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';

/* The upstream must exist before the server module reads its environment. */
let upstream: Server;
let upstreamPort = 0;
/** Which model ids the fake upstream should reject. */
const failing = new Set<string>();
let upstreamHits: string[] = [];

function readBody(req: any): Promise<string> {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk: Buffer) => {
      data += chunk.toString();
    });
    req.on('end', () => resolve(data));
  });
}

async function startUpstream(): Promise<void> {
  upstream = createServer(async (req, res) => {
    const raw = await readBody(req);
    const body = raw ? JSON.parse(raw) : {};
    const model = String(body.model ?? 'unknown');
    upstreamHits.push(model);
    if (failing.has(model)) {
      res.writeHead(503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: `model ${model} is over quota` } }));
      return;
    }
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });
    const words = ['Passport ', 'validity ', 'is ', 'recorded ', 'in ', 'the ', 'case ', 'file.'];
    for (const word of words) {
      res.write(`data: ${JSON.stringify({ choices: [{ delta: { content: word } }], model })}\n\n`);
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    res.write('data: [DONE]\n\n');
    res.end();
  });
  await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  upstreamPort = (upstream.address() as AddressInfo).port;
}

await startUpstream();
process.env.AI_CLOUD_NAME = 'N Space Cloud';
process.env.AI_CLOUD_URL = `http://127.0.0.1:${upstreamPort}/v1`;
process.env.AI_CLOUD_KEY = 'test-relay-key';
process.env.AI_CLOUD_MODELS = 'gpt-4o-mini,gemini-2.5-flash,grok-4.3';

let app: any;
let baseUrl = '';
let token = '';

interface SseEvent {
  event: string;
  data: any;
  at: number;
}

/** Reads an SSE response and records when each event actually arrived. */
async function readSse(response: Response): Promise<SseEvent[]> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const events: SseEvent[] = [];
  let buffer = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const records = buffer.split('\n\n');
    buffer = records.pop() ?? '';
    for (const record of records) {
      const lines = record.split('\n');
      const event = lines.find((l) => l.startsWith('event: '))?.slice(7) ?? 'message';
      const data = lines
        .filter((l) => l.startsWith('data: '))
        .map((l) => l.slice(6))
        .join('\n');
      if (!data) continue;
      events.push({ event, data: JSON.parse(data), at: Date.now() });
    }
  }
  return events;
}

async function chat(message: string, extra: Record<string, unknown> = {}): Promise<SseEvent[]> {
  const response = await fetch(`${baseUrl}/api/chat`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ agentKey: 'raffi', message, ...extra }),
  });
  expect(response.status).toBe(200);
  expect(response.headers.get('content-type')).toContain('text/event-stream');
  return readSse(response);
}

beforeAll(async () => {
  const { getDb, runMigrations } = await import('../src/db/index.js');
  await runMigrations(getDb());
  const { seedDatabase } = await import('../src/domain/seed.js');
  await seedDatabase();
  const { buildServer } = await import('../src/index.js');
  app = await buildServer();
  await app.listen({ port: 0, host: '127.0.0.1' });
  baseUrl = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;

  const login = await app.inject({
    method: 'POST',
    url: '/api/auth/login',
    payload: { email: 'admin@test.local', password: 'TestPassword!2024' },
  });
  token = login.json().token;
});

afterAll(async () => {
  await app?.close().catch(() => undefined);
  await new Promise<void>((resolve) => upstream.close(() => resolve()));
});

describe('real-time chat streaming', () => {
  it('requires authentication', async () => {
    const response = await fetch(`${baseUrl}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ agentKey: 'raffi', message: 'hello' }),
    });
    expect(response.status).toBe(401);
  });

  it('streams tokens incrementally from the first cloud model', async () => {
    failing.clear();
    upstreamHits = [];
    const events = await chat('What is the passport validity rule?');

    const start = events.find((e) => e.event === 'start');
    expect(start?.data.conversationId).toBeTruthy();
    expect(Array.isArray(start?.data.chain)).toBe(true);

    const model = events.find((e) => e.event === 'model');
    expect(model?.data.model).toBe('gpt-4o-mini');
    expect(model?.data.provider).toBe('N Space Cloud');
    expect(model?.data.tier).toBe('cloud');

    const deltas = events.filter((e) => e.event === 'delta');
    expect(deltas.length).toBeGreaterThanOrEqual(4);
    // Genuine streaming: the first token lands well before the last one.
    expect(deltas[deltas.length - 1].at - deltas[0].at).toBeGreaterThan(50);

    const done = events.find((e) => e.event === 'done');
    expect(done?.data.content).toContain('Passport validity is recorded');
    expect(done?.data.usedLocalEngine).toBe(false);
    expect(deltas.map((d) => d.data.text).join('')).toBe(done?.data.content);
  });

  it('persists the streamed turn so it survives a reload', async () => {
    failing.clear();
    const events = await chat('Second question about the dossier.');
    const done = events.find((e) => e.event === 'done')!;
    const stored = await app.inject({
      method: 'GET',
      url: `/api/conversations/${done.data.conversationId}/messages`,
      headers: { authorization: `Bearer ${token}` },
    });
    const messages = stored.json().messages;
    expect(messages.some((m: any) => m.role === 'user' && m.content.includes('dossier'))).toBe(true);
    const assistant = messages.find((m: any) => m.id === done.data.messageId);
    expect(assistant.content).toBe(done.data.content);
    expect(assistant.meta.streamed).toBe(true);
  });

  it('fails over to the next model when the first one errors', async () => {
    failing.clear();
    failing.add('gpt-4o-mini');
    upstreamHits = [];
    const events = await chat('Which documents are missing?');

    const fallback = events.find((e) => e.event === 'fallback');
    expect(fallback?.data.from).toBe('GPT-4o mini');
    expect(String(fallback?.data.reason)).toContain('over quota');

    const model = events.find((e) => e.event === 'model');
    expect(model?.data.model).toBe('gemini-2.5-flash');

    const done = events.find((e) => e.event === 'done');
    expect(done?.data.content).toContain('Passport validity is recorded');
    expect(upstreamHits).toEqual(['gpt-4o-mini', 'gemini-2.5-flash']);
  });

  it('answers with the local engine when every model fails', async () => {
    failing.clear();
    failing.add('gpt-4o-mini');
    failing.add('gemini-2.5-flash');
    failing.add('grok-4.3');
    const events = await chat('Give me the workflow status for VM-UNKNOWN.');

    expect(events.filter((e) => e.event === 'fallback').length).toBeGreaterThanOrEqual(3);
    const model = events.filter((e) => e.event === 'model').pop();
    expect(model?.data.tier).toBe('local');

    const done = events.find((e) => e.event === 'done');
    expect(done?.data.usedLocalEngine).toBe(true);
    expect(String(done?.data.content).length).toBeGreaterThan(20);
    // The local engine still streams: several deltas, not one blob.
    expect(events.filter((e) => e.event === 'delta').length).toBeGreaterThan(1);
    failing.clear();
  });

  it('reports the model chain and what is usable', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/api/chat/models',
      headers: { authorization: `Bearer ${token}` },
    });
    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.cloudName).toBe('N Space Cloud');
    expect(payload.relayConfigured).toBe(true);
    expect(payload.live).toBe(true);
    const ids = payload.candidates.map((c: any) => c.id);
    expect(ids).toContain('cloud:gpt-4o-mini');
    expect(ids).toContain('visahobe-local-engine');
    const local = payload.candidates.find((c: any) => c.id === 'visahobe-local-engine');
    expect(local.available).toBe(true);
  });

  it('probes every candidate with a real request', async () => {
    failing.clear();
    failing.add('grok-4.3');
    const response = await app.inject({
      method: 'POST',
      url: '/api/chat/probe',
      headers: { authorization: `Bearer ${token}` },
      payload: {},
    });
    expect(response.statusCode).toBe(200);
    const results = response.json().results as any[];
    const openai = results.find((r) => r.id === 'cloud:gpt-4o-mini');
    expect(openai.ok).toBe(true);
    expect(openai.firstTokenMs).toBeGreaterThanOrEqual(0);
    const grok = results.find((r) => r.id === 'cloud:grok-4.3');
    expect(grok.ok).toBe(false);
    expect(String(grok.message)).toContain('quota');
    failing.clear();
  });
});

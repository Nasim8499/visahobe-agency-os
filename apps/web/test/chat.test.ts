// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmoothTyper, parseSseRecords, streamChat } from '../src/lib/chatStream';
import {
  clearStoredConversations,
  conversationTitle,
  deleteStoredConversation,
  listStoredConversations,
  renameStoredConversation,
  saveStoredConversation,
} from '../src/lib/conversationStore';

function sseStream(records: string[], delayMs = 0): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const record of records) {
        controller.enqueue(encoder.encode(record));
        if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
      controller.close();
    },
  });
  return new Response(body, { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

const event = (name: string, data: unknown) => `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`;

afterEach(() => {
  vi.unstubAllGlobals();
  clearStoredConversations();
});

describe('SSE parsing', () => {
  it('returns complete records and keeps the partial tail', () => {
    const { events, rest } = parseSseRecords('event: delta\ndata: {"text":"hi"}\n\nevent: delta\ndata: {"te');
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ event: 'delta', data: '{"text":"hi"}' });
    expect(rest).toBe('event: delta\ndata: {"te');
  });

  it('ignores keep-alive comments and handles CRLF', () => {
    const { events } = parseSseRecords(': keep-alive\r\n\r\nevent: done\r\ndata: {"ok":true}\r\n\r\n');
    expect(events).toHaveLength(1);
    expect(events[0].event).toBe('done');
  });
});

describe('streamChat', () => {
  it('dispatches start, model, delta and done in order', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        sseStream([
          event('start', { conversationId: 'cnv_1', taskId: 'tsk_1', caseId: null, chain: [] }),
          event('model', { id: 'cloud:gpt-4o-mini', label: 'GPT-4o mini', provider: 'N Space Cloud', model: 'gpt-4o-mini', tier: 'cloud' }),
          event('delta', { text: 'Hello ' }),
          event('delta', { text: 'world' }),
          event('done', { conversationId: 'cnv_1', content: 'Hello world', usedLocalEngine: false }),
        ]),
      ),
    );

    const seen: string[] = [];
    let text = '';
    await streamChat(
      { agentKey: 'raffi', message: 'hi' },
      {
        onStart: () => seen.push('start'),
        onModel: () => seen.push('model'),
        onDelta: (chunk) => {
          seen.push('delta');
          text += chunk;
        },
        onDone: () => seen.push('done'),
        onError: () => seen.push('error'),
      },
    );

    expect(seen).toEqual(['start', 'model', 'delta', 'delta', 'done']);
    expect(text).toBe('Hello world');
  });

  it('reports a fallback when a model is swapped out', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        sseStream([
          event('start', { conversationId: 'cnv_2', taskId: 't', caseId: null, chain: [] }),
          event('fallback', { from: 'GPT-4o mini', reason: 'over quota' }),
          event('model', { id: 'cloud:gemini-2.5-flash', label: 'Gemini 2.5 Flash', provider: 'N Space Cloud', model: 'gemini-2.5-flash', tier: 'cloud' }),
          event('delta', { text: 'ok' }),
          event('done', { conversationId: 'cnv_2', content: 'ok', usedLocalEngine: false }),
        ]),
      ),
    );

    const fallbacks: string[] = [];
    let used = '';
    await streamChat(
      { agentKey: 'raffi', message: 'hi' },
      {
        onFallback: (data) => fallbacks.push(`${data.from}: ${data.reason}`),
        onModel: (model) => {
          used = model.model;
        },
      },
    );
    expect(fallbacks).toEqual(['GPT-4o mini: over quota']);
    expect(used).toBe('gemini-2.5-flash');
  });

  it('surfaces a server error payload instead of hanging', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(JSON.stringify({ error: { message: 'Session expired or invalid' } }), {
          status: 401,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );
    let message = '';
    await streamChat({ agentKey: 'raffi', message: 'hi' }, { onError: (value) => (message = value) });
    expect(message).toBe('Session expired or invalid');
  });
});

describe('SmoothTyper', () => {
  it('emits every character it was given', async () => {
    let out = '';
    const typer = new SmoothTyper((chunk) => {
      out += chunk;
    });
    typer.push('The quick brown fox jumps over the lazy dog.');
    typer.push(' And then it streams some more text.');
    await new Promise((resolve) => setTimeout(resolve, 400));
    typer.flush();
    expect(out).toBe('The quick brown fox jumps over the lazy dog. And then it streams some more text.');
  });

  it('releases the backlog immediately on flush', () => {
    let out = '';
    const typer = new SmoothTyper((chunk) => {
      out += chunk;
    });
    typer.push('abcdefghij');
    typer.flush();
    expect(out).toBe('abcdefghij');
  });
});

describe('on-device conversation history', () => {
  it('stores, lists and deletes conversations per agent', () => {
    saveStoredConversation({
      id: 'cnv_1',
      agentKey: 'raffi',
      title: 'Passport rule',
      caseId: null,
      updatedAt: '2026-01-01T00:00:00.000Z',
      messages: [{ id: 'm1', role: 'user', content: 'hi', createdAt: '2026-01-01T00:00:00.000Z' }],
    });
    saveStoredConversation({
      id: 'cnv_2',
      agentKey: 'nova',
      title: 'Documents',
      caseId: null,
      updatedAt: '2026-01-02T00:00:00.000Z',
      messages: [{ id: 'm2', role: 'user', content: 'docs', createdAt: '2026-01-02T00:00:00.000Z' }],
    });

    expect(listStoredConversations('raffi')).toHaveLength(1);
    expect(listStoredConversations()).toHaveLength(2);
    // Newest first.
    expect(listStoredConversations()[0].id).toBe('cnv_2');

    deleteStoredConversation('cnv_2');
    expect(listStoredConversations('nova')).toHaveLength(0);
  });

  it('re-keys a local conversation once the server assigns an id', () => {
    saveStoredConversation({
      id: 'local-1',
      agentKey: 'raffi',
      title: 'Draft',
      caseId: null,
      updatedAt: '2026-01-03T00:00:00.000Z',
      messages: [{ id: 'm1', role: 'user', content: 'hi', createdAt: '2026-01-03T00:00:00.000Z' }],
    });
    renameStoredConversation('local-1', 'cnv_server');
    const all = listStoredConversations('raffi');
    expect(all).toHaveLength(1);
    expect(all[0].id).toBe('cnv_server');
    expect(all[0].messages[0].content).toBe('hi');
  });

  it('builds a readable title from the first message', () => {
    expect(conversationTitle('  What documents does   Serbia need? ')).toBe('What documents does Serbia need?');
    expect(conversationTitle('x'.repeat(80))).toHaveLength(53);
  });
});

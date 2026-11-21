/**
 * Real-time (token by token) model streaming.
 *
 * Every adapter here opens a single HTTP request to the upstream model and
 * yields text fragments as they arrive on the wire — nothing is buffered until
 * the end and nothing is simulated. If an upstream fails it throws a
 * ProviderError; the caller (see chain.ts) then moves on to the next candidate.
 */
import { ProviderError, type ChatMessage } from './types.js';

export interface StreamRequest {
  system?: string;
  messages: ChatMessage[];
  temperature?: number;
  maxTokens?: number;
}

export interface StreamOptions {
  signal?: AbortSignal;
  /** Time allowed for the upstream to send its response headers. */
  connectTimeoutMs?: number;
  /** Time allowed between two chunks once streaming has started. */
  idleTimeoutMs?: number;
}

export type TextStream = AsyncGenerator<string, void, unknown>;

const DEFAULT_CONNECT_TIMEOUT = 20_000;
const DEFAULT_IDLE_TIMEOUT = 45_000;

function linkSignals(signal: AbortSignal | undefined, controller: AbortController): () => void {
  if (!signal) return () => undefined;
  if (signal.aborted) controller.abort();
  const onAbort = () => controller.abort();
  signal.addEventListener('abort', onAbort);
  return () => signal.removeEventListener('abort', onAbort);
}

async function openStream(
  url: string,
  init: RequestInit,
  options: StreamOptions,
): Promise<{ response: Response; controller: AbortController; release: () => void }> {
  const controller = new AbortController();
  const unlink = linkSignals(options.signal, controller);
  const connectTimer = setTimeout(
    () => controller.abort(),
    options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT,
  );
  let response: Response;
  try {
    response = await fetch(url, { ...init, signal: controller.signal });
  } catch (err) {
    clearTimeout(connectTimer);
    unlink();
    if (options.signal?.aborted) throw new ProviderError('Cancelled by the client');
    if ((err as Error)?.name === 'AbortError') throw new ProviderError('Upstream did not respond in time');
    throw new ProviderError(`Upstream unreachable: ${(err as Error).message}`);
  }
  clearTimeout(connectTimer);
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    unlink();
    let detail = body.slice(0, 400);
    try {
      const parsed = JSON.parse(body);
      detail = parsed?.error?.message ?? parsed?.message ?? detail;
    } catch {
      /* plain text body */
    }
    throw new ProviderError(detail || `HTTP ${response.status}`, response.status, body);
  }
  if (!response.body) {
    unlink();
    throw new ProviderError('Upstream returned an empty stream');
  }
  return { response, controller, release: unlink };
}

/**
 * Iterate the `data:` payloads of a Server-Sent Events response as they land,
 * enforcing an idle timeout so a silently stalled upstream cannot hang a chat.
 */
async function* sseEvents(
  response: Response,
  controller: AbortController,
  options: StreamOptions,
): AsyncGenerator<string, void, unknown> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const idleMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT;
  let buffer = '';
  try {
    for (;;) {
      let timer: NodeJS.Timeout | undefined;
      const idle = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new ProviderError('Upstream stream stalled'));
        }, idleMs);
      });
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await Promise.race([reader.read(), idle]);
      } finally {
        if (timer) clearTimeout(timer);
      }
      if (result.done) break;
      buffer += decoder.decode(result.value, { stream: true });
      // SSE records are separated by a blank line; tolerate \r\n endings.
      const records = buffer.split(/\r?\n\r?\n/);
      buffer = records.pop() ?? '';
      for (const record of records) {
        const data = record
          .split(/\r?\n/)
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trim())
          .join('\n');
        if (data) yield data;
      }
    }
    const tail = buffer.trim();
    if (tail.startsWith('data:')) yield tail.slice(5).trim();
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

function safeJson(raw: string): any {
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
}

/* ----------------------------- OpenAI style ----------------------------- */

export interface OpenAiStreamConfig {
  baseUrl: string;
  model: string;
  apiKey?: string;
  headers?: Record<string, string>;
}

export async function* streamOpenAiCompatible(
  config: OpenAiStreamConfig,
  request: StreamRequest,
  options: StreamOptions = {},
): TextStream {
  const body: Record<string, unknown> = {
    model: config.model,
    stream: true,
    temperature: request.temperature ?? 0.3,
    messages: [
      ...(request.system ? [{ role: 'system', content: request.system }] : []),
      ...request.messages.map((m) => ({
        role: m.role === 'tool' ? 'assistant' : m.role,
        content: m.content,
      })),
    ],
  };
  if (request.maxTokens) body.max_tokens = request.maxTokens;

  const { response, controller, release } = await openStream(
    `${config.baseUrl.replace(/\/$/, '')}/chat/completions`,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        accept: 'text/event-stream',
        ...(config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {}),
        ...(config.headers ?? {}),
      },
      body: JSON.stringify(body),
    },
    options,
  );
  try {
    for await (const data of sseEvents(response, controller, options)) {
      if (data === '[DONE]') return;
      const parsed = safeJson(data);
      if (!parsed) continue;
      if (parsed.error) throw new ProviderError(parsed.error.message ?? 'Upstream error');
      const delta = parsed.choices?.[0]?.delta;
      const text: unknown = delta?.content ?? parsed.choices?.[0]?.text;
      if (typeof text === 'string' && text) yield text;
      // Some gateways send the full message in the final chunk instead.
      else if (Array.isArray(delta?.content)) {
        for (const part of delta.content) {
          if (typeof part?.text === 'string' && part.text) yield part.text;
        }
      }
    }
  } finally {
    release();
  }
}

/* -------------------------------- Gemini -------------------------------- */

export interface GeminiStreamConfig {
  baseUrl?: string;
  model: string;
  apiKey: string;
}

export async function* streamGemini(
  config: GeminiStreamConfig,
  request: StreamRequest,
  options: StreamOptions = {},
): TextStream {
  const base = (config.baseUrl ?? 'https://generativelanguage.googleapis.com/v1beta').replace(/\/$/, '');
  const body: Record<string, unknown> = {
    contents: request.messages
      .filter((m) => m.role !== 'system')
      .map((m) => ({
        role: m.role === 'assistant' ? 'model' : 'user',
        parts: [{ text: m.content }],
      })),
    generationConfig: {
      temperature: request.temperature ?? 0.3,
      maxOutputTokens: request.maxTokens ?? 2048,
    },
  };
  if (request.system) body.systemInstruction = { parts: [{ text: request.system }] };

  const url = `${base}/models/${encodeURIComponent(config.model)}:streamGenerateContent?alt=sse&key=${encodeURIComponent(
    config.apiKey,
  )}`;
  const { response, controller, release } = await openStream(
    url,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify(body),
    },
    options,
  );
  try {
    for await (const data of sseEvents(response, controller, options)) {
      if (data === '[DONE]') return;
      const parsed = safeJson(data);
      if (!parsed) continue;
      if (parsed.error) throw new ProviderError(parsed.error.message ?? 'Upstream error');
      const parts = parsed.candidates?.[0]?.content?.parts ?? [];
      for (const part of parts) {
        if (typeof part?.text === 'string' && part.text) yield part.text;
      }
    }
  } finally {
    release();
  }
}

/* ------------------------------- Anthropic ------------------------------- */

export interface AnthropicStreamConfig {
  baseUrl?: string;
  model: string;
  apiKey: string;
}

export async function* streamAnthropic(
  config: AnthropicStreamConfig,
  request: StreamRequest,
  options: StreamOptions = {},
): TextStream {
  const base = (config.baseUrl ?? 'https://api.anthropic.com').replace(/\/$/, '');
  const body: Record<string, unknown> = {
    model: config.model,
    stream: true,
    max_tokens: request.maxTokens ?? 2048,
    temperature: request.temperature ?? 0.3,
    messages: request.messages
      .filter((m) => m.role !== 'system')
      .map((m) => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
  };
  if (request.system) body.system = request.system;

  const { response, controller, release } = await openStream(
    `${base}/v1/messages`,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        accept: 'text/event-stream',
        'x-api-key': config.apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify(body),
    },
    options,
  );
  try {
    for await (const data of sseEvents(response, controller, options)) {
      const parsed = safeJson(data);
      if (!parsed) continue;
      if (parsed.type === 'error') throw new ProviderError(parsed.error?.message ?? 'Upstream error');
      if (parsed.type === 'content_block_delta' && typeof parsed.delta?.text === 'string') {
        if (parsed.delta.text) yield parsed.delta.text;
      }
      if (parsed.type === 'message_stop') return;
    }
  } finally {
    release();
  }
}

/* ----------------------------- Local engine ------------------------------ */

/**
 * Stream a locally composed answer. The text comes from the deterministic
 * engine (agency records only); it is emitted in small pieces so the operator
 * sees the same live cadence as a hosted model instead of a frozen spinner.
 */
export async function* streamLocalText(
  text: string,
  options: StreamOptions & { chunkChars?: number; delayMs?: number } = {},
): TextStream {
  const size = options.chunkChars ?? 18;
  const delay = options.delayMs ?? 12;
  // Split on word boundaries so the typewriter never cuts a word in half.
  const words = text.split(/(\s+)/);
  let buffer = '';
  for (const word of words) {
    if (options.signal?.aborted) return;
    buffer += word;
    if (buffer.length >= size) {
      yield buffer;
      buffer = '';
      if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  if (buffer && !options.signal?.aborted) yield buffer;
}

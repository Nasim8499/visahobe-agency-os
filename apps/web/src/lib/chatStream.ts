/**
 * Client for the real-time chat endpoint.
 *
 * `POST /api/chat` answers with Server-Sent Events. We read the body as a
 * stream and forward every token the moment it arrives. A small pacing queue
 * turns the bursty network cadence into a steady typewriter so the answer
 * reads smoothly instead of jumping in blocks.
 */

export interface ChatChainEntry {
  id: string;
  label: string;
  tier: 'cloud' | 'byok' | 'local';
  available: boolean;
}

export interface ChatModelInfo {
  id: string;
  label: string;
  provider: string;
  model: string;
  tier: 'cloud' | 'byok' | 'local';
}

export interface ChatDonePayload {
  conversationId: string;
  taskId: string;
  messageId: string;
  content: string;
  provider: string;
  label: string;
  model: string;
  tier: 'cloud' | 'byok' | 'local';
  usedLocalEngine: boolean;
  cancelled: boolean;
  attempts: Array<{ id: string; label: string; ok: boolean; message?: string }>;
}

export interface ChatStreamHandlers {
  onStart?: (data: { conversationId: string; taskId: string; caseId: string | null; chain: ChatChainEntry[] }) => void;
  onModel?: (model: ChatModelInfo) => void;
  onDelta?: (text: string) => void;
  onFallback?: (data: { from: string; reason: string }) => void;
  onNotice?: (data: { message: string }) => void;
  onDone?: (data: ChatDonePayload) => void;
  onError?: (message: string) => void;
}

export interface ChatStreamInput {
  agentKey: string;
  message: string;
  conversationId?: string;
  caseId?: string;
}

/**
 * Paces incoming text so the UI types it out smoothly. Nothing is invented:
 * every character comes from the model, only the cadence is evened out.
 */
export class SmoothTyper {
  private pending = '';
  private running = false;
  private finished = false;
  private frame: number | null = null;

  constructor(
    private readonly emit: (text: string) => void,
    private readonly onIdle?: () => void,
  ) {}

  push(text: string): void {
    if (!text) return;
    this.pending += text;
    this.start();
  }

  /** Stop pacing and release everything that is still queued. */
  flush(): void {
    this.cancelFrame();
    this.running = false;
    if (this.pending) {
      const rest = this.pending;
      this.pending = '';
      this.emit(rest);
    }
    this.onIdle?.();
  }

  finish(): void {
    this.finished = true;
    if (!this.pending) {
      this.cancelFrame();
      this.running = false;
      this.onIdle?.();
    }
  }

  dispose(): void {
    this.cancelFrame();
    this.pending = '';
    this.running = false;
  }

  private cancelFrame() {
    if (this.frame === null) return;
    if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(this.frame);
    else clearTimeout(this.frame as unknown as ReturnType<typeof setTimeout>);
    this.frame = null;
  }

  private schedule(step: () => void) {
    if (typeof requestAnimationFrame === 'function') this.frame = requestAnimationFrame(step);
    else this.frame = setTimeout(step, 16) as unknown as number;
  }

  private start() {
    if (this.running) return;
    this.running = true;
    const step = () => {
      this.frame = null;
      if (!this.pending) {
        this.running = false;
        if (this.finished) this.onIdle?.();
        return;
      }
      // Drain proportionally: long backlogs catch up, short ones stay gentle.
      const take = Math.max(2, Math.ceil(this.pending.length / 6));
      const slice = this.pending.slice(0, take);
      this.pending = this.pending.slice(take);
      this.emit(slice);
      this.schedule(step);
    };
    this.schedule(step);
  }
}

/** Split a raw SSE buffer into complete records. Exported for tests. */
export function parseSseRecords(buffer: string): { events: Array<{ event: string; data: string }>; rest: string } {
  const parts = buffer.split(/\r?\n\r?\n/);
  const rest = parts.pop() ?? '';
  const events: Array<{ event: string; data: string }> = [];
  for (const part of parts) {
    const lines = part.split(/\r?\n/);
    let event = 'message';
    const data: string[] = [];
    for (const line of lines) {
      if (line.startsWith(':')) continue; // keep-alive comment
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
    }
    if (data.length) events.push({ event, data: data.join('\n') });
  }
  return { events, rest };
}

export async function streamChat(
  input: ChatStreamInput,
  handlers: ChatStreamHandlers,
  signal?: AbortSignal,
): Promise<void> {
  let response: Response;
  try {
    response = await fetch('/api/chat', {
      method: 'POST',
      credentials: 'include',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify(input),
      signal,
    });
  } catch (error) {
    if ((error as Error).name === 'AbortError') return;
    handlers.onError?.('Could not reach the server. Check your connection and try again.');
    return;
  }

  if (!response.ok || !response.body) {
    let message = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      message = payload?.error?.message ?? message;
    } catch {
      /* non JSON error body */
    }
    handlers.onError?.(message);
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const { events, rest } = parseSseRecords(buffer);
      buffer = rest;
      for (const item of events) {
        let payload: any;
        try {
          payload = JSON.parse(item.data);
        } catch {
          continue;
        }
        switch (item.event) {
          case 'start':
            handlers.onStart?.(payload);
            break;
          case 'model':
            handlers.onModel?.(payload);
            break;
          case 'delta':
            handlers.onDelta?.(payload.text ?? '');
            break;
          case 'fallback':
            handlers.onFallback?.(payload);
            break;
          case 'notice':
            handlers.onNotice?.(payload);
            break;
          case 'done':
            handlers.onDone?.(payload);
            break;
          case 'error':
            handlers.onError?.(payload.message ?? 'The model stream failed.');
            break;
          default:
            break;
        }
      }
    }
  } catch (error) {
    if ((error as Error).name !== 'AbortError') {
      handlers.onError?.((error as Error).message);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

/**
 * Live status of the AI model chain.
 *
 * One shared store so the header badge, the chat screen and the Connect screen
 * all show the same truth without each firing its own request.
 */
import { useSyncExternalStore } from 'react';
import { api } from '../api';

export interface ChainCandidate {
  id: string;
  label: string;
  providerName: string;
  model: string;
  vendor: string;
  tier: 'cloud' | 'byok' | 'local';
  available: boolean;
  note?: string;
}

export interface ChatStatusState {
  loading: boolean;
  error: string | null;
  live: boolean;
  cloudName: string;
  relayConfigured: boolean;
  candidates: ChainCandidate[];
  checkedAt: string | null;
}

const EMPTY: ChatStatusState = {
  loading: true,
  error: null,
  live: false,
  cloudName: 'N Space Cloud',
  relayConfigured: false,
  candidates: [],
  checkedAt: null,
};

let state: ChatStatusState = EMPTY;
const listeners = new Set<() => void>();
let inFlight: Promise<void> | null = null;
let timer: ReturnType<typeof setInterval> | null = null;

function set(next: Partial<ChatStatusState>) {
  state = { ...state, ...next };
  for (const listener of listeners) listener();
}

export async function refreshChatStatus(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const payload = await api.get<{
        live: boolean;
        cloudName: string;
        relayConfigured: boolean;
        candidates: ChainCandidate[];
      }>('/api/chat/models');
      set({
        loading: false,
        error: null,
        live: Boolean(payload.live),
        cloudName: payload.cloudName ?? 'N Space Cloud',
        relayConfigured: Boolean(payload.relayConfigured),
        candidates: payload.candidates ?? [],
        checkedAt: new Date().toISOString(),
      });
    } catch (error) {
      set({ loading: false, error: (error as Error).message, live: false, checkedAt: new Date().toISOString() });
    } finally {
      inFlight = null;
    }
  })();
  return inFlight;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  if (listeners.size === 1) {
    void refreshChatStatus();
    if (typeof window !== 'undefined') {
      timer = setInterval(() => void refreshChatStatus(), 90_000);
      window.addEventListener('focus', refreshChatStatus);
      window.addEventListener('online', refreshChatStatus);
    }
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      if (timer) clearInterval(timer);
      timer = null;
      if (typeof window !== 'undefined') {
        window.removeEventListener('focus', refreshChatStatus);
        window.removeEventListener('online', refreshChatStatus);
      }
    }
  };
}

const getSnapshot = () => state;

export function useChatStatus(): ChatStatusState {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/** Label for the header badge: honest about which engine is actually serving. */
export function statusLabel(current: ChatStatusState): { text: string; tone: 'green' | 'amber' | 'red' } {
  if (current.loading) return { text: 'CHECKING', tone: 'amber' };
  if (current.error) return { text: 'OFFLINE', tone: 'red' };
  if (current.live) return { text: 'LIVE', tone: 'green' };
  return { text: 'LOCAL', tone: 'amber' };
}

/** Test seam: reset the shared store. */
export function __resetChatStatus(): void {
  state = EMPTY;
  listeners.clear();
  if (timer) clearInterval(timer);
  timer = null;
}

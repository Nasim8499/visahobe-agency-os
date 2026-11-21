/**
 * On-device conversation history.
 *
 * Chats are stored on the server too, but keeping a copy in localStorage means
 * the thread is still there instantly after a reload, after going offline, or
 * when the PWA is reopened from the home screen.
 */

export interface StoredMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  createdAt: string;
  provider?: string;
  model?: string;
  tier?: 'cloud' | 'byok' | 'local';
  cancelled?: boolean;
}

export interface StoredConversation {
  id: string;
  agentKey: string;
  title: string;
  caseId?: string | null;
  updatedAt: string;
  messages: StoredMessage[];
}

const KEY = 'visahobe.chat.v1';
const MAX_CONVERSATIONS = 24;
const MAX_MESSAGES = 120;

function readAll(): StoredConversation[] {
  if (typeof localStorage === 'undefined') return [];
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as StoredConversation[]) : [];
  } catch {
    return [];
  }
}

function writeAll(items: StoredConversation[]): void {
  if (typeof localStorage === 'undefined') return;
  try {
    const trimmed = items
      .slice()
      .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1))
      .slice(0, MAX_CONVERSATIONS)
      .map((item) => ({ ...item, messages: item.messages.slice(-MAX_MESSAGES) }));
    localStorage.setItem(KEY, JSON.stringify(trimmed));
  } catch {
    /* storage can be full or blocked in private mode — history is optional */
  }
}

export function listStoredConversations(agentKey?: string): StoredConversation[] {
  const all = readAll();
  const filtered = agentKey ? all.filter((item) => item.agentKey === agentKey) : all;
  return filtered.sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function getStoredConversation(id: string): StoredConversation | undefined {
  return readAll().find((item) => item.id === id);
}

export function saveStoredConversation(conversation: StoredConversation): void {
  const all = readAll().filter((item) => item.id !== conversation.id);
  all.push(conversation);
  writeAll(all);
}

/** The server assigns the real id on the first turn; re-key the local copy. */
export function renameStoredConversation(fromId: string, toId: string): void {
  if (fromId === toId) return;
  const all = readAll();
  const source = all.find((item) => item.id === fromId);
  if (!source) return;
  const rest = all.filter((item) => item.id !== fromId && item.id !== toId);
  rest.push({ ...source, id: toId });
  writeAll(rest);
}

export function deleteStoredConversation(id: string): void {
  writeAll(readAll().filter((item) => item.id !== id));
}

export function clearStoredConversations(agentKey?: string): void {
  if (!agentKey) {
    writeAll([]);
    return;
  }
  writeAll(readAll().filter((item) => item.agentKey !== agentKey));
}

export function conversationTitle(message: string): string {
  const clean = message.replace(/\s+/g, ' ').trim();
  return clean.length > 52 ? `${clean.slice(0, 52)}…` : clean || 'New chat';
}

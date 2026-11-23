import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearch } from '@tanstack/react-router';
import { AGENT_PROFILES, type AgentKey } from '@visahobe/shared';
import { api } from '../api';
import { useApi } from '../lib/useAsync';
import { Avatar, Card, EmptyState, Pill, useToast } from '../components/ui';
import { RefreshIcon, SendIcon, StopIcon } from '../components/icons';
import { renderMarkdown, relativeTime } from '../lib/format';
import { SmoothTyper, streamChat, type ChatModelInfo } from '../lib/chatStream';
import {
  clearStoredConversations,
  conversationTitle,
  deleteStoredConversation,
  listStoredConversations,
  renameStoredConversation,
  saveStoredConversation,
  type StoredConversation,
  type StoredMessage,
} from '../lib/conversationStore';
import { refreshChatStatus, statusLabel, useChatStatus } from '../lib/chatStatus';

interface CaseDto {
  id: string;
  reference: string;
  country: string;
  visaCategory: string;
  applicantName: string;
}

const newLocalId = () => `local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

export function ChatRoute() {
  const search = useSearch({ from: '/chat' }) as { agent?: string };
  const toast = useToast();
  const status = useChatStatus();

  const [agentKey, setAgentKey] = useState<AgentKey>((search.agent as AgentKey) ?? 'raffi');
  const [conversationId, setConversationId] = useState<string>(() => newLocalId());
  const [messages, setMessages] = useState<StoredMessage[]>([]);
  const [history, setHistory] = useState<StoredConversation[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [draft, setDraft] = useState('');
  const [caseId, setCaseId] = useState<string>('');
  const [model, setModel] = useState<ChatModelInfo | null>(null);
  const [notices, setNotices] = useState<string[]>([]);

  const abortRef = useRef<AbortController | null>(null);
  const modelRef = useRef<ChatModelInfo | null>(null);
  // Read through a function: the ref is only written from stream callbacks,
  // which TypeScript's flow analysis cannot see, so a direct read would be
  // narrowed to the last statically visible assignment.
  const activeModel = (): ChatModelInfo | null => modelRef.current;
  const typerRef = useRef<SmoothTyper | null>(null);
  const draftRef = useRef('');
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const pinnedRef = useRef(true);

  const cases = useApi<{ items: CaseDto[] }>('/api/cases?pageSize=25');
  const profile = AGENT_PROFILES[agentKey];
  const badge = statusLabel(status);

  /* ------------------------- on-device history -------------------------- */

  const reloadHistory = useCallback((key: AgentKey) => {
    setHistory(listStoredConversations(key));
  }, []);

  useEffect(() => {
    reloadHistory(agentKey);
    const stored = listStoredConversations(agentKey);
    const latest = stored[0];
    if (latest) {
      setConversationId(latest.id);
      setMessages(latest.messages);
      setCaseId(latest.caseId ?? '');
    } else {
      setConversationId(newLocalId());
      setMessages([]);
    }
    setModel(null);
    setNotices([]);
    setDraft('');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [agentKey]);

  const persist = useCallback(
    (id: string, list: StoredMessage[], linkedCase: string) => {
      if (!list.length) return;
      saveStoredConversation({
        id,
        agentKey,
        title: conversationTitle(list.find((m) => m.role === 'user')?.content ?? 'New chat'),
        caseId: linkedCase || null,
        updatedAt: new Date().toISOString(),
        messages: list,
      });
      reloadHistory(agentKey);
    },
    [agentKey, reloadHistory],
  );

  /* ---------------------------- auto scroll ----------------------------- */

  const onScroll = () => {
    const node = scrollRef.current;
    if (!node) return;
    pinnedRef.current = node.scrollHeight - node.scrollTop - node.clientHeight < 120;
  };

  useEffect(() => {
    if (pinnedRef.current) bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [messages.length, draft]);

  useEffect(() => () => {
    abortRef.current?.abort();
    typerRef.current?.dispose();
  }, []);

  /* ------------------------------- send --------------------------------- */

  const send = async () => {
    const text = input.trim();
    if (!text || streaming) return;

    const userMessage: StoredMessage = {
      id: newLocalId(),
      role: 'user',
      content: text,
      createdAt: new Date().toISOString(),
    };
    const base = [...messages, userMessage];
    setMessages(base);
    setInput('');
    setDraft('');
    setNotices([]);
    setModel(null);
    modelRef.current = null;
    setStreaming(true);
    draftRef.current = '';
    pinnedRef.current = true;

    const controller = new AbortController();
    abortRef.current = controller;
    const typer = new SmoothTyper((chunk) => {
      draftRef.current += chunk;
      setDraft(draftRef.current);
    });
    typerRef.current = typer;

    let activeId = conversationId;
    let finished = false;

    await streamChat(
      {
        agentKey,
        message: text,
        conversationId: activeId.startsWith('local-') ? undefined : activeId,
        caseId: caseId || undefined,
      },
      {
        onStart: (data) => {
          if (data.conversationId && data.conversationId !== activeId) {
            renameStoredConversation(activeId, data.conversationId);
            activeId = data.conversationId;
            setConversationId(data.conversationId);
          }
        },
        onModel: (info) => {
          modelRef.current = info;
          setModel(info);
        },
        onDelta: (chunk) => typer.push(chunk),
        onFallback: (data) =>
          setNotices((current) => [...current, `${data.from} unavailable — switching model. (${data.reason})`]),
        onNotice: (data) => setNotices((current) => [...current, data.message]),
        onDone: (data) => {
          finished = true;
          typer.flush();
          const assistant: StoredMessage = {
            id: data.messageId || newLocalId(),
            role: 'assistant',
            content: data.content || draftRef.current,
            createdAt: new Date().toISOString(),
            provider: data.provider,
            model: data.model,
            tier: data.tier,
            cancelled: data.cancelled,
          };
          const next = [...base, assistant];
          setMessages(next);
          setDraft('');
          draftRef.current = '';
          persist(activeId, next, caseId);
          if (data.usedLocalEngine) void refreshChatStatus();
        },
        onError: (message) => {
          finished = true;
          typer.flush();
          toast.push(message, 'error');
        },
      },
      controller.signal,
    );

    typer.finish();
    if (!finished) {
      // Stream stopped early (operator pressed Stop, or the tab lost network):
      // keep whatever text already arrived instead of dropping the turn.
      const partial = draftRef.current.trim();
      const streamed = activeModel();
      if (partial) {
        const assistant: StoredMessage = {
          id: newLocalId(),
          role: 'assistant',
          content: partial,
          createdAt: new Date().toISOString(),
          provider: streamed?.provider,
          model: streamed?.model,
          tier: streamed?.tier,
          cancelled: true,
        };
        const next = [...base, assistant];
        setMessages(next);
        persist(activeId, next, caseId);
      } else {
        persist(activeId, base, caseId);
      }
      setDraft('');
      draftRef.current = '';
    }
    typerRef.current = null;
    abortRef.current = null;
    setStreaming(false);
  };

  const stop = () => {
    abortRef.current?.abort();
  };

  const newChat = () => {
    abortRef.current?.abort();
    setConversationId(newLocalId());
    setMessages([]);
    setDraft('');
    setModel(null);
    setNotices([]);
  };

  const openStored = (conversation: StoredConversation) => {
    abortRef.current?.abort();
    setConversationId(conversation.id);
    setMessages(conversation.messages);
    setCaseId(conversation.caseId ?? '');
    setDraft('');
    setModel(null);
    setNotices([]);
  };

  const removeStored = (id: string) => {
    deleteStoredConversation(id);
    reloadHistory(agentKey);
    if (id === conversationId) newChat();
  };

  const loadServerCopy = async () => {
    if (conversationId.startsWith('local-')) {
      toast.push('This chat has not been sent to the server yet.');
      return;
    }
    try {
      const { messages: list } = await api.get<{ messages: any[] }>(
        `/api/conversations/${conversationId}/messages`,
      );
      const mapped: StoredMessage[] = list
        .filter((m) => m.role === 'user' || m.role === 'assistant')
        .map((m) => ({
          id: m.id,
          role: m.role,
          content: m.content,
          createdAt: m.createdAt,
          provider: m.meta?.provider,
          model: m.meta?.model,
          tier: m.meta?.tier,
        }));
      setMessages(mapped);
      persist(conversationId, mapped, caseId);
      toast.push('Loaded the server copy of this conversation.');
    } catch (error) {
      toast.push((error as Error).message, 'error');
    }
  };

  const caseOptions = useMemo(() => cases.data?.items ?? [], [cases.data]);

  return (
    <div className="chat-page">
      <header className="page-head">
        <div>
          <h1 className="page-title">Chat</h1>
          <div className="page-sub">
            Live models, grounded in the agency database. Nothing is scripted.
          </div>
        </div>
        <span className={`live-badge ${badge.tone}`} title={status.error ?? `${status.cloudName} model chain`}>
          <i className="dot" />
          {badge.text}
        </span>
      </header>

      <div className="scroll-x" style={{ marginBottom: 12 }}>
        {(Object.keys(AGENT_PROFILES) as AgentKey[]).map((key) => {
          const item = AGENT_PROFILES[key];
          const active = key === agentKey;
          return (
            <button
              key={key}
              className="card agent-chip"
              onClick={() => setAgentKey(key)}
              data-active={active}
              style={{
                minWidth: 132,
                borderColor: active ? item.accent : 'var(--line)',
                boxShadow: active ? `0 0 0 3px ${item.accent}22` : undefined,
              }}
            >
              <div className="row">
                <Avatar name={item.name} color={item.accent} size={30} />
                <div>
                  <div className="item-title" style={{ fontSize: 14 }}>
                    {item.name}
                  </div>
                  <div className="item-sub" style={{ fontSize: 11 }}>
                    {item.role}
                  </div>
                </div>
              </div>
            </button>
          );
        })}
      </div>

      <Card glass style={{ padding: 12 }}>
        <div className="row-between" style={{ gap: 10, flexWrap: 'wrap' }}>
          <div className="row" style={{ gap: 8, flex: 1, minWidth: 200 }}>
            <span className="label" style={{ flex: 'none' }}>
              Case
            </span>
            <select
              className="select"
              value={caseId}
              onChange={(event) => setCaseId(event.target.value)}
              aria-label="Linked case"
            >
              <option value="">No case linked</option>
              {caseOptions.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.reference} — {item.applicantName} ({item.country})
                </option>
              ))}
            </select>
          </div>
          <div className="row" style={{ gap: 8 }}>
            <button className="btn ghost small" onClick={newChat}>
              <RefreshIcon size={15} /> New chat
            </button>
            <button className="btn ghost small" onClick={() => void loadServerCopy()}>
              Sync
            </button>
          </div>
        </div>
      </Card>

      {history.length > 0 && (
        <div className="scroll-x" style={{ marginTop: 10 }}>
          {history.slice(0, 10).map((conversation) => (
            <span
              key={conversation.id}
              className="chat-chip"
              data-active={conversation.id === conversationId}
            >
              <button type="button" onClick={() => openStored(conversation)}>
                {conversation.title} · {relativeTime(conversation.updatedAt)}
              </button>
              <button
                type="button"
                className="chat-chip-x"
                aria-label={`Delete ${conversation.title}`}
                onClick={() => removeStored(conversation.id)}
              >
                ×
              </button>
            </span>
          ))}
          <button
            className="chat-chip chat-chip-plain"
            onClick={() => {
              clearStoredConversations(agentKey);
              reloadHistory(agentKey);
              newChat();
            }}
          >
            Clear device history
          </button>
        </div>
      )}

      <div className="chat-scroll mt" ref={scrollRef} onScroll={onScroll}>
        {messages.length === 0 && !draft && <EmptyState title={`Ask ${profile.name}`} hint={profile.focus} />}
        {messages.map((message) => (
          <div key={message.id} className={`bubble ${message.role === 'user' ? 'user' : 'assistant'}`}>
            {message.role === 'assistant' ? (
              <>
                <div className="md" dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }} />
                {(message.model || message.cancelled) && (
                  <div className="bubble-meta">
                    {message.model && <span>{message.provider ? `${message.provider} · ` : ''}{message.model}</span>}
                    {message.cancelled && <span> · stopped</span>}
                  </div>
                )}
              </>
            ) : (
              message.content
            )}
          </div>
        ))}

        {streaming && (
          <div className="bubble assistant">
            {draft ? (
              <>
                <div className="md" dangerouslySetInnerHTML={{ __html: renderMarkdown(draft) }} />
                <span className="caret" aria-hidden="true" />
              </>
            ) : (
              <span className="typing" aria-label="The model is responding">
                <i />
                <i />
                <i />
              </span>
            )}
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {notices.map((notice, index) => (
        <div key={`${notice}-${index}`} className="stream-notice">
          {notice}
        </div>
      ))}

      {(model || streaming) && (
        <div className="row" style={{ gap: 6, flexWrap: 'wrap', marginTop: 10 }}>
          {streaming && <Pill tone="green" dot>streaming</Pill>}
          {model && <Pill tone={model.tier === 'local' ? 'amber' : 'blue'}>{model.provider}</Pill>}
          {model && <Pill>{model.model}</Pill>}
        </div>
      )}

      <div className="composer">
        <textarea
          className="textarea"
          placeholder={`Message ${profile.name}…`}
          value={input}
          aria-label={`Message ${profile.name}`}
          onChange={(event) => setInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) {
              event.preventDefault();
              void send();
            }
          }}
        />
        {streaming ? (
          <button className="btn danger" onClick={stop} aria-label="Stop generating">
            <StopIcon size={18} />
          </button>
        ) : (
          <button className="btn" onClick={() => void send()} disabled={!input.trim()} aria-label="Send message">
            <SendIcon size={18} />
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * Real-time chat endpoint.
 *
 *   POST /api/chat          Server-Sent Events: tokens are forwarded the moment
 *                           the model produces them.
 *   GET  /api/chat/models   The model chain and what is actually usable.
 *   POST /api/chat/probe    Really opens a stream on each candidate and reports
 *                           first-token latency — no guesses.
 *
 * Failover: candidates are tried in order. A candidate that fails before it
 * emits its first token is skipped (the client is told, via a `fallback`
 * event) and the next one takes over. The VisaHOBe Local Engine closes the
 * chain, so a chat can never end with "no provider configured".
 */
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { AGENT_KEYS, AGENT_PROFILES, agentSchema, type AgentKey } from '@visahobe/shared';
import { parseBody } from '../lib/http.js';
import { badRequest, notFound } from '../lib/errors.js';
import { env } from '../env.js';
import { buildChain, describeChain, localCandidateDescriptor, type ChainCandidate } from '../ai/chain.js';
import { streamLocalText, type StreamRequest } from '../ai/stream.js';
import { builtinAnswer } from '../ai/builtin.js';
import { buildContext, renderContextForModel } from '../ai/engine.js';
import {
  addMessage,
  addTaskEvent,
  createConversation,
  createTask,
  getAgentByKey,
  getConversation,
  listMessages,
  updateTask,
} from '../domain/agents.js';
import { listCases } from '../domain/cases.js';
import { activeSkillsForAgent, renderSkillsForPrompt } from '../domain/skills.js';
import { recordAudit } from '../domain/audit.js';
import type { ChatMessage } from '../ai/types.js';

const streamChatSchema = z.object({
  agentKey: agentSchema,
  message: z.string().min(1).max(8000),
  conversationId: z.string().optional(),
  caseId: z.string().optional(),
});

const probeSchema = z.object({ candidateId: z.string().optional() });

/** Writes one SSE record. Returns false once the socket is gone. */
function makeSender(reply: FastifyReply) {
  let closed = false;
  reply.raw.on('close', () => {
    closed = true;
  });
  return {
    get closed() {
      return closed;
    },
    send(event: string, data: unknown): boolean {
      if (closed || reply.raw.writableEnded) return false;
      reply.raw.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
      return true;
    },
    comment(text: string) {
      if (closed || reply.raw.writableEnded) return;
      reply.raw.write(`: ${text}\n\n`);
    },
    end() {
      if (!reply.raw.writableEnded) reply.raw.end();
    },
  };
}

function openSseResponse(request: FastifyRequest, reply: FastifyReply) {
  const headers: Record<string, string> = {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    // Disable proxy buffering (nginx and most preview proxies honour this).
    'x-accel-buffering': 'no',
  };
  const origin = request.headers.origin;
  if (origin) {
    headers['access-control-allow-origin'] = origin;
    headers['access-control-allow-credentials'] = 'true';
    headers.vary = 'Origin';
  }
  reply.hijack();
  reply.raw.writeHead(200, headers);
  // Flush headers immediately so the browser starts reading right away.
  if (typeof (reply.raw as any).flushHeaders === 'function') (reply.raw as any).flushHeaders();
}

async function resolveCaseId(message: string, explicit?: string | null): Promise<string | null> {
  if (explicit) return explicit;
  const match = message.match(/\bVM-[A-Z0-9]{4,}\b/i);
  if (!match) return null;
  const { items } = await listCases({ q: match[0], pageSize: 3 });
  return items[0]?.id ?? null;
}

export async function chatRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/chat/models', { preHandler: app.requireAuth }, async () => {
    const chain = await describeChain();
    return {
      ...chain,
      streaming: true,
      timeoutMs: env.CHAT_STREAM_TIMEOUT_MS,
    };
  });

  app.post('/api/chat/probe', { preHandler: app.requireAuth }, async (req) => {
    const body = parseBody(probeSchema, req.body ?? {});
    const chain = await buildChain();
    const targets = body.candidateId ? chain.filter((c) => c.id === body.candidateId) : chain;
    if (body.candidateId && !targets.length) throw notFound('Unknown model candidate');

    const probeRequest: StreamRequest = {
      system: 'Reply with the single word: ready.',
      messages: [{ role: 'user', content: 'ready?' }],
      maxTokens: 8,
      temperature: 0,
    };

    const results = [];
    for (const candidate of targets) {
      if (!candidate.available || !candidate.open) {
        results.push({
          id: candidate.id,
          label: candidate.label,
          tier: candidate.tier,
          ok: false,
          message: candidate.note ?? 'Not configured on this server.',
        });
        continue;
      }
      const started = Date.now();
      const controller = new AbortController();
      try {
        let first = '';
        for await (const chunk of candidate.open(probeRequest, {
          signal: controller.signal,
          connectTimeoutMs: 12_000,
          idleTimeoutMs: 12_000,
        })) {
          first += chunk;
          if (first.trim().length) break;
        }
        controller.abort();
        results.push({
          id: candidate.id,
          label: candidate.label,
          tier: candidate.tier,
          ok: true,
          firstTokenMs: Date.now() - started,
          message: `Streaming — first token in ${Date.now() - started} ms`,
        });
      } catch (err) {
        controller.abort();
        results.push({
          id: candidate.id,
          label: candidate.label,
          tier: candidate.tier,
          ok: false,
          firstTokenMs: Date.now() - started,
          message: (err as Error).message,
        });
      }
    }
    const local = localCandidateDescriptor();
    results.push({
      id: local.id,
      label: local.label,
      tier: local.tier,
      ok: true,
      firstTokenMs: 0,
      message: 'Offline engine — always ready.',
    });
    return { results };
  });

  app.post('/api/chat', { preHandler: app.requireAuth }, async (req, reply) => {
    const body = parseBody(streamChatSchema, req.body);
    if (!AGENT_KEYS.includes(body.agentKey)) throw badRequest('Unknown agent');
    const agentKey = body.agentKey as AgentKey;
    const agent = await getAgentByKey(agentKey);
    if (!agent) throw notFound('Agent not found');

    let conversation = body.conversationId ? await getConversation(body.conversationId) : undefined;
    if (!conversation) {
      conversation = await createConversation({
        agentId: agent.id,
        userId: req.user!.id,
        title: body.message.slice(0, 60),
        caseId: body.caseId ?? null,
      });
    }
    const caseId = await resolveCaseId(body.message, body.caseId ?? conversation.caseId);

    const task = await createTask({
      agentId: agent.id,
      conversationId: conversation.id,
      userId: req.user!.id,
      caseId,
      kind: 'chat.stream',
      title: body.message.slice(0, 80),
      input: body.message,
    });

    // From here on the response is a live stream: errors are reported as SSE
    // events, never as an HTTP status, because the headers are already sent.
    const sse = makeSender(reply);
    openSseResponse(req, reply);

    const controller = new AbortController();
    req.raw.on('close', () => controller.abort());
    const heartbeat = setInterval(() => sse.comment('keep-alive'), 15_000);

    let answer = '';
    let usedCandidate: ChainCandidate = localCandidateDescriptor();
    const attempts: Array<{ id: string; label: string; ok: boolean; message?: string }> = [];

    try {
      const context = await buildContext(caseId, agentKey);
      const history = await listMessages(conversation.id, 12);
      await addMessage({
        conversationId: conversation.id,
        agentId: agent.id,
        role: 'user',
        content: body.message,
        taskId: task.id,
      });

      const skills = await activeSkillsForAgent(agentKey).catch(() => []);
      const profile = AGENT_PROFILES[agentKey];
      const system = [
        agent.systemPrompt,
        '',
        `You are ${profile?.name ?? agent.name}, ${profile?.role ?? agent.role} at the VisaHOBe agency.`,
        'Answer in the language the operator writes in (Bangla or English). Be concise and practical.',
        '',
        'Context from the agency database (use it as the only source of truth):',
        renderContextForModel(context),
        '',
        renderSkillsForPrompt(skills),
        '',
        'Rules: never invent official requirements, receipts, approvals or government outcomes. ' +
          'If something is not in the context, say it is not recorded. ' +
          'For consequential actions (portal submission, payments, account changes) ask for human approval.',
      ].join('\n');

      const messages: ChatMessage[] = [
        ...history.map((m) => ({ role: m.role as ChatMessage['role'], content: m.content })),
        { role: 'user' as const, content: body.message },
      ];
      const streamRequest: StreamRequest = {
        system,
        messages,
        temperature: agent.temperature ?? 0.3,
        maxTokens: 1500,
      };

      const chain = await buildChain({ modelOverride: agent.model || null });
      sse.send('start', {
        conversationId: conversation.id,
        taskId: task.id,
        agentKey,
        caseId,
        chain: chain.map((c) => ({ id: c.id, label: c.label, tier: c.tier, available: c.available })),
      });
      await addTaskEvent({
        taskId: task.id,
        type: 'plan',
        message: `Streaming turn for ${agent.name} across ${chain.filter((c) => c.available).length} model(s).`,
        data: { chain: chain.map((c) => c.id) },
      });
      await updateTask(task.id, { status: 'running', progress: 15 });

      for (const candidate of chain) {
        if (controller.signal.aborted) break;
        if (!candidate.available || !candidate.open) {
          attempts.push({ id: candidate.id, label: candidate.label, ok: false, message: candidate.note });
          continue;
        }
        let produced = false;
        try {
          for await (const delta of candidate.open(streamRequest, {
            signal: controller.signal,
            idleTimeoutMs: env.CHAT_STREAM_TIMEOUT_MS,
          })) {
            if (controller.signal.aborted) break;
            if (!produced) {
              produced = true;
              usedCandidate = candidate;
              sse.send('model', {
                id: candidate.id,
                label: candidate.label,
                provider: candidate.providerName,
                model: candidate.model,
                tier: candidate.tier,
              });
              await addTaskEvent({
                taskId: task.id,
                type: 'provider',
                message: `${candidate.providerName} → ${candidate.label} is streaming.`,
              });
            }
            answer += delta;
            if (!sse.send('delta', { text: delta })) {
              controller.abort();
              break;
            }
          }
          if (produced && answer.trim()) {
            attempts.push({ id: candidate.id, label: candidate.label, ok: true });
            break;
          }
          attempts.push({
            id: candidate.id,
            label: candidate.label,
            ok: false,
            message: 'Upstream produced no text.',
          });
          sse.send('fallback', { from: candidate.label, reason: 'Model returned an empty answer.' });
        } catch (err) {
          const message = (err as Error).message;
          attempts.push({ id: candidate.id, label: candidate.label, ok: false, message });
          await addTaskEvent({
            taskId: task.id,
            type: 'provider_error',
            message: `${candidate.label} failed: ${message}`,
          });
          if (produced && answer.trim()) {
            // Tokens already reached the operator: keep them and stop here.
            sse.send('notice', { message: `${candidate.label} dropped the connection mid-answer.` });
            break;
          }
          answer = '';
          sse.send('fallback', { from: candidate.label, reason: message });
        }
      }

      if (!answer.trim() && !controller.signal.aborted) {
        // Nothing upstream worked — answer from the agency database instead.
        const local = builtinAnswer(agentKey, body.message, context);
        usedCandidate = localCandidateDescriptor();
        sse.send('model', {
          id: usedCandidate.id,
          label: usedCandidate.label,
          provider: usedCandidate.providerName,
          model: usedCandidate.model,
          tier: 'local',
        });
        for await (const delta of streamLocalText(local.content, { signal: controller.signal })) {
          answer += delta;
          if (!sse.send('delta', { text: delta })) break;
        }
        await addTaskEvent({
          taskId: task.id,
          type: 'provider',
          message: 'Answered with the VisaHOBe Local Engine (no upstream model was reachable).',
        });
      }

      const cancelled = controller.signal.aborted;
      const stored = await addMessage({
        conversationId: conversation.id,
        agentId: agent.id,
        role: 'assistant',
        content: answer || '(no answer produced)',
        taskId: task.id,
        meta: {
          provider: usedCandidate.providerName,
          providerKind: usedCandidate.vendor,
          model: usedCandidate.model,
          tier: usedCandidate.tier,
          streamed: true,
          attempts,
          cancelled,
        },
      });
      await updateTask(task.id, {
        status: cancelled ? 'cancelled' : 'completed',
        progress: 100,
        result: { content: answer, provider: usedCandidate.providerName, model: usedCandidate.model, attempts },
      });
      await recordAudit({
        actorId: req.user!.id,
        actorEmail: req.user!.email,
        action: 'agent.chat.stream',
        entity: 'agent',
        entityId: agentKey,
        meta: { taskId: task.id, model: usedCandidate.model, tier: usedCandidate.tier, cancelled },
        ip: req.ip,
      }).catch(() => undefined);

      sse.send('done', {
        conversationId: conversation.id,
        taskId: task.id,
        messageId: stored.id,
        content: answer,
        provider: usedCandidate.providerName,
        label: usedCandidate.label,
        model: usedCandidate.model,
        tier: usedCandidate.tier,
        usedLocalEngine: usedCandidate.tier === 'local',
        cancelled,
        attempts,
      });
    } catch (err) {
      const message = (err as Error).message;
      await addTaskEvent({ taskId: task.id, type: 'error', message }).catch(() => undefined);
      await updateTask(task.id, { status: 'failed', error: message, progress: 100 }).catch(() => undefined);
      sse.send('error', { message });
    } finally {
      clearInterval(heartbeat);
      sse.end();
    }
  });
}

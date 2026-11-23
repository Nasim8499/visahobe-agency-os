/**
 * The model chain.
 *
 * A chat request walks an ordered list of candidates and uses the first one
 * that actually produces tokens. The order is deliberate:
 *
 *   1. Managed cloud models (N Space Cloud) — the key lives on this server, so
 *      operators never have to paste one into the browser.
 *   2. Direct vendor models for which this server holds a key.
 *   3. Providers configured by the agency in Settings (bring your own key).
 *   4. The VisaHOBe Local Engine — offline, deterministic, always available.
 *
 * A candidate that is not configured is still reported to the UI with the
 * reason, so the Connect screen can show the truth instead of a guess.
 */
import { env } from '../env.js';
import { getProviderWithSecret, listProviders } from '../domain/providers.js';
import {
  streamAnthropic,
  streamGemini,
  streamOpenAiCompatible,
  type StreamOptions,
  type StreamRequest,
  type TextStream,
} from './stream.js';

export type CandidateTier = 'cloud' | 'byok' | 'local';
export type CandidateVendor = 'openai' | 'gemini' | 'anthropic' | 'xai' | 'relay' | 'compatible' | 'local';

export interface ChainCandidate {
  /** Stable id used in SSE events and in the UI. */
  id: string;
  /** Human label, e.g. "GPT-4o mini". */
  label: string;
  /** Who serves it, e.g. "N Space Cloud" or the provider name from Settings. */
  providerName: string;
  model: string;
  vendor: CandidateVendor;
  tier: CandidateTier;
  available: boolean;
  /** Why it cannot be used, when available is false. */
  note?: string;
  open?: (request: StreamRequest, options: StreamOptions) => TextStream;
}

export interface CloudModelSpec {
  id: string;
  label: string;
  vendor: CandidateVendor;
  model: string;
  keyEnv: string;
  baseUrl?: string;
  note?: string;
}

/**
 * Models the managed cloud can serve, in preference order. Each entry can be
 * served either through the relay (one key for all of them) or directly when
 * this server holds that vendor's key.
 */
export const CLOUD_CATALOGUE: CloudModelSpec[] = [
  /* ---- Coarena Model API (one key serves every vendor below) ---- */
  {
    id: 'gpt-6-luna',
    label: 'GPT-6 Luna',
    vendor: 'relay',
    model: 'gpt-6-luna',
    keyEnv: 'COARENA_API_KEY',
    baseUrl: 'https://coarena.ai/api/v1',
  },
  {
    id: 'gpt-6-2-astra',
    label: 'GPT-6.2 Astra',
    vendor: 'relay',
    model: 'gpt-6-2-astra',
    keyEnv: 'COARENA_API_KEY',
    baseUrl: 'https://coarena.ai/api/v1',
  },
  {
    id: 'gemini-3-8-flash',
    label: 'Gemini 3.8 Flash',
    vendor: 'relay',
    model: 'gemini-3-8-flash',
    keyEnv: 'COARENA_API_KEY',
    baseUrl: 'https://coarena.ai/api/v1',
  },
  {
    id: 'deepseek-v4-1-flash-1017',
    label: 'DeepSeek V4.1 Flash',
    vendor: 'relay',
    model: 'deepseek-v4-1-flash-1017',
    keyEnv: 'COARENA_API_KEY',
    baseUrl: 'https://coarena.ai/api/v1',
  },
  {
    id: 'claude-sonnet-5-5',
    label: 'Claude Sonnet 5.5',
    vendor: 'relay',
    model: 'claude-sonnet-5-5',
    keyEnv: 'COARENA_API_KEY',
    baseUrl: 'https://coarena.ai/api/v1',
  },
  {
    id: 'gpt-4o-mini',
    label: 'GPT-4o mini',
    vendor: 'openai',
    model: 'gpt-4o-mini',
    keyEnv: 'OPENAI_API_KEY',
    baseUrl: 'https://api.openai.com/v1',
  },
  {
    id: 'gemini-2.5-flash',
    label: 'Gemini 2.5 Flash',
    vendor: 'gemini',
    model: 'gemini-2.5-flash',
    keyEnv: 'GEMINI_API_KEY',
  },
  {
    id: 'claude-3-5-haiku',
    label: 'Claude 3.5 Haiku',
    vendor: 'anthropic',
    model: 'claude-3-5-haiku-latest',
    keyEnv: 'ANTHROPIC_API_KEY',
  },
  {
    id: 'grok-4.3',
    label: 'Grok 4.3',
    vendor: 'xai',
    model: 'grok-4.3',
    keyEnv: 'XAI_API_KEY',
    baseUrl: 'https://api.x.ai/v1',
    note: 'Paid tier — add billing on the xAI account, then set XAI_API_KEY.',
  },
];

export const LOCAL_CANDIDATE_ID = 'visahobe-local-engine';

function cloudModelIds(): string[] {
  const configured = env.AI_CLOUD_MODELS.split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (configured.length) return configured;
  return CLOUD_CATALOGUE.map((entry) => entry.id);
}

function specFor(id: string): CloudModelSpec {
  const known = CLOUD_CATALOGUE.find((entry) => entry.id === id || entry.model === id);
  if (known) return known;
  // An operator can name any model the relay understands.
  return { id, label: id, vendor: 'relay', model: id, keyEnv: 'AI_CLOUD_KEY' };
}

function vendorKey(spec: CloudModelSpec): string | undefined {
  const value = (process.env[spec.keyEnv] ?? '').trim();
  return value || undefined;
}

/* --------------------- self-hosted reachability cache -------------------- */

const reachability = new Map<string, { at: number; ok: boolean }>();
const REACH_TTL_MS = 60_000;

function isSelfHosted(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return (
      host === 'localhost' ||
      host === '127.0.0.1' ||
      host === '::1' ||
      host === '0.0.0.0' ||
      host.endsWith('.local') ||
      /^10\./.test(host) ||
      /^192\.168\./.test(host) ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(host)
    );
  } catch {
    return false;
  }
}

/**
 * A self-hosted model server (Ollama, LM Studio, vLLM) is usually either
 * running or not. Checking costs a few milliseconds on the loopback, so the
 * UI can say "not reachable" instead of pretending the model is ready.
 */
async function selfHostedReachable(baseUrl: string): Promise<boolean> {
  const key = baseUrl.replace(/\/$/, '');
  const hit = reachability.get(key);
  if (hit && Date.now() - hit.at < REACH_TTL_MS) return hit.ok;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 900);
  let ok = false;
  try {
    const response = await fetch(`${key}/models`, { signal: controller.signal });
    ok = response.status < 500;
  } catch {
    ok = false;
  } finally {
    clearTimeout(timer);
  }
  reachability.set(key, { at: Date.now(), ok });
  return ok;
}

/** Test seam / operator action: forget cached reachability results. */
export function clearReachabilityCache(): void {
  reachability.clear();
}

/** Candidates served by the managed cloud (relay first, then direct keys). */
function cloudCandidates(): ChainCandidate[] {
  const relayUrl = env.AI_CLOUD_URL.trim().replace(/\/$/, '');
  const relayKey = env.AI_CLOUD_KEY.trim();
  const out: ChainCandidate[] = [];

  for (const id of cloudModelIds()) {
    const spec = specFor(id);
    if (relayUrl) {
      out.push({
        id: `cloud:${spec.id}`,
        label: spec.label,
        providerName: env.AI_CLOUD_NAME,
        model: spec.model,
        vendor: spec.vendor,
        tier: 'cloud',
        available: true,
        open: (request, options) =>
          streamOpenAiCompatible(
            { baseUrl: relayUrl, model: spec.model, apiKey: relayKey || undefined },
            request,
            options,
          ),
      });
      continue;
    }
    const key = vendorKey(spec);
    if (!key) {
      out.push({
        id: `cloud:${spec.id}`,
        label: spec.label,
        providerName: env.AI_CLOUD_NAME,
        model: spec.model,
        vendor: spec.vendor,
        tier: 'cloud',
        available: false,
        note: spec.note ?? `Not enabled on this server — set ${spec.keyEnv} or AI_CLOUD_URL.`,
      });
      continue;
    }
    out.push({
      id: `cloud:${spec.id}`,
      label: spec.label,
      providerName: env.AI_CLOUD_NAME,
      model: spec.model,
      vendor: spec.vendor,
      tier: 'cloud',
      available: true,
      open: (request, options) => {
        if (spec.vendor === 'gemini') return streamGemini({ model: spec.model, apiKey: key }, request, options);
        if (spec.vendor === 'anthropic') return streamAnthropic({ model: spec.model, apiKey: key }, request, options);
        return streamOpenAiCompatible(
          { baseUrl: spec.baseUrl ?? 'https://api.openai.com/v1', model: spec.model, apiKey: key },
          request,
          options,
        );
      },
    });
  }
  return out;
}

/** Candidates configured by the agency in Settings → Providers. */
async function providerCandidates(modelOverride?: string | null): Promise<ChainCandidate[]> {
  const providers = await listProviders().catch(() => []);
  const out: ChainCandidate[] = [];
  for (const summary of providers) {
    if (!summary.enabled || summary.kind === 'builtin') continue;
    const record = await getProviderWithSecret(summary.id);
    if (!record) continue;
    const model = modelOverride || record.model;
    const needsKey = record.kind === 'openai' || record.kind === 'anthropic' || record.kind === 'gemini';
    if (needsKey && !record.apiKey) {
      out.push({
        id: `provider:${record.id}`,
        label: `${record.name} · ${model}`,
        providerName: record.name,
        model,
        vendor: record.kind === 'gemini' ? 'gemini' : record.kind === 'anthropic' ? 'anthropic' : 'openai',
        tier: 'byok',
        available: false,
        note: 'No API key stored for this provider.',
      });
      continue;
    }
    const baseUrl =
      record.baseUrl ||
      (record.kind === 'ollama'
        ? `${env.OLLAMA_BASE_URL.replace(/\/$/, '')}/v1`
        : record.kind === 'deepseek'
          ? 'https://api.deepseek.com/v1'
          : 'https://api.openai.com/v1');
    // A self-hosted endpoint that is not running must not be advertised as ready.
    const reachable = isSelfHosted(baseUrl) ? await selfHostedReachable(baseUrl) : true;
    out.push({
      id: `provider:${record.id}`,
      label: `${record.name} · ${model}`,
      providerName: record.name,
      model,
      vendor:
        record.kind === 'gemini'
          ? 'gemini'
          : record.kind === 'anthropic'
            ? 'anthropic'
            : 'compatible',
      tier: 'byok',
      available: reachable,
      note: reachable ? undefined : `Not reachable at ${baseUrl} — start the local model server.`,
      open: (request, options) => {
        if (record.kind === 'gemini') {
          return streamGemini({ model, apiKey: record.apiKey ?? '' }, request, options);
        }
        if (record.kind === 'anthropic') {
          return streamAnthropic(
            { model, apiKey: record.apiKey ?? '', baseUrl: record.baseUrl ?? undefined },
            request,
            options,
          );
        }
        return streamOpenAiCompatible({ baseUrl, model, apiKey: record.apiKey }, request, options);
      },
    });
  }
  // The agency default provider goes first.
  const defaultId = providers.find((p) => p.isDefault)?.id;
  if (defaultId) {
    out.sort((a, b) => Number(b.id === `provider:${defaultId}`) - Number(a.id === `provider:${defaultId}`));
  }
  return out;
}

export function localCandidateDescriptor(): ChainCandidate {
  return {
    id: LOCAL_CANDIDATE_ID,
    label: 'VisaHOBe Local Engine',
    providerName: 'VisaHOBe Local Engine',
    model: 'deterministic-ops-engine',
    vendor: 'local',
    tier: 'local',
    available: true,
    note: 'Offline answer composed from agency records. Always available.',
  };
}

/**
 * Ordered candidates for a chat turn. The local engine is not included: the
 * caller appends it because its text depends on the case context.
 */
export async function buildChain(options: { modelOverride?: string | null } = {}): Promise<ChainCandidate[]> {
  const [cloud, byok] = await Promise.all([
    Promise.resolve(cloudCandidates()),
    providerCandidates(options.modelOverride),
  ]);
  return [...cloud, ...byok];
}

/** Full chain (including the local engine) for status screens. */
export async function describeChain(): Promise<{
  cloudName: string;
  relayConfigured: boolean;
  live: boolean;
  candidates: Array<Omit<ChainCandidate, 'open'>>;
}> {
  const candidates = await buildChain();
  const stripped = candidates.map(({ open, ...rest }) => rest);
  return {
    cloudName: env.AI_CLOUD_NAME,
    relayConfigured: Boolean(env.AI_CLOUD_URL.trim()),
    live: candidates.some((candidate) => candidate.available),
    candidates: [...stripped, localCandidateDescriptor()],
  };
}

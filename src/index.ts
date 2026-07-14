/**
 * Omi real-time conversation assistant — Cloudflare Worker.
 *
 * Omi POSTs live transcript segments to /webhook. The Worker follows the
 * conversation and, when it would help, asks Claude Haiku for something Kevin
 * could say next — PROACTIVELY, not only when directly asked — then delivers
 * that suggestion to Kevin's phone via Pushover.
 *
 * Design rules:
 *  - Respond to Omi in <1ms: return 200 {"accepted": true} and do all real work
 *    asynchronously via ctx.waitUntil(). Omi never waits on the AI call.
 *  - A failure anywhere in the async pipeline must never surface to Omi.
 *  - Claude itself decides whether a moment is worth a suggestion (else "SKIP"),
 *    so we can be proactive without spamming.
 *  - Logs use short tags: RECV / FILTERED / AI_CALL / AI_DONE / SKIPPED /
 *    NOTIFIED / RATE_LIMITED / ERROR. Secrets are never logged.
 */

export interface Env {
  OMI_SESSIONS: KVNamespace;
  ANTHROPIC_API_KEY: string;
  // Delivery is via Pushover (no per-hour limit). Omi remains the listener that
  // POSTs transcripts to /webhook.
  PUSHOVER_APP_TOKEN: string;
  PUSHOVER_USER_KEY: string;
}

// =============================================================================
// KEVIN PROFILE  ── REPLACE THIS ONE CONSTANT to change what the assistant knows.
// This is a temporary short profile. Later, swap the string below for a full
// knowledge-base document (bio, methods, sample answers, do's and don'ts).
// Nothing else in the code needs to change.
// =============================================================================
const KEVIN_PROFILE = `Kevin D. Trice, Ed.D., is a senior education leader with ~15 years of experience as a practitioner, district leader, and researcher. He is a Senior Director at Communities In Schools working on district partnerships, and separately runs LaunchPoint, an education consulting practice. His expertise: MTSS, in-school suspension redesign, implementation science (NIRN Active Implementation Frameworks), school counseling infrastructure, and district–community partnerships.`;

// The full system prompt, assembled from the profile above.
const SYSTEM_PROMPT = `You are Kevin's silent real-time conversation assistant. You quietly follow Kevin's live, in-person conversations and suggest what he could say next — delivered to his phone. You are PROACTIVE: you don't only wait to be asked a direct question; you offer something to say whenever it would genuinely help Kevin in the moment.

Offer a suggestion (starting with "Say:") when:
- Someone asked a question (any subject) — provide the answer Kevin could give.
- Someone shared something Kevin could warmly respond to, build on, ask a good follow-up about, or acknowledge.
- There's a natural opening for Kevin to add a useful insight, a thoughtful reply, or to move the conversation forward.

Each line is labelled by speaker: "Them:" is the other person and "Me:" is Kevin. Focus on what the OTHER person ("Them:") asks or says — that's what Kevin needs help responding to. Speaker labels are usually right but can occasionally be off, so if there's clearly a question in the air, help even if it looks mislabelled. Lean toward being helpful.

Reply with EXACTLY "SKIP" (nothing else) only when there's genuinely nothing worth saying — pure filler ("okay", "yeah"), logistics, half-finished thoughts, or unclear audio. When in doubt and there's a question in the air, answer it rather than skipping.

When you do suggest something:
- Begin with "Say:"
- Write about 25–40 words: warm and genuinely helpful with a bit of real substance, but keep it tight and fast to read aloud — not a speech.
- First person, confident and friendly, in natural spoken language.
- Reply in the SAME language the other person used — if they asked in Spanish, write Kevin's reply in Spanish.
- Answer questions on ANY subject; never refuse a harmless one.
- Never mention AI or that this is generated.
- Don't invent specific statistics, names, prices, or commitments as fact; if unsure, speak in general terms or offer a warm clarifying question.

Background on Kevin (use it when the conversation relates to his work; otherwise ignore it):
${KEVIN_PROFILE}`;

// -----------------------------------------------------------------------------
// Tunables — the knobs you'll most likely want to adjust while tuning.
// -----------------------------------------------------------------------------
const TRANSCRIPT_CAP = 3000; // max chars of rolling transcript kept per session
const COOLDOWN_MS = 10_000; // min gap between suggestions for one session
const EVAL_DEDUP_MS = 30_000; // don't re-evaluate the same utterance within this window
const RATE_LIMIT_BACKOFF_MS = 5 * 60 * 1000; // pause this long after a 429 from the push provider
const SESSION_TTL_SECONDS = 2 * 60 * 60; // KV key self-expires after 2 hours
const ANTHROPIC_MODEL = "claude-haiku-4-5";
const ANTHROPIC_MAX_TOKENS = 160; // enough for a warm 25–40 word reply, fast to generate
const CONTEXT_CHARS = 1200; // chars of recent transcript sent to Claude

// Per-isolate counter so we log the full raw payload for the first few requests
// (to verify the real payload shape against our assumptions), then go quiet.
let rawLogCount = 0;
const RAW_LOG_LIMIT = 5;

interface Segment {
  text?: string;
  speaker?: string;
  is_user?: boolean;
  start?: number;
  end?: number;
  [k: string]: unknown; // tolerate extra fields
}

interface SessionState {
  recent_transcript: string; // rolling, speaker-labelled ("Them:" / "Me:")
  last_eval_hash: string; // hash of the last utterance we evaluated (answered or skipped)
  last_eval_at: number; // epoch ms of the last evaluation
  last_notified_at: number; // epoch ms of the last suggestion actually sent
  uid: string; // captured from the request
  last_answer?: string; // last draft we sent (handy for inspecting via KV)
  last_delivery_status?: number; // HTTP status from the push provider for the last delivery
  last_delivery_body?: string; // push provider response body for the last delivery
  backoff_until?: number; // pause firing until this epoch ms (set after a 429)
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    if (request.method === "POST" && url.pathname === "/webhook") {
      // Read the body here (before responding); process everything else async.
      const rawText = await request.text();

      if (rawLogCount < RAW_LOG_LIMIT) {
        rawLogCount++;
        console.log(`RECV raw payload (${rawLogCount}/${RAW_LOG_LIMIT}) query=${url.search} body=${rawText}`);
      } else {
        console.log(`RECV query=${url.search} bytes=${rawText.length}`);
      }

      let body: Record<string, unknown> = {};
      try {
        body = rawText ? (JSON.parse(rawText) as Record<string, unknown>) : {};
      } catch {
        // Malformed payload: log and 200 anyway — Omi should never see an error.
        console.log(`ERROR malformed JSON body=${rawText.slice(0, 500)}`);
        return accepted();
      }

      // Never let the async pipeline crash the webhook response.
      ctx.waitUntil(
        processWebhook(env, url, body).catch((e) => {
          console.log(`ERROR pipeline ${stringifyErr(e)}`);
        }),
      );

      return accepted();
    }

    return new Response("not found", { status: 404 });
  },
};

function accepted(): Response {
  return new Response(JSON.stringify({ accepted: true }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

// -----------------------------------------------------------------------------
// The async pipeline. Runs after we've already replied 200 to Omi.
// -----------------------------------------------------------------------------
async function processWebhook(
  env: Env,
  url: URL,
  body: Record<string, unknown>,
): Promise<void> {
  // uid and session_id can arrive as query params OR in the body — handle both.
  const uid = str(url.searchParams.get("uid")) || str(body.uid) || "";
  const sessionId =
    str(url.searchParams.get("session_id")) || str(body.session_id) || uid || "unknown";

  const segments: Segment[] = Array.isArray(body.segments) ? (body.segments as Segment[]) : [];

  // Load (or initialize) session state.
  const stored = await env.OMI_SESSIONS.get(sessionId);
  const state: SessionState = stored
    ? (JSON.parse(stored) as SessionState)
    : { recent_transcript: "", last_eval_hash: "", last_eval_at: 0, last_notified_at: 0, uid };
  if (uid) state.uid = uid;

  // Append every new segment (both sides — context needs both). Omi streams
  // tiny partial fragments (often mid-word), so we MERGE consecutive segments
  // from the same speaker into one line instead of one line per fragment —
  // this reassembles a coherent utterance we can scan for a completed sentence.
  for (const seg of segments) {
    const text = str(seg?.text).trim();
    if (!text) continue;
    const prefix = seg?.is_user === true ? "Me:" : "Them:";
    const lines = state.recent_transcript ? state.recent_transcript.split("\n") : [];
    if (lines.length > 0 && lines[lines.length - 1].startsWith(prefix)) {
      lines[lines.length - 1] += ` ${text}`; // same speaker still talking → merge
    } else {
      lines.push(`${prefix} ${text}`); // speaker changed → new line
    }
    state.recent_transcript = lines.join("\n");
  }
  // Cap to the most recent ~TRANSCRIPT_CAP chars.
  if (state.recent_transcript.length > TRANSCRIPT_CAP) {
    state.recent_transcript = state.recent_transcript.slice(-TRANSCRIPT_CAP);
  }

  const now = Date.now();
  const decision = decide(state, now);

  if (!decision.fire) {
    console.log(`FILTERED ${decision.reason} session=${sessionId}`);
    await persist(env, sessionId, state);
    return;
  }

  // A new completed thing was said → let Claude decide if it's worth a suggestion.
  console.log(`AI_CALL session=${sessionId} heard="${truncate(decision.candidate, 120)}"`);
  const t0 = Date.now();
  const answer = await callClaude(env, state.recent_transcript);
  console.log(`AI_DONE session=${sessionId} ms=${Date.now() - t0} ok=${answer !== null}`);

  // Mark this utterance evaluated regardless of outcome, so we don't re-call on it.
  state.last_eval_hash = decision.hash;
  state.last_eval_at = now;

  if (!answer) {
    // AI failed — log and move on. Do NOT retry in a loop.
    console.log(`ERROR ai returned no answer session=${sessionId}`);
    await persist(env, sessionId, state);
    return;
  }

  // Claude decides nothing is worth saying right now.
  if (answer.trim().toUpperCase().startsWith("SKIP")) {
    console.log(`SKIPPED session=${sessionId} (nothing worth saying)`);
    await persist(env, sessionId, state);
    return;
  }

  // Deliver the suggestion.
  const delivery = await notifyPushover(env, answer);
  state.last_answer = answer;
  state.last_delivery_status = delivery.status;
  state.last_delivery_body = delivery.body;

  const delivered = delivery.status >= 200 && delivery.status < 300;
  if (delivered) {
    // Success: start the cooldown and clear the slate so the NEXT moment is
    // evaluated fresh (avoids a growing transcript blob).
    state.last_notified_at = now;
    state.backoff_until = 0;
    state.recent_transcript = "";
  } else if (delivery.status === 429) {
    // Push provider rate-limited us — back off, keep the transcript so we retry.
    state.backoff_until = now + RATE_LIMIT_BACKOFF_MS;
    console.log(`RATE_LIMITED session=${sessionId} backoff_until=${now + RATE_LIMIT_BACKOFF_MS}`);
  }
  await persist(env, sessionId, state);
}

async function persist(env: Env, sessionId: string, state: SessionState): Promise<void> {
  await env.OMI_SESSIONS.put(sessionId, JSON.stringify(state), {
    expirationTtl: SESSION_TTL_SECONDS,
  });
}

// -----------------------------------------------------------------------------
// Decide whether to evaluate this moment with Claude. (Claude then decides
// whether to actually suggest something or SKIP.)
// -----------------------------------------------------------------------------
type Decision =
  | { fire: true; candidate: string; hash: string }
  | { fire: false; reason: string };

function decide(state: SessionState, now: number): Decision {
  const recent = state.recent_transcript.slice(-CONTEXT_CHARS).trim();
  if (!recent) return { fire: false, reason: "no-content" };

  // Need at least one finished sentence somewhere (don't fire on pure fragments).
  if (!extractLatestCompletedUtterance(state.recent_transcript)) {
    return { fire: false, reason: "no-completed-utterance-yet" };
  }

  if (state.backoff_until && now < state.backoff_until) {
    return { fire: false, reason: "rate-limit-backoff" };
  }

  // Dedup on the WHOLE recent context (not just the last sentence) — so any new
  // content re-triggers a look. Claude then finds the latest UNANSWERED question
  // in the context, which fixes buried questions when several arrive at once.
  const hash = hashString(normalizeText(recent));
  if (hash === state.last_eval_hash && now - (state.last_eval_at ?? 0) < EVAL_DEDUP_MS) {
    return { fire: false, reason: "already-evaluated" };
  }

  // Space out delivered suggestions.
  if (now - state.last_notified_at < COOLDOWN_MS) return { fire: false, reason: "cooldown" };

  return { fire: true, candidate: recent, hash };
}

// The most recent COMPLETED sentence (ends in . ? !) in `text`, or null if the
// speaker hasn't finished a sentence yet. Whether it's worth responding to is
// left to Claude (proactive mode), not a rigid question filter.
function extractLatestCompletedUtterance(text: string): string | null {
  const sentences = text.match(/[^.?!]+[.?!]+/g);
  if (!sentences) return null;
  const last = sentences[sentences.length - 1].trim();
  return last || null;
}

function normalizeText(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

// Small, dependency-free deterministic hash (djb2) — plenty for dedup.
function hashString(s: string): string {
  let h = 5381;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(16);
}

// -----------------------------------------------------------------------------
// Anthropic Messages API — draft Kevin's next words (or "SKIP").
// -----------------------------------------------------------------------------
async function callClaude(env: Env, transcript: string): Promise<string | null> {
  // Send only the tail of the transcript, keeping the speaker labels ("Them:" is
  // the other person, "Me:" is Kevin) so Claude knows who is asking.
  const context = transcript.slice(-CONTEXT_CHARS);
  const userMessage =
    `Here is the recent conversation Kevin is in ("Them:" is the other person, "Me:" is Kevin; most recent last):\n${context}\n\n` +
    `Find the most recent question or clear request from the other person that Kevin hasn't answered yet — it may NOT be the very last line — and write Kevin's reply to it, starting with "Say:". If there's genuinely nothing that needs a response, reply with exactly SKIP.`;

  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: ANTHROPIC_MODEL,
        max_tokens: ANTHROPIC_MAX_TOKENS,
        system: SYSTEM_PROMPT,
        messages: [{ role: "user", content: userMessage }],
      }),
    });

    if (!res.ok) {
      const errBody = await res.text();
      console.log(`ERROR anthropic status=${res.status} body=${truncate(errBody, 300)}`);
      return null;
    }

    const data = (await res.json()) as { content?: Array<{ type?: string; text?: string }> };
    const text = Array.isArray(data.content)
      ? data.content
          .filter((b) => b.type === "text" && typeof b.text === "string")
          .map((b) => b.text as string)
          .join("")
          .trim()
      : "";

    return text || null;
  } catch (e) {
    console.log(`ERROR anthropic exception ${stringifyErr(e)}`);
    return null;
  }
}

// -----------------------------------------------------------------------------
// Pushover — deliver the draft to the phone. No per-hour limit like Omi's.
// -----------------------------------------------------------------------------
async function notifyPushover(
  env: Env,
  message: string,
): Promise<{ status: number; body: string }> {
  const form = new URLSearchParams({
    token: env.PUSHOVER_APP_TOKEN,
    user: env.PUSHOVER_USER_KEY,
    message,
    title: "Suggested reply",
  });

  try {
    const res = await fetch("https://api.pushover.net/1/messages.json", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: form.toString(),
    });

    const respBody = await res.text();
    if (res.ok) {
      console.log(`NOTIFIED pushover status=${res.status} msg="${truncate(message, 120)}"`);
    } else {
      console.log(`ERROR pushover status=${res.status} body=${truncate(respBody, 300)}`);
    }
    return { status: res.status, body: truncate(respBody, 300) };
  } catch (e) {
    console.log(`ERROR pushover exception ${stringifyErr(e)}`);
    return { status: -1, body: stringifyErr(e) };
  }
}

// -----------------------------------------------------------------------------
// Small helpers.
// -----------------------------------------------------------------------------
function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n) + "…" : s;
}

function stringifyErr(e: unknown): string {
  if (e instanceof Error) return `${e.name}: ${e.message}`;
  return String(e);
}

/**
 * Omi real-time conversation assistant — Cloudflare Worker.
 *
 * Omi POSTs live transcript segments to /webhook. When the OTHER person asks a
 * question, we send the recent conversation to Claude Haiku, which drafts the
 * exact words Kevin could say next, then deliver that draft back to the phone
 * via Omi's direct-notification API.
 *
 * Design rules:
 *  - Respond to Omi in <1ms: return 200 {"accepted": true} and do all real work
 *    asynchronously via ctx.waitUntil(). Omi never waits on the AI call.
 *  - A failure anywhere in the async pipeline must never surface to Omi.
 *  - Logs read like a story via short tags: RECV / FILTERED / AI_CALL /
 *    NOTIFIED / ERROR. Secrets are never logged.
 */

export interface Env {
  OMI_SESSIONS: KVNamespace;
  ANTHROPIC_API_KEY: string;
  OMI_APP_ID: string;
  OMI_APP_SECRET: string;
}

// =============================================================================
// KEVIN PROFILE  ── REPLACE THIS ONE CONSTANT to change what the assistant knows.
// This is a temporary short profile. Later, swap the string below for a full
// knowledge-base document (bio, methods, sample answers, do's and don'ts).
// Nothing else in the code needs to change.
// =============================================================================
const KEVIN_PROFILE = `Kevin D. Trice, Ed.D., is a senior education leader with ~15 years of experience as a practitioner, district leader, and researcher. He is a Senior Director at Communities In Schools working on district partnerships, and separately runs LaunchPoint, an education consulting practice. His expertise: MTSS, in-school suspension redesign, implementation science (NIRN Active Implementation Frameworks), school counseling infrastructure, and district–community partnerships.`;

// The full system prompt, assembled from the profile above.
const SYSTEM_PROMPT = `You are Dr. Kevin Trice's silent real-time conversation assistant. The OTHER speaker has just asked Kevin a question in a live, in-person conversation. Write the exact words Kevin can say next.

About Kevin (temporary short profile — will be replaced with a full knowledge base later):
${KEVIN_PROFILE}

Rules:
- Begin with "Say:"
- Answer the question directly in 20–45 words: direct answer first, then one supporting point.
- First-person, confident, warm, practical, executive-level. No jargon dumps.
- Never mention AI or that this is generated.
- Never invent statistics, client names, prices, commitments, or results.
- If key information is missing, instead give Kevin one concise clarifying question to ask, still starting with "Say:".
- If the answer would commit Kevin to something that needs verification, tell him to note it and confirm later.`;

// -----------------------------------------------------------------------------
// Tunables — the knobs you'll most likely want to adjust while tuning.
// -----------------------------------------------------------------------------
const TRANSCRIPT_CAP = 3000; // max chars of rolling transcript kept per session
const COOLDOWN_MS = 20_000; // min gap between notifications for one session
const SESSION_TTL_SECONDS = 2 * 60 * 60; // KV key self-expires after 2 hours
const MIN_QUESTION_WORDS = 4; // below this (and no "?") we wait for more segments
const ANTHROPIC_MODEL = "claude-haiku-4-5";
const ANTHROPIC_MAX_TOKENS = 200;

// Words that, at the START of the other person's utterance, mark a question.
const INTERROGATIVES = new Set([
  "what", "why", "how", "when", "where", "who",
  "can", "could", "would", "should", "do", "does", "is", "are",
]);
// Phrases that mark a question anywhere in the utterance.
const QUESTION_PHRASES = ["tell me", "help me understand", "walk me through"];

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
  last_question_hash: string; // hash of the last question we answered
  last_notified_at: number; // epoch ms of the last notification sent
  uid: string; // captured from the request
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
    : { recent_transcript: "", last_question_hash: "", last_notified_at: 0, uid };
  if (uid) state.uid = uid;

  // Append EVERY new segment (both sides — context needs both) with a label.
  for (const seg of segments) {
    const text = str(seg?.text).trim();
    if (!text) continue;
    const label = seg?.is_user === true ? "Me:" : "Them:";
    state.recent_transcript += (state.recent_transcript ? "\n" : "") + `${label} ${text}`;
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

  // We have a fresh, completed question from the other person → ask Claude.
  console.log(`AI_CALL session=${sessionId} q="${truncate(decision.question, 120)}"`);
  const answer = await callClaude(env, state.recent_transcript, decision.question);

  if (!answer) {
    // AI failed — log and skip. Do NOT retry in a loop. Leave hash/time as-is so
    // the same question can be retried on the next segment for this session.
    console.log(`ERROR ai returned no answer session=${sessionId}`);
    await persist(env, sessionId, state);
    return;
  }

  await notifyOmi(env, state.uid, answer);

  // Update session state AFTER sending (dedup hash + cooldown timestamp).
  state.last_question_hash = decision.hash;
  state.last_notified_at = now;
  await persist(env, sessionId, state);
}

async function persist(env: Env, sessionId: string, state: SessionState): Promise<void> {
  await env.OMI_SESSIONS.put(sessionId, JSON.stringify(state), {
    expirationTtl: SESSION_TTL_SECONDS,
  });
}

// -----------------------------------------------------------------------------
// Decide whether the latest thing the OTHER person said is a question to answer.
// -----------------------------------------------------------------------------
type Decision =
  | { fire: true; question: string; hash: string }
  | { fire: false; reason: string };

function decide(state: SessionState, now: number): Decision {
  // The candidate question = the trailing run of consecutive "Them:" lines
  // (i.e. everything the other person has said since Kevin last spoke). This
  // naturally accumulates a question split across multiple webhook calls.
  const candidate = trailingOtherSpeakerText(state.recent_transcript);
  if (!candidate) return { fire: false, reason: "no-question-from-other-speaker" };

  const shape = looksLikeCompletedQuestion(candidate);
  if (!shape.ok) return { fire: false, reason: shape.reason };

  const hash = hashString(normalizeQuestion(candidate));
  if (hash === state.last_question_hash) return { fire: false, reason: "duplicate-question" };

  if (now - state.last_notified_at < COOLDOWN_MS) return { fire: false, reason: "cooldown" };

  return { fire: true, question: candidate, hash };
}

// Everything the other person has said since Kevin last spoke, joined to one string.
function trailingOtherSpeakerText(transcript: string): string {
  const lines = transcript.split("\n");
  const them: string[] = [];
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i];
    if (line.startsWith("Them:")) {
      them.unshift(line.slice("Them:".length).trim());
    } else {
      // Hit a "Me:" line (Kevin spoke) or a truncated/unlabelled line — stop.
      break;
    }
  }
  return them.join(" ").trim();
}

function looksLikeCompletedQuestion(text: string): { ok: true } | { ok: false; reason: string } {
  const t = text.trim();
  if (!t) return { ok: false, reason: "empty" };

  const words = t.split(/\s+/);
  const first = words[0].toLowerCase().replace(/[^a-z]/g, "");
  const lower = t.toLowerCase();

  const endsWithQuestionMark = t.endsWith("?");
  const startsWithInterrogative = INTERROGATIVES.has(first);
  const containsQuestionPhrase = QUESTION_PHRASES.some((p) => lower.includes(p));

  if (!endsWithQuestionMark && !startsWithInterrogative && !containsQuestionPhrase) {
    return { ok: false, reason: "not-question-shaped" };
  }

  // "Appears finished" guard: if there's no "?" and it's very short, it's
  // probably still mid-sentence — wait for more segments rather than firing.
  if (!endsWithQuestionMark && words.length < MIN_QUESTION_WORDS) {
    return { ok: false, reason: "too-short-and-unfinished" };
  }

  return { ok: true };
}

function normalizeQuestion(text: string): string {
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
// Anthropic Messages API — draft Kevin's next words.
// -----------------------------------------------------------------------------
async function callClaude(
  env: Env,
  transcript: string,
  question: string,
): Promise<string | null> {
  const userMessage =
    `Recent conversation:\n${transcript}\n\n` +
    `Latest question from the other person:\n${question}`;

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
// Omi direct-notification API — deliver the draft to the phone.
// -----------------------------------------------------------------------------
async function notifyOmi(env: Env, uid: string, message: string): Promise<void> {
  if (!uid) {
    console.log("ERROR omi missing uid — cannot deliver notification");
    return;
  }

  const endpoint =
    `https://api.omi.me/v2/integrations/${encodeURIComponent(env.OMI_APP_ID)}` +
    `/notification?uid=${encodeURIComponent(uid)}&message=${encodeURIComponent(message)}`;

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { Authorization: `Bearer ${env.OMI_APP_SECRET}` },
    });

    if (!res.ok) {
      const errBody = await res.text();
      console.log(`ERROR omi status=${res.status} body=${truncate(errBody, 300)}`);
    } else {
      console.log(`NOTIFIED uid=${uid} msg="${truncate(message, 120)}"`);
    }
  } catch (e) {
    console.log(`ERROR omi exception ${stringifyErr(e)}`);
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

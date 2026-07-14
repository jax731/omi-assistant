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
const SYSTEM_PROMPT = `You are Kevin's silent real-time conversation assistant. In a live, in-person conversation, the other person has just asked Kevin a question. Write the exact words Kevin can say next to answer it.

Answer questions on ANY subject — general knowledge, everyday and practical topics, how-to, technical, casual, personal, professional, anything. NEVER say the question is off-topic, NEVER redirect to a preferred subject, and NEVER refuse a harmless question. Just answer whatever was actually asked, helpfully and naturally.

Background on Kevin (use it only when the question is actually about his work; otherwise ignore it):
${KEVIN_PROFILE}

Rules:
- Begin with "Say:"
- Answer the question directly and helpfully. Aim for ~20–45 words: the direct answer first, then at most one supporting point.
- First-person, confident, warm, practical, conversational — natural spoken English, no jargon dumps.
- Never mention AI or that this is generated.
- Don't state specific statistics, names, prices, or commitments as verified fact unless you're sure; if unsure of a specific detail, answer in general terms, or ask one short clarifying question (still starting with "Say:").`;

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
  last_answer?: string; // last draft we sent (handy for inspecting via KV)
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

  // Append every new segment (both sides — context needs both). Omi streams
  // tiny partial fragments (often mid-word), so we MERGE consecutive segments
  // from the same speaker into one line instead of one line per fragment —
  // this reassembles a coherent utterance we can scan for a real question.
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
  state.last_answer = answer;
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
  // Look only at the other person's most recent turn (the last "Them:" line).
  const themTurn = lastSpeakerLine(state.recent_transcript, "Them:");
  if (!themTurn) return { fire: false, reason: "no-other-speaker-utterance" };

  // Extract the most recent COMPLETED question from that turn. "Completed"
  // means it ends in sentence punctuation — so we naturally wait for Omi's
  // stream of fragments to finish the sentence before firing, and we hand
  // Claude one clean question instead of a garbled running blob.
  const question = extractLatestCompletedQuestion(themTurn);
  if (!question) return { fire: false, reason: "no-completed-question-yet" };

  const hash = hashString(normalizeQuestion(question));
  if (hash === state.last_question_hash) return { fire: false, reason: "duplicate-question" };

  if (now - state.last_notified_at < COOLDOWN_MS) return { fire: false, reason: "cooldown" };

  return { fire: true, question, hash };
}

// The other person's most recent continuous turn (the last line with `prefix`).
function lastSpeakerLine(transcript: string, prefix: string): string {
  const lines = transcript.split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    if (lines[i].startsWith(prefix)) return lines[i].slice(prefix.length).trim();
  }
  return "";
}

// Find the most recent COMPLETED, question-shaped sentence in `text`.
// Only sentences that end in `.`/`?`/`!` are considered — an in-progress
// fragment with no terminator yet is ignored, so we wait for the speaker to
// finish. Returns null if there's no completed question.
function extractLatestCompletedQuestion(text: string): string | null {
  const sentences = text.match(/[^.?!]+[.?!]+/g);
  if (!sentences) return null;
  for (let i = sentences.length - 1; i >= 0; i--) {
    const s = sentences[i].trim();
    if (isQuestionShaped(s)) return s;
  }
  return null;
}

function isQuestionShaped(sentence: string): boolean {
  const t = sentence.trim();
  if (!t) return false;
  if (t.endsWith("?")) return true; // clearest signal — Omi punctuates questions

  // No "?" — accept only if it clearly opens like a question and is long enough
  // to trust (guards against stray one-word fragments like "Is." / "Do.").
  const words = t.split(/\s+/);
  if (words.length < MIN_QUESTION_WORDS) return false;
  const first = words[0].toLowerCase().replace(/[^a-z]/g, "");
  const lower = t.toLowerCase();
  if (INTERROGATIVES.has(first)) return true;
  if (QUESTION_PHRASES.some((p) => lower.includes(p))) return true;
  return false;
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

    const respBody = await res.text();
    if (res.ok) {
      console.log(
        `NOTIFIED uid=${uid} status=${res.status} body=${truncate(respBody, 300)} msg="${truncate(message, 120)}"`,
      );
    } else {
      console.log(`ERROR omi status=${res.status} body=${truncate(respBody, 300)}`);
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

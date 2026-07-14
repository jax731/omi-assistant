/**
 * Omi real-time conversation assistant — Cloudflare Worker.
 *
 * Omi POSTs live transcript segments to /webhook. The Worker forwards them to a
 * per-session Durable Object that acts as a ~2-second PAUSE DETECTOR: each new
 * segment resets a short alarm, and when the speaker stops talking the alarm
 * fires and we evaluate the whole recent conversation once. This is reliable
 * even when Omi never adds punctuation, and it never strands a question waiting
 * for a webhook that won't come.
 *
 * On evaluation, Claude Haiku finds the most recent unanswered question from the
 * other person and drafts what Kevin could say (or "SKIP"). If the other person
 * spoke another language, it also translates what they said. The draft is pushed
 * to Kevin's phone via Pushover.
 */

export interface Env {
  OMI_SESSIONS: KVNamespace;
  SESSION_DO: DurableObjectNamespace;
  ANTHROPIC_API_KEY: string;
  PUSHOVER_APP_TOKEN: string;
  PUSHOVER_USER_KEY: string;
  DEEPGRAM_API_KEY: string; // for the /audio real-time path (prototype)
}

// =============================================================================
// KEVIN PROFILE  ── REPLACE THIS ONE CONSTANT to change what the assistant knows.
// Swap the string for a full knowledge-base document later; nothing else changes.
// =============================================================================
const KEVIN_PROFILE = `Kevin D. Trice, Ed.D., is a senior education leader with ~15 years of experience as a practitioner, district leader, and researcher. He is a Senior Director at Communities In Schools working on district partnerships, and separately runs LaunchPoint, an education consulting practice. His expertise: MTSS, in-school suspension redesign, implementation science (NIRN Active Implementation Frameworks), school counseling infrastructure, and district–community partnerships.`;

const SYSTEM_PROMPT = `You are Kevin's silent real-time conversation assistant. You quietly follow Kevin's live, in-person conversations and suggest what he could say next — delivered to his phone. You are PROACTIVE: you don't only wait for a direct question; you offer something to say whenever it would genuinely help Kevin in the moment.

Offer a suggestion (starting with "Say:") when:
- Someone asked a question (any subject) — provide the answer Kevin could give.
- Someone shared something Kevin could warmly respond to, build on, ask a good follow-up about, or acknowledge.
- There's a natural opening for Kevin to add a useful insight or a thoughtful reply.

Each line is labelled by speaker: "Them:" is the other person and "Me:" is Kevin. Focus on what the OTHER person ("Them:") asks or says — that's what Kevin needs help responding to. Speaker labels are usually right but can occasionally be off, so if there's clearly a question in the air, help even if it looks mislabelled. Lean toward being helpful.

Reply with EXACTLY "SKIP" (nothing else) only when there's genuinely nothing worth saying — pure filler ("okay", "yeah"), logistics, half-finished thoughts, or unclear audio.

When you do suggest something:
- LANGUAGE HANDLING:
  - If the other person spoke English, just give the "Say:" line in English.
  - If they spoke ANOTHER language, Kevin must reply IN THAT LANGUAGE. Output exactly these three lines and nothing else:
    They said: "<their words translated into English>"
    Say: <the reply written in THEIR language — this is what Kevin reads aloud to them, so it must NOT be in English>
    Meaning: <the English meaning of that reply>
    Example (the other person spoke Spanish):
    They said: "How was your weekend?"
    Say: Estuvo muy bien, gracias. Pasé tiempo con mi familia.
    Meaning: It was great, thanks — I spent time with my family.
- Keep the "Say:" reply to about 25–40 words: warm and genuinely helpful with a bit of real substance, but tight and fast to say aloud — not a speech.
- First person, confident and friendly, natural spoken language.
- Answer questions on ANY subject; never refuse a harmless one.
- Never mention AI or that this is generated.
- Don't invent specific statistics, names, prices, or commitments as fact; if unsure, speak in general terms or offer a warm clarifying question.

Background on Kevin (use it when the conversation relates to his work; otherwise ignore it):
${KEVIN_PROFILE}`;

// -----------------------------------------------------------------------------
// Tunables.
// -----------------------------------------------------------------------------
const DEBOUNCE_MS = 2_000; // evaluate this long after the last segment (a pause)
const TRANSCRIPT_CAP = 3000; // max chars of rolling transcript kept per session
const COOLDOWN_MS = 10_000; // min gap between suggestions for one session
const EVAL_DEDUP_MS = 30_000; // don't re-evaluate identical context within this window
const RATE_LIMIT_BACKOFF_MS = 5 * 60 * 1000; // pause after a 429 from the push provider
const ANTHROPIC_MODEL = "claude-haiku-4-5";
const ANTHROPIC_MAX_TOKENS = 220; // room for a translation line + a warm reply
const CONTEXT_CHARS = 1200; // chars of recent transcript sent to Claude

// /audio (real-time via Deepgram) tunables.
const DEEPGRAM_UTTERANCE_END_MS = 1000; // Deepgram signals "speaker paused" after this silence
const AUDIO_IDLE_MS = 20_000; // close the Deepgram connection after this much silence

// Per-isolate counter: log the full raw payload for the first few requests.
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
  last_eval_hash: string; // hash of the last context we evaluated
  last_eval_at: number; // epoch ms of the last evaluation
  last_notified_at: number; // epoch ms of the last suggestion actually sent
  uid: string; // captured from the request
  last_answer?: string; // last draft we sent (handy for inspecting via KV)
  last_delivery_status?: number; // HTTP status from the push provider
  last_delivery_body?: string; // push provider response body
  backoff_until?: number; // pause firing until this epoch ms (set after a 429)
}

// -----------------------------------------------------------------------------
// Worker entrypoint: ack Omi instantly, forward segments to the session's DO.
// -----------------------------------------------------------------------------
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    // Debug: inspect a session's state (held in the Durable Object, not KV).
    if (request.method === "GET" && url.pathname === "/debug") {
      const session = url.searchParams.get("session") || "";
      if (!session) return new Response("missing ?session", { status: 400 });
      const stub = env.SESSION_DO.get(env.SESSION_DO.idFromName(session));
      return stub.fetch("https://session.do/debug");
    }

    if (request.method === "POST" && url.pathname === "/webhook") {
      const rawText = await request.text();

      if (rawLogCount < RAW_LOG_LIMIT) {
        rawLogCount++;
        console.log(`RECV raw (${rawLogCount}/${RAW_LOG_LIMIT}) query=${url.search} body=${rawText}`);
      } else {
        console.log(`RECV query=${url.search} bytes=${rawText.length}`);
      }

      let body: Record<string, unknown> = {};
      try {
        body = rawText ? (JSON.parse(rawText) as Record<string, unknown>) : {};
      } catch {
        console.log(`ERROR malformed JSON body=${rawText.slice(0, 500)}`);
        return accepted();
      }

      const uid = str(url.searchParams.get("uid")) || str(body.uid) || "";
      const sessionId =
        str(url.searchParams.get("session_id")) || str(body.session_id) || uid || "unknown";
      const segments: Segment[] = Array.isArray(body.segments) ? (body.segments as Segment[]) : [];

      // Forward to the per-session Durable Object (the debounce timer lives there).
      const stub = env.SESSION_DO.get(env.SESSION_DO.idFromName(sessionId));
      ctx.waitUntil(
        stub
          .fetch("https://session.do/ingest", {
            method: "POST",
            body: JSON.stringify({ uid, sessionId, segments }),
          })
          .catch((e) => console.log(`ERROR forward-to-do ${stringifyErr(e)}`)),
      );

      return accepted();
    }

    // Real-time audio path (prototype): Omi POSTs raw PCM16 chunks here.
    if (request.method === "POST" && url.pathname === "/audio") {
      const uid = str(url.searchParams.get("uid")) || "unknown";
      const sampleRate = url.searchParams.get("sample_rate") || "16000";
      const bytes = await request.arrayBuffer();
      const stub = env.SESSION_DO.get(env.SESSION_DO.idFromName(uid));
      ctx.waitUntil(
        stub
          .fetch(
            `https://session.do/audio?uid=${encodeURIComponent(uid)}&sample_rate=${sampleRate}`,
            { method: "POST", body: bytes },
          )
          .catch((e) => console.log(`ERROR forward-audio ${stringifyErr(e)}`)),
      );
      return new Response("ok", { status: 200 }); // 200 quickly, as Omi requires
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
// Durable Object: one instance per session. Accumulates segments and, ~2s after
// the last one (a pause), fires alarm() to evaluate once.
// -----------------------------------------------------------------------------
export class SessionDO {
  // In-memory audio-bridge state (re-established on demand after eviction).
  private dgWs?: WebSocket;
  private dgConnecting = false;
  private audioQueue: ArrayBuffer[] = [];
  private pendingTranscript = "";
  private audioSessionId = "";
  private audioUid = "";

  constructor(
    private state: DurableObjectState,
    private env: Env,
  ) {}

  // State lives in the DO's own storage (NOT KV) — built for frequent per-request
  // writes, so we don't burn Cloudflare's small daily KV write quota.
  private async load(uid: string): Promise<SessionState> {
    const s = await this.state.storage.get<SessionState>("state");
    return (
      s ?? { recent_transcript: "", last_eval_hash: "", last_eval_at: 0, last_notified_at: 0, uid }
    );
  }
  private async save(state: SessionState): Promise<void> {
    await this.state.storage.put("state", state);
  }

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (path === "/debug") {
      const state = (await this.state.storage.get<SessionState>("state")) ?? null;
      const sessionId = (await this.state.storage.get<string>("sessionId")) ?? "";
      return Response.json({ sessionId, state });
    }

    // --- Real-time audio chunk (prototype) ---
    if (path === "/audio") {
      const u = new URL(request.url);
      const uid = u.searchParams.get("uid") || "";
      const sampleRate = parseInt(u.searchParams.get("sample_rate") || "16000", 10) || 16000;
      const bytes = await request.arrayBuffer();
      await this.state.storage.put("sessionId", uid);
      await this.handleAudio(uid, sampleRate, bytes);
      return new Response("ok");
    }

    // --- Transcript path (Omi's default: it POSTs already-transcribed segments) ---
    try {
      const { uid, sessionId, segments } = (await request.json()) as {
        uid: string;
        sessionId: string;
        segments: Segment[];
      };
      await this.state.storage.put("sessionId", sessionId);

      const s = await this.load(uid);
      if (uid) s.uid = uid;
      mergeSegments(s, segments);
      await this.save(s);

      // (Re)set the 2s debounce alarm — each new segment pushes it back.
      await this.state.storage.put("alarmMode", "debounce");
      await this.state.storage.setAlarm(Date.now() + DEBOUNCE_MS);
      console.log(`INGEST session=${sessionId} segs=${segments.length}`);
    } catch (e) {
      console.log(`ERROR do.fetch ${stringifyErr(e)}`);
    }
    return new Response("ok");
  }

  async alarm(): Promise<void> {
    const mode = (await this.state.storage.get<string>("alarmMode")) ?? "debounce";
    const sessionId = (await this.state.storage.get<string>("sessionId")) ?? "";
    try {
      if (mode === "idle") {
        // Audio path went quiet — flush anything pending and close Deepgram.
        await this.finalizeUtterance();
        if (this.dgWs) {
          try {
            this.dgWs.close();
          } catch {
            /* ignore */
          }
          this.dgWs = undefined;
        }
        console.log(`AUDIO_IDLE_CLOSE session=${sessionId}`);
        return;
      }
      // Transcript path: the speaker paused ~DEBOUNCE_MS ago → evaluate.
      const s = await this.load("");
      await evaluateAndDeliver(this.env, sessionId, s);
      await this.save(s);
    } catch (e) {
      console.log(`ERROR alarm ${stringifyErr(e)}`);
    }
  }

  // ---- Audio bridge -----------------------------------------------------------
  private async handleAudio(uid: string, sampleRate: number, bytes: ArrayBuffer): Promise<void> {
    this.audioSessionId = uid;
    this.audioUid = uid;
    await this.ensureDeepgram(sampleRate);
    if (this.dgWs) {
      try {
        this.dgWs.send(bytes);
      } catch (e) {
        console.log(`ERROR dg-send ${stringifyErr(e)}`);
        this.dgWs = undefined;
        this.audioQueue.push(bytes);
      }
    } else {
      this.audioQueue.push(bytes);
      if (this.audioQueue.length > 300) this.audioQueue.shift(); // cap the buffer
    }
    // Idle-close timer, pushed back on every chunk.
    await this.state.storage.put("alarmMode", "idle");
    await this.state.storage.setAlarm(Date.now() + AUDIO_IDLE_MS);
  }

  private async ensureDeepgram(sampleRate: number): Promise<void> {
    if (this.dgWs || this.dgConnecting) return;
    this.dgConnecting = true;
    try {
      const params = new URLSearchParams({
        encoding: "linear16",
        sample_rate: String(sampleRate),
        channels: "1",
        model: "nova-3",
        language: "multi", // English + Spanish (code-switching)
        smart_format: "true",
        interim_results: "true",
        utterance_end_ms: String(DEEPGRAM_UTTERANCE_END_MS),
        vad_events: "true",
      });
      const resp = await fetch(`https://api.deepgram.com/v1/listen?${params.toString()}`, {
        headers: { Upgrade: "websocket", Authorization: `Token ${this.env.DEEPGRAM_API_KEY}` },
      });
      const ws = resp.webSocket;
      if (!ws) {
        console.log(`ERROR deepgram upgrade failed status=${resp.status}`);
        this.dgConnecting = false;
        return;
      }
      ws.accept();
      this.dgWs = ws;
      this.dgConnecting = false;
      console.log(`DG_OPEN session=${this.audioSessionId}`);
      for (const chunk of this.audioQueue) {
        try {
          ws.send(chunk);
        } catch {
          /* ignore */
        }
      }
      this.audioQueue = [];
      ws.addEventListener("message", (event: MessageEvent) => {
        const data = typeof event.data === "string" ? event.data : "";
        this.onDeepgramMessage(data).catch((e) => console.log(`ERROR dg-msg ${stringifyErr(e)}`));
      });
      ws.addEventListener("close", () => {
        console.log("DG_CLOSE");
        this.dgWs = undefined;
      });
      ws.addEventListener("error", () => {
        console.log("DG_ERROR");
        this.dgWs = undefined;
      });
    } catch (e) {
      console.log(`ERROR deepgram connect ${stringifyErr(e)}`);
      this.dgConnecting = false;
    }
  }

  private async onDeepgramMessage(data: string): Promise<void> {
    if (!data) return;
    let msg: { type?: string; is_final?: boolean; channel?: { alternatives?: Array<{ transcript?: string }> } };
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }
    if (msg.type === "Results") {
      const text = (msg.channel?.alternatives?.[0]?.transcript || "").trim();
      if (text && msg.is_final) {
        this.pendingTranscript += (this.pendingTranscript ? " " : "") + text;
      }
    } else if (msg.type === "UtteranceEnd") {
      await this.finalizeUtterance();
    }
  }

  // A full utterance finished (Deepgram detected a pause) → append + evaluate.
  private async finalizeUtterance(): Promise<void> {
    const utter = this.pendingTranscript.trim();
    this.pendingTranscript = "";
    if (!utter) return;
    console.log(`DG_UTTERANCE session=${this.audioSessionId} text="${truncate(utter, 120)}"`);
    const s = await this.load(this.audioUid);
    if (this.audioUid) s.uid = this.audioUid;
    s.recent_transcript += (s.recent_transcript ? "\n" : "") + `Them: ${utter}`;
    if (s.recent_transcript.length > TRANSCRIPT_CAP) {
      s.recent_transcript = s.recent_transcript.slice(-TRANSCRIPT_CAP);
    }
    await evaluateAndDeliver(this.env, this.audioSessionId, s);
    await this.save(s);
  }
}

// Merge Omi's fragments into speaker-labelled lines (consecutive same-speaker
// fragments join into one line — Omi streams tiny, often mid-word pieces).
function mergeSegments(state: SessionState, segments: Segment[]): void {
  for (const seg of segments) {
    const text = str(seg?.text).trim();
    if (!text) continue;
    const prefix = seg?.is_user === true ? "Me:" : "Them:";
    const lines = state.recent_transcript ? state.recent_transcript.split("\n") : [];
    if (lines.length > 0 && lines[lines.length - 1].startsWith(prefix)) {
      lines[lines.length - 1] += ` ${text}`;
    } else {
      lines.push(`${prefix} ${text}`);
    }
    state.recent_transcript = lines.join("\n");
  }
  if (state.recent_transcript.length > TRANSCRIPT_CAP) {
    state.recent_transcript = state.recent_transcript.slice(-TRANSCRIPT_CAP);
  }
}

// -----------------------------------------------------------------------------
// The evaluation, run on the debounce alarm (speaker paused).
// -----------------------------------------------------------------------------
async function evaluateAndDeliver(env: Env, sessionId: string, state: SessionState): Promise<void> {
  const now = Date.now();

  const recent = state.recent_transcript.slice(-CONTEXT_CHARS).trim();
  if (!recent) return;

  if (state.backoff_until && now < state.backoff_until) {
    console.log(`FILTERED rate-limit-backoff session=${sessionId}`);
    return;
  }
  if (now - state.last_notified_at < COOLDOWN_MS) {
    console.log(`FILTERED cooldown session=${sessionId}`);
    return;
  }

  // Dedup on the whole recent context so an unchanged transcript isn't re-run.
  const hash = hashString(normalizeText(recent));
  if (hash === state.last_eval_hash && now - (state.last_eval_at ?? 0) < EVAL_DEDUP_MS) {
    console.log(`FILTERED already-evaluated session=${sessionId}`);
    return;
  }
  state.last_eval_hash = hash;
  state.last_eval_at = now;

  console.log(`AI_CALL session=${sessionId} chars=${recent.length}`);
  const t0 = Date.now();
  const answer = await callClaude(env, state.recent_transcript);
  console.log(`AI_DONE session=${sessionId} ms=${Date.now() - t0} ok=${answer !== null}`);

  if (!answer) {
    console.log(`ERROR ai returned no answer session=${sessionId}`);
    return;
  }
  if (answer.trim().toUpperCase().startsWith("SKIP")) {
    console.log(`SKIPPED session=${sessionId}`);
    return;
  }

  const delivery = await notifyPushover(env, answer);
  state.last_answer = answer;
  state.last_delivery_status = delivery.status;
  state.last_delivery_body = delivery.body;

  const delivered = delivery.status >= 200 && delivery.status < 300;
  if (delivered) {
    state.last_notified_at = now;
    state.backoff_until = 0;
    state.recent_transcript = ""; // clean slate for the next moment
  } else if (delivery.status === 429) {
    state.backoff_until = now + RATE_LIMIT_BACKOFF_MS;
    console.log(`RATE_LIMITED session=${sessionId} backoff_until=${now + RATE_LIMIT_BACKOFF_MS}`);
  }
}

function normalizeText(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

// djb2 — small deterministic hash, plenty for dedup.
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
  const context = transcript.slice(-CONTEXT_CHARS);
  const userMessage =
    `Here is the recent conversation Kevin is in ("Them:" is the other person, "Me:" is Kevin; most recent last):\n${context}\n\n` +
    `Find the most recent question or clear request from the other person that Kevin hasn't answered yet — it may NOT be the very last line — and write Kevin's reply to it, starting with "Say:". If they spoke another language, translate what they said first (see the rules). If there's genuinely nothing that needs a response, reply with exactly SKIP.`;

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

# Omi Conversation Assistant (Cloudflare Worker)

Real-time conversation assistant for the Omi wearable. Omi POSTs live transcript
segments to `/webhook`; when the other person asks a question, the Worker asks
Claude Haiku to draft the exact words Kevin could say next, and delivers that
draft to the phone via Omi's direct-notification API.

## Endpoints

- `POST /webhook?uid=...&session_id=...` — receives Omi transcript payloads.
- `GET /health` — returns `ok`.

## Deploy

```bash
npm install
npx wrangler login                                  # opens a browser
npx wrangler kv namespace create OMI_SESSIONS       # copy the id into wrangler.toml
npx wrangler secret put ANTHROPIC_API_KEY           # paste when prompted
npx wrangler secret put OMI_APP_ID
npx wrangler secret put OMI_APP_SECRET
npx wrangler deploy                                 # prints the public URL
```

The webhook URL to paste into Omi is:
`https://omi-assistant.<your-subdomain>.workers.dev/webhook`

## Live testing

```bash
npx wrangler tail            # streams logs: RECV / FILTERED / AI_CALL / NOTIFIED / ERROR
```

## Tuning

- **What the assistant knows:** replace the `KEVIN_PROFILE` constant near the top
  of `src/index.ts`. It's clearly marked and is the only thing you need to edit
  to swap the short bio for a full knowledge-base document.
- **Behaviour knobs** (also near the top of `src/index.ts`): `COOLDOWN_MS`,
  `TRANSCRIPT_CAP`, `MIN_QUESTION_WORDS`, `ANTHROPIC_MAX_TOKENS`, the
  `INTERROGATIVES` / `QUESTION_PHRASES` sets, and the system-prompt rules.

After any change: `npx wrangler deploy`.

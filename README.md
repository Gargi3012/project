# Project 2 — Premium Low-Latency Real-Time Voice Pipeline

Stack: **Pipecat** + **Deepgram (STT)** + **Groq (LLM)** + **ElevenLabs (TTS)**, over
**LiveKit** (WebRTC) and **Twilio** (telephony).

```
[User Voice] --(WebRTC/LiveKit or Twilio SIP)--> Deepgram STT --> Groq LLM --> ElevenLabs TTS --(audio)--> [User Ear]
```

## 0. What changed in this version (verified against real pipecat-ai 0.0.55)

Every import and constructor call in `app/` was tested against an actual
installed `pipecat-ai==0.0.55` in a clean environment. Three real bugs were
found and fixed:

1. **`DeepgramSTTService(live_options=...)`** needs an actual `LiveOptions`
   object (`from deepgram import LiveOptions`), not a plain dict — a dict
   would raise a `TypeError` at startup.
2. **`ElevenLabsTTSService.InputParams(optimize_streaming_latency=...)`**
   must be a string (`"4"`), not an int (`4`).
3. **`LiveKitTransport` has no `api_key`/`api_secret` parameters.** It needs
   an already-signed JWT `token`. `app/livekit_bot.py` now generates this
   token itself using `livekit.api.AccessToken`.
4. **`PipelineRunner(handle_sigint=True)` (the default) crashes on Windows**
   — it calls `asyncio.loop.add_signal_handler`, which raises
   `NotImplementedError` on Windows's event loop. Both `livekit_bot.py` and
   `twilio_bot.py` now explicitly pass `handle_sigint=False`.
5. **VAD wasn't actually being used** — both transports need
   `vad_enabled=True` set alongside `vad_analyzer=...`; passing only the
   analyzer silently does nothing.

## 1. Setup

```bash
cd project2-voice-pipeline
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# now open .env and paste your Groq / Deepgram / ElevenLabs / LiveKit / Twilio keys
```

> Note: `pipecat-ai` ships new releases often and occasionally moves import
> paths (e.g. `pipecat.services.deepgram` vs `pipecat.services.deepgram.stt`).
> If you hit an `ImportError`, run `pip show pipecat-ai` to check your
> installed version and check `python -c "import pipecat; print(pipecat.__file__)"`
> against the installed package's folder structure — the fix is almost always
> a one-line import path change in `app/pipeline.py`.

## 2. Folder structure

```
project2-voice-pipeline/
├── .env.example          <- copy to .env, paste keys here
├── requirements.txt
├── app/
│   ├── config.py         <- Pillar 1: single source of truth for all env vars
│   ├── prompts.py         <- Pillar 3: system prompt, tuned for short/fast TTS-friendly output
│   ├── pipeline.py        <- Pillar 1 + 4: shared STT->LLM->TTS pipeline + interruption config
│   ├── metrics.py          <- Latency diagnostic logger (stt_to_llm_ms, llm_to_tts_ms, total)
│   ├── livekit_bot.py      <- Pillar 2: WebRTC entrypoint (browser mic demo)
│   └── twilio_bot.py       <- Pillar 2: Telephony entrypoint (phone calls in)
├── run_livekit.py
└── run_twilio.py
```

## 3. Running the WebRTC (LiveKit) demo — Week 4 browser mic demo

1. Create a LiveKit room (via LiveKit Cloud dashboard, or `lk room create`).
2. Set `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` in `.env`.
3. Start the bot:
   ```bash
   python run_livekit.py --room voice-agent-room --call-id demo-001
   ```
4. Join the same room from a browser using any LiveKit web client
   (LiveKit's `meet` sample app, or your own React widget using
   `livekit-client`). Speak — the bot listens, thinks, and replies.

## 4. Running the Twilio (telephony) gateway

1. Set `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER` in `.env`.
2. Expose your local server publicly (dev only): `ngrok http 8765`
   Set `PUBLIC_BASE_URL=https://<your-ngrok-domain>` in `.env`.
3. Start the server:
   ```bash
   python run_twilio.py
   ```
4. In the Twilio console, set your phone number's **"A Call Comes In"**
   webhook to: `https://<your-ngrok-domain>/twilio/incoming` (HTTP POST).
5. Call the Twilio number — you should hear the agent answer.

## 4b. Making an OUTBOUND call (you call someone, instead of waiting)

Keep `run_twilio.py` and `ngrok http 8765` both running (same as section 4),
then in a **third** terminal (with `venv` activated):

```bash
python outbound_call.py --to +91XXXXXXXXXX
```

This tells Twilio to ring that number. The moment it's answered, Twilio
fetches the exact same `/twilio/incoming` webhook used for inbound calls, so
the agent connects the same way — you'll hear it start talking.

For a personalised outbound qualification call (Team A integration):
```bash
python outbound_call.py --to +91XXXXXXXXXX --company-context "Company: Acme Corp. Industry: SaaS. Pain point: slow onboarding."
```

## 5. Interruption handling (Pillar 4)

Interruption is controlled in two places, working together:

- `PipelineParams(allow_interruptions=True)` in `app/pipeline.py` — tells
  Pipecat's context aggregator to cut the assistant's turn short the moment
  new user speech is confirmed.
- `SileroVADAnalyzer` in `build_vad_analyzer()` — detects the user starting
  to talk. Tuned with `start_secs=0.2` / `stop_secs=0.8` / `min_volume=0.6`
  so short coughs or stutters don't falsely trigger a full turn (the
  "Guardrail Shield" requirement), while genuine speech still cuts in fast.

## 6. Latency diagnostics (Key Deliverable)

`app/metrics.py`'s `LatencyLoggerProcessor` sits inside the pipeline and logs,
per turn:

```
[call-id] stt_to_llm_ms=180
[call-id] llm_to_tts_ms=95
[call-id] stt_to_tts_total_ms=275
```

Pipe these logs into any dashboard (Grafana/Datadog) to track your <1.2s
round-trip target across real calls.

## 7. Team A × Team B Integration (Final 2 Weeks)

`build_pipeline_task()` and `build_llm_context()` in `app/pipeline.py` both
accept a `company_context: str` argument. Wire Team A's extracted B2B record
into this field before starting the call:

```python
company_context = f"""
Company: {record['company_name']}
Industry: {record['industry']}
Known pain point: {record['pain_point']}
"""

task = build_pipeline_task(transport, call_id=lead_id, company_context=company_context)
```

This gets injected into the system prompt (see `app/prompts.py`) so the
agent runs a personalised outbound qualification call without ever reading
the record out verbatim.

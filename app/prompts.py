"""
Pillar 3 — Text Generation & Context Layer.

Prompt is deliberately tuned for LOW LATENCY:
- Short sentences -> fewer tokens for ElevenLabs to synthesize -> faster
  time-to-first-audio-byte.
- No markdown, no lists, no emojis (TTS reads them out badly and they add
  synthesis latency for no benefit).
"""

BASE_SYSTEM_PROMPT = """You are a warm, professional voice assistant speaking to someone on a live call.

Rules you must always follow:
- Speak in short sentences. Never more than 2 sentences per turn unless asked for detail.
- Never use markdown, bullet points, emojis, or asterisks. This is spoken audio, not text.
- Get to the point immediately. No filler like "great question" or "I'd be happy to help".
- If interrupted, stop your point immediately and respond to what the user just said.
- If you don't understand the user due to noise or a cut-off word, briefly ask them to repeat
  instead of guessing.
- Numbers, dates, and prices should be said the way a human would say them out loud.
"""


def build_system_prompt(company_context: str | None = None) -> str:
    """
    Builds the final system prompt.

    `company_context` is injected during the Team A <-> Team B integration phase:
    Team A's scraped B2B record (company name, industry, pain points, etc.) gets
    dropped in here so the agent runs a personalised outbound qualification call.
    """
    if not company_context:
        return BASE_SYSTEM_PROMPT

    return (
        f"{BASE_SYSTEM_PROMPT}\n\n"
        f"Context for this specific call (from our lead database):\n{company_context}\n\n"
        "Use this context naturally in conversation. Do not read it out verbatim or "
        "mention that it came from a database."
    )

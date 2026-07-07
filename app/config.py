"""
Central configuration for the voice pipeline.
Loads all secrets/settings from .env (see .env.example).
Every other module imports `settings` from here — no module reads
os.environ directly. This keeps env-handling in exactly one place.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Pillar 3 — Groq LLM
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"

    # Pillar 2 — Deepgram STT
    deepgram_api_key: str = ""
    deepgram_model: str = "nova-2"

    # Pillar 4 — ElevenLabs TTS
    elevenlabs_api_key: str = ""
    elevenlabs_voice_id: str = ""
    elevenlabs_model: str = "eleven_flash_v2_5"

    # Pillar 2 — LiveKit (WebRTC)
    livekit_url: str = ""
    livekit_api_key: str = ""
    livekit_api_secret: str = ""
    livekit_room_name: str = "voice-agent-room"

    # Pillar 2 — Twilio (Telephony / SIP)
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_phone_number: str = ""

    # Server
    host: str = "0.0.0.0"
    port: int = 8765
    public_base_url: str = ""

    log_level: str = "INFO"


settings = Settings()

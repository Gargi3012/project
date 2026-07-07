"""
Pillar 2 — Audio Ingestion & Transport (WebRTC Lead)

Runs the voice agent as a participant inside a LiveKit room. A browser client
(Daily/LiveKit prebuilt UI, or your own React mic widget) joins the same room
and talks to this bot in real time.

Usage:
    python run_livekit.py --room voice-agent-room --call-id demo-001

For the Team A x Team B integration phase, pass --company-context with the
extracted B2B record so the agent opens with a personalised pitch.

NOTE: LiveKitTransport does NOT accept api_key/api_secret directly — it needs
an already-signed JWT `token`. We generate that token here using livekit-api's
AccessToken, which is the correct/verified approach for pipecat-ai 0.0.55.
"""

import argparse
import asyncio

from livekit import api
from loguru import logger
from pipecat.pipeline.runner import PipelineRunner
from pipecat.transports.services.livekit import LiveKitParams, LiveKitTransport

from app.config import settings
from app.pipeline import build_pipeline_task, build_vad_analyzer


def _generate_livekit_token(room_name: str, identity: str = "voice-agent-bot") -> str:
    """Signs a short-lived JWT so our bot process can join the given room."""
    token = (
        api.AccessToken(settings.livekit_api_key, settings.livekit_api_secret)
        .with_identity(identity)
        .with_name("Voice Agent")
        .with_grants(api.VideoGrants(room_join=True, room=room_name))
        .to_jwt()
    )
    return token


async def run_bot(room_name: str, call_id: str, company_context: str | None):
    token = _generate_livekit_token(room_name)

    transport = LiveKitTransport(
        url=settings.livekit_url,
        token=token,
        room_name=room_name,
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_enabled=True,           # required for vad_analyzer below to actually run
            vad_analyzer=build_vad_analyzer(),
            # Bot's own TTS output can be interrupted the instant user audio
            # crosses the VAD threshold above.
            audio_out_is_live=True,
        ),
    )

    task = build_pipeline_task(transport, call_id=call_id, company_context=company_context)

    @transport.event_handler("on_participant_connected")
    async def on_participant_connected(_transport, participant):
        logger.info(f"[{call_id}] Participant joined: {participant.identity}")

    @transport.event_handler("on_participant_disconnected")
    async def on_participant_disconnected(_transport, participant):
        logger.info(f"[{call_id}] Participant left: {participant.identity} — ending call")
        await task.cancel()

    # handle_sigint=False: asyncio's add_signal_handler (used when True) is
    # NOT implemented on Windows and raises NotImplementedError there. False
    # works identically on Windows/macOS/Linux — Ctrl+C still works via the
    # default KeyboardInterrupt path, we just don't get graceful shutdown.
    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)


def main():
    parser = argparse.ArgumentParser(description="Run the LiveKit voice agent bot")
    parser.add_argument("--room", default=settings.livekit_room_name, help="LiveKit room name")
    parser.add_argument("--call-id", default="livekit-dev-call", help="Identifier for logs/metrics")
    parser.add_argument("--company-context", default=None, help="Injected B2B record text (Team A integration)")
    args = parser.parse_args()

    asyncio.run(run_bot(args.room, args.call_id, args.company_context))


if __name__ == "__main__":
    main()
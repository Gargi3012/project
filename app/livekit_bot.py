"""
Pillar 2 — Audio Ingestion & Transport (WebRTC Lead)

Runs the voice agent as a participant inside a LiveKit room. A browser client
(Daily/LiveKit prebuilt UI, or your own React mic widget) joins the same room
and talks to this bot in real time.

Usage:
    python -m app.livekit_bot --room voice-agent-room --call-id demo-001

For the Team A x Team B integration phase, pass --company-context with the
extracted B2B record so the agent opens with a personalised pitch.
"""

import argparse
import asyncio

from loguru import logger
from pipecat.pipeline.runner import PipelineRunner
from pipecat.transports.services.livekit import LiveKitParams, LiveKitTransport

from app.config import settings
from app.pipeline import build_pipeline_task, build_vad_analyzer


async def run_bot(room_name: str, call_id: str, company_context: str | None):
    transport = LiveKitTransport(
        url=settings.livekit_url,
        token=None,  # let the transport mint a bot token via api_key/api_secret below
        room_name=room_name,
        api_key=settings.livekit_api_key,
        api_secret=settings.livekit_api_secret,
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
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

    runner = PipelineRunner()
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

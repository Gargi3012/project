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
            vad_enabled=False,          # FIX: Disable VAD to prevent pipecat from calling finalize() on Deepgram
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

    runner = PipelineRunner(handle_sigint=False)
    try:
        await runner.run(task)
    except asyncio.CancelledError:
        pass
    finally:
        await task.cancel()


def main():
    parser = argparse.ArgumentParser(description="Run the LiveKit voice agent bot")
    parser.add_argument("--room", default=settings.livekit_room_name, help="LiveKit room name")
    parser.add_argument("--call-id", default="livekit-dev-call", help="Identifier for logs/metrics")
    parser.add_argument("--company-context", default=None, help="Injected B2B record text (Team A integration)")
    args = parser.parse_args()

    asyncio.run(run_bot(args.room, args.call_id, args.company_context))


if __name__ == "__main__":
    main()
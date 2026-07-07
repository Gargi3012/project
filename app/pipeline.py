"""
Pillar 1 — Pipecat Orchestration & State Management (Core Infrastructure)
Pillar 4 — Premium Audio Generation & Interruption (TTS & Performance Lead)

This module is transport-agnostic. It builds:

    [Transport IN] -> Deepgram STT -> Context Aggregator (user)
        -> Groq LLM -> ElevenLabs TTS -> [Transport OUT] -> Context Aggregator (assistant)

Both livekit_bot.py and twilio_bot.py call `build_pipeline_task(transport, ...)`
with their own transport instance. This is the single source of truth for the
conversation loop, so any latency/interruption fix here benefits both channels.
"""

from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.deepgram import DeepgramSTTService
from pipecat.services.elevenlabs import ElevenLabsTTSService
from pipecat.services.groq import GroqLLMService

from app.config import settings
from app.metrics import LatencyLoggerProcessor
from app.prompts import build_system_prompt


def build_llm_context(company_context: str | None = None) -> OpenAILLMContext:
    """Fresh per-call conversation context, seeded with the system prompt.

    `company_context` is where Team A's B2B record gets injected for the
    final integration phase (personalised outbound qualification demo).
    """
    messages = [{"role": "system", "content": build_system_prompt(company_context)}]
    return OpenAILLMContext(messages)


def build_pipeline_task(
    transport,
    call_id: str,
    company_context: str | None = None,
) -> PipelineTask:
    """
    Assembles the full STT -> LLM -> TTS loop for a single call/session and
    returns a ready-to-run PipelineTask.

    `transport` must already be constructed by the caller (LiveKitTransport
    for WebRTC, or FastAPIWebsocketTransport w/ TwilioFrameSerializer for
    telephony) — this function does not care which one it is.
    """

    logger.info(f"[{call_id}] Building pipeline (Deepgram -> Groq -> ElevenLabs)")

    # ---- Pillar 2: STT ----
    stt = DeepgramSTTService(
        api_key=settings.deepgram_api_key,
        live_options={
            "model": settings.deepgram_model,
            "language": "en-US",
            "smart_format": True,
            "interim_results": True,   # required for fast, streaming partials
            "endpointing": 300,        # ms of silence before we consider the user done
            "vad_events": True,
        },
    )

    # ---- Pillar 3: LLM ----
    llm = GroqLLMService(
        api_key=settings.groq_api_key,
        model=settings.groq_model,
    )

    # ---- Pillar 4: TTS ----
    tts = ElevenLabsTTSService(
        api_key=settings.elevenlabs_api_key,
        voice_id=settings.elevenlabs_voice_id,
        model=settings.elevenlabs_model,
        # Streams audio chunk-by-chunk over websocket instead of waiting for
        # the full sentence — this is the single biggest latency win in the
        # whole pipeline.
        params=ElevenLabsTTSService.InputParams(
            optimize_streaming_latency=4,  # 0-4, 4 = max speed / lowest quality tradeoff
            stability=0.5,
            similarity_boost=0.8,
        ),
    )

    context = build_llm_context(company_context)
    context_aggregator = llm.create_context_aggregator(context)

    latency_logger = LatencyLoggerProcessor(call_id=call_id)

    pipeline = Pipeline(
        [
            transport.input(),            # audio in from WebRTC/SIP
            stt,                           # audio -> text
            context_aggregator.user(),     # append user turn to context
            llm,                           # text -> text (streamed)
            latency_logger,                # measures LLM-first-token -> TTS-first-byte
            tts,                           # text -> audio (streamed)
            transport.output(),            # audio out to WebRTC/SIP
            context_aggregator.assistant(),  # append assistant turn to context
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,   # Pillar 4: core interruption switch
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    return task


def build_vad_analyzer() -> SileroVADAnalyzer:
    """
    Shared VAD config for both transports. Tuned to be resistant to short
    coughs/stutters (the 'Guardrail Shield' requirement) while still cutting
    off the bot's TTS fast when the user genuinely starts talking.
    """
    return SileroVADAnalyzer(
        params=VADParams(
            confidence=0.7,
            start_secs=0.2,     # user must speak for 200ms before we treat it as a turn
            stop_secs=0.8,      # 800ms of silence before we consider user done talking
            min_volume=0.6,
        )
    )

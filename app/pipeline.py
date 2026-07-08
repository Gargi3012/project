from deepgram import LiveOptions
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.frames.frames import Frame, TranscriptionFrame, InterimTranscriptionFrame
from pipecat.services.deepgram import DeepgramSTTService

from pipecat.services.groq import GroqLLMService

from app.config import settings
from app.metrics import LatencyLoggerProcessor
from app.prompts import build_system_prompt


def build_llm_context(company_context: str | None = None) -> OpenAILLMContext:
    messages = [{"role": "system", "content": build_system_prompt(company_context)}]
    return OpenAILLMContext(messages)


def build_pipeline_task(
    transport,
    call_id: str,
    company_context: str | None = None,
) -> PipelineTask:

    logger.info(f"[{call_id}] Building pipeline (Deepgram -> Groq -> [TTS disabled for debugging])")
    stt = DeepgramSTTService(
        api_key=settings.deepgram_api_key,
        live_options=LiveOptions(
            # nova-2-phonecall is tuned for 8 kHz telephony audio (Twilio)
            model="nova-2-phonecall",
            language="en-US",
            smart_format=True,
            interim_results=True,
            endpointing="300",
            encoding="linear16",
            sample_rate=8000,
            channels=1,
        ),
    )

    # ---- Pillar 3: LLM ----
    llm = GroqLLMService(
        api_key=settings.groq_api_key,
        model=settings.groq_model,
    )

    # ---- TTS DISABLED FOR DEBUGGING — uncomment this whole block to re-enable ----
    # tts = ElevenLabsTTSService(
    #     api_key=settings.elevenlabs_api_key,
    #     voice_id=settings.elevenlabs_voice_id,
    #     model=settings.elevenlabs_model,
    #     params=ElevenLabsTTSService.InputParams(
    #         optimize_streaming_latency="4",  # must be a string ("0"-"4"); 4 = fastest
    #         stability=0.5,
    #         similarity_boost=0.8,
    #     ),
    # )

    context = build_llm_context(company_context)
    context_aggregator = llm.create_context_aggregator(context)

    latency_logger = LatencyLoggerProcessor(call_id=call_id)

    class STTLogger(FrameProcessor):
        async def process_frame(self, frame: Frame, direction: FrameDirection):
            await super().process_frame(frame, direction)
            if isinstance(frame, TranscriptionFrame):
                logger.info(f"[{call_id}] STT RAW OUTPUT: {frame.text!r}")
            elif isinstance(frame, InterimTranscriptionFrame):
                logger.debug(f"[{call_id}] STT INTERIM: {frame.text!r}")
            await self.push_frame(frame, direction)

    pipeline = Pipeline(
        [
            transport.input(),            # audio in from WebRTC/SIP
            stt,                           # audio -> text
            STTLogger(),                   # <--- Logs exactly what Deepgram outputs
            context_aggregator.user(),     # append user turn to context
            llm,                           # text -> text (streamed)
            latency_logger,                # logs USER SAID / LLM REPLIED + timings
            # tts,                         
            transport.output(),            # audio out to WebRTC/SIP (silent while TTS is off)
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
    return SileroVADAnalyzer(
        params=VADParams(
            confidence=0.7,
            start_secs=0.2,     
            stop_secs=0.8,      
            min_volume=0.6,
        )
    )
"""
Key Deliverable: Latency Diagnostic Log.

A lightweight FrameProcessor that sits in the middle of the pipeline and logs
how long each stage takes, per call. This is intentionally simple (structured
log lines) so it can be piped into any observability stack later (Grafana,
Datadog, etc.) without changing the pipeline.
"""

import time

from loguru import logger

from pipecat.frames.frames import (
    Frame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    TextFrame,
    TTSStartedFrame,
    TranscriptionFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor


class LatencyLoggerProcessor(FrameProcessor):
    """
    Tracks three timestamps per conversational turn:
      t0 = final user transcription arrives (STT done)
      t1 = LLM starts streaming its response
      t2 = TTS starts producing audio

    Logs:  stt_to_llm_ms, llm_to_tts_ms, stt_to_tts_total_ms
    """

    def __init__(self, call_id: str):
        super().__init__()
        self.call_id = call_id
        self._t_transcription: float | None = None
        self._t_llm_start: float | None = None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        now = time.monotonic()

        if isinstance(frame, TranscriptionFrame):
            self._t_transcription = now
            logger.info(f"[{self.call_id}] USER SAID: {frame.text!r}")

        elif isinstance(frame, LLMFullResponseStartFrame):
            self._t_llm_start = now
            self._llm_response_text = ""
            if self._t_transcription:
                delta_ms = (now - self._t_transcription) * 1000
                logger.info(f"[{self.call_id}] stt_to_llm_ms={delta_ms:.0f}")

        elif isinstance(frame, TextFrame):
            # Streamed LLM text chunks — accumulate so we can log the full reply
            if hasattr(self, "_llm_response_text"):
                self._llm_response_text += frame.text

        elif isinstance(frame, LLMFullResponseEndFrame):
            if hasattr(self, "_llm_response_text"):
                logger.info(f"[{self.call_id}] LLM REPLIED: {self._llm_response_text!r}")

        elif isinstance(frame, TTSStartedFrame):
            if self._t_llm_start:
                delta_ms = (now - self._t_llm_start) * 1000
                logger.info(f"[{self.call_id}] llm_to_tts_ms={delta_ms:.0f}")
            if self._t_transcription:
                total_ms = (now - self._t_transcription) * 1000
                logger.info(f"[{self.call_id}] stt_to_tts_total_ms={total_ms:.0f}")
                # Reset for the next turn
                self._t_transcription = None
                self._t_llm_start = None

        await self.push_frame(frame, direction)
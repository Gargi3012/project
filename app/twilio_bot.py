"""
Pillar 2 — Audio Ingestion & Transport (Telephony Lead)

FastAPI app that:
  1. Answers a Twilio call (inbound OR outbound — see outbound_call.py) with
     TwiML that opens a <Stream> to our websocket endpoint.
  2. On the websocket, wraps the raw Twilio Media Stream frames using
     Pipecat's TwilioFrameSerializer and hands them to the same
     build_pipeline_task() used by the LiveKit bot.

Run with:  python run_twilio.py   (or: uvicorn app.twilio_bot:app --host 0.0.0.0 --port 8765)
Point your Twilio phone number's "A Call Comes In" webhook at:
   POST {PUBLIC_BASE_URL}/twilio/incoming
"""

import json
import uuid

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse
from loguru import logger
from pipecat.pipeline.runner import PipelineRunner
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.transports.network.fastapi_websocket import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from app.config import settings
from app.pipeline import build_pipeline_task, build_vad_analyzer

app = FastAPI(title="Project 2 - Twilio Voice Gateway")


@app.post("/twilio/incoming")
async def twilio_incoming_call(request: Request):
    """
    Twilio hits this both for inbound calls and for outbound calls placed by
    outbound_call.py. We respond with TwiML that opens a bidirectional Media
    Stream websocket back to /twilio/media-stream.
    """
    form = await request.form()
    call_sid = form.get("CallSid", str(uuid.uuid4()))
    logger.info(f"Call event received: {call_sid}")

    # For outbound calls placed by outbound_call.py, company_context is passed
    # as a query param on the webhook URL. Thread it through to the media
    # stream via a Twilio <Parameter>, since query params on the Stream url
    # itself are not reliably passed to the websocket "start" event.
    company_context = request.query_params.get("company_context", "")

    stream_url = f"wss://{_host_from_base_url()}/twilio/media-stream"

    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="{stream_url}">
            <Parameter name="call_sid" value="{call_sid}" />
            <Parameter name="company_context" value="{company_context}" />
        </Stream>
    </Connect>
</Response>"""

    return PlainTextResponse(content=twiml, media_type="text/xml")


@app.websocket("/twilio/media-stream")
async def twilio_media_stream(websocket: WebSocket):
    await websocket.accept()
    logger.info("Twilio media stream connected")

    # Twilio's <Stream> handshake sends a "start" event before audio frames
    # begin. TwilioFrameSerializer needs the streamSid from that event to
    # correctly tag outbound audio frames.
    start_data = websocket.iter_text()
    await start_data.__anext__()  # "connected" event, discard
    call_data = await start_data.__anext__()

    call_info = json.loads(call_data)
    stream_sid = call_info["start"]["streamSid"]
    call_sid = call_info["start"].get("callSid", stream_sid)
    custom_params = call_info["start"].get("customParameters", {})
    company_context = custom_params.get("company_context") or None

    serializer = TwilioFrameSerializer(stream_sid=stream_sid)

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            vad_enabled=True,        # required for vad_analyzer below to actually run
            vad_analyzer=build_vad_analyzer(),
            serializer=serializer,
        ),
    )

    task = build_pipeline_task(transport, call_id=call_sid, company_context=company_context)

    # handle_sigint=False: asyncio's add_signal_handler (used when True) is
    # NOT implemented on Windows and raises NotImplementedError there. It's
    # also called once per incoming call here, which would be wrong to pair
    # with sigint handling anyway — the FastAPI/uvicorn process owns that.
    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)


def _host_from_base_url() -> str:
    """Strips the scheme off PUBLIC_BASE_URL to build a wss:// stream URL."""
    url = settings.public_base_url
    return url.replace("https://", "").replace("http://", "").rstrip("/")


@app.get("/health")
async def health():
    return {"status": "ok"}
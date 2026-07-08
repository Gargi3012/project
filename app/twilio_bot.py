import asyncio
from fastapi import WebSocketDisconnect

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
TWILIO_SAMPLE_RATE = 8000


@app.post("/twilio/incoming")
async def twilio_incoming_call(request: Request):
    form = await request.form()
    call_sid = form.get("CallSid", str(uuid.uuid4()))
    logger.info(f"Call event received: {call_sid}")
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


    start_data = websocket.iter_text()
    await start_data.__anext__()  # "connected" event, discard
    call_data = await start_data.__anext__()

    call_info = json.loads(call_data)
    stream_sid = call_info["start"]["streamSid"]
    call_sid = call_info["start"].get("callSid", stream_sid)
    custom_params = call_info["start"].get("customParameters", {})
    company_context = custom_params.get("company_context") or None

    serializer = TwilioFrameSerializer(
        stream_sid=stream_sid,
        params=TwilioFrameSerializer.InputParams(
            twilio_sample_rate=TWILIO_SAMPLE_RATE,
            sample_rate=TWILIO_SAMPLE_RATE,
        ),
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=TWILIO_SAMPLE_RATE,   # FIX: match Twilio's real rate
            audio_out_sample_rate=TWILIO_SAMPLE_RATE,  # FIX: match Twilio's real rate
            add_wav_header=False,
            vad_enabled=False,       # FIX: Disable VAD to prevent pipecat from calling finalize() on Deepgram
            vad_analyzer=build_vad_analyzer(),
            serializer=serializer,
        ),
    )

    task = build_pipeline_task(transport, call_id=call_sid, company_context=company_context)


    runner = PipelineRunner(handle_sigint=False)
    
    try:
        await runner.run(task)
    except asyncio.CancelledError:
        pass
    except WebSocketDisconnect:
        pass
    finally:
        await task.cancel()


def _host_from_base_url() -> str:
    """Strips the scheme off PUBLIC_BASE_URL to build a wss:// stream URL."""
    url = settings.public_base_url
    return url.replace("https://", "").replace("http://", "").rstrip("/")


@app.get("/health")
async def health():
    return {"status": "ok"}
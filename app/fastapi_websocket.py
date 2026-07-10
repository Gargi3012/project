import asyncio
import typing
from loguru import logger
from fastapi import WebSocket
from dataclasses import dataclass

from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.frames.frames import StartFrame, EndFrame, CancelFrame, Frame, InputAudioRawFrame, TTSStoppedFrame, BotStoppedSpeakingFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.serializers.base_serializer import FrameSerializer

class FastAPIWebsocketParams(TransportParams):
    add_wav_header: bool = False
    vad_enabled: bool = False
    vad_analyzer: typing.Any = None
    vad_audio_passthrough: bool = False
    serializer: typing.Any = None
    session_timeout: int = 0

class FastAPIWebsocketCallbacks:
    def __init__(self, on_client_connected, on_client_disconnected, on_session_timeout):
        self.on_client_connected = on_client_connected
        self.on_client_disconnected = on_client_disconnected
        self.on_session_timeout = on_session_timeout

class FastAPIWebsocketInputTransport(BaseInputTransport):
    def __init__(
        self,
        websocket: WebSocket,
        params: FastAPIWebsocketParams,
        callbacks: FastAPIWebsocketCallbacks,
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._websocket = websocket
        self._params = params
        self._callbacks = callbacks

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._params.serializer.setup(frame)
        await self.set_transport_ready(frame)
        if not hasattr(self, '_audio_in_queue'):
            logger.warning(f"set_transport_ready failed to create _audio_in_queue! Force-creating.")
            self._audio_in_queue = asyncio.Queue()
            if not self._audio_task:
                self._audio_task = self.create_task(self._audio_task_handler())
        if self._params.session_timeout:
            self._monitor_websocket_task = self.create_task(self._monitor_websocket())
        await self._callbacks.on_client_connected(self._websocket)
        self._receive_task = self.create_task(self._receive_messages())

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self.cancel_task(self._receive_task)

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self.cancel_task(self._receive_task)

    def _iter_data(self) -> typing.AsyncIterator[bytes | str]:
        is_binary = getattr(self._params.serializer, "is_binary", False) if self._params.serializer else False
        if is_binary:
            return self._websocket.iter_bytes()
        else:
            return self._websocket.iter_text()

    async def _receive_messages(self):
        try:
            async for message in self._iter_data():
                frame = await self._params.serializer.deserialize(message)

                if not frame:
                    continue

                if isinstance(frame, InputAudioRawFrame):
                    await self.push_audio_frame(frame)
                else:
                    await self.push_frame(frame)
        except Exception as e:
            logger.error(f"{self} exception receiving data: {e.__class__.__name__} ({e})")
        finally:
            await self._callbacks.on_client_disconnected(self._websocket)

    async def _monitor_websocket(self):
        await asyncio.sleep(self._params.session_timeout)
        await self._callbacks.on_session_timeout(self._websocket)

class FastAPIWebsocketOutputTransport(BaseOutputTransport):
    def __init__(self, websocket: WebSocket, params: FastAPIWebsocketParams, **kwargs):
        super().__init__(params, **kwargs)
        self._websocket = websocket
        self._params = params

    async def _handle_frame(self, frame: Frame):
        if isinstance(frame, TTSStoppedFrame):
            logger.debug(f"{self} caught TTSStoppedFrame, emitting BotStoppedSpeakingFrame upstream to un-mute user.")
            upstream_frame = BotStoppedSpeakingFrame()
            await self.push_frame(upstream_frame, FrameDirection.UPSTREAM)
            # Optionally push downstream if needed, but upstream is what un-mutes the LLM aggregator.
            downstream_frame = BotStoppedSpeakingFrame()
            await self.push_frame(downstream_frame, FrameDirection.DOWNSTREAM)
            
        serialized = await self._params.serializer.serialize(frame)
        if serialized:
            is_binary = getattr(self._params.serializer, "is_binary", False) if self._params.serializer else False
            if is_binary:
                await self._websocket.send_bytes(serialized)
            else:
                await self._websocket.send_text(serialized)

class FastAPIWebsocketTransport(BaseTransport):
    def __init__(
        self,
        websocket: WebSocket,
        params: FastAPIWebsocketParams,
        input_name: str | None = None,
        output_name: str | None = None,
    ):
        super().__init__(input_name=input_name, output_name=output_name)
        self._params = params

        self._callbacks = FastAPIWebsocketCallbacks(
            on_client_connected=self._on_client_connected,
            on_client_disconnected=self._on_client_disconnected,
            on_session_timeout=self._on_session_timeout,
        )

        self._input = FastAPIWebsocketInputTransport(
            websocket, self._params, self._callbacks, name=self._input_name
        )
        self._output = FastAPIWebsocketOutputTransport(
            websocket, self._params, name=self._output_name
        )

        self._register_event_handler("on_client_connected")
        self._register_event_handler("on_client_disconnected")
        self._register_event_handler("on_session_timeout")

    def input(self) -> FastAPIWebsocketInputTransport:
        return self._input

    def output(self) -> FastAPIWebsocketOutputTransport:
        return self._output

    async def _on_client_connected(self, websocket):
        await self._call_event_handler("on_client_connected", websocket)

    async def _on_client_disconnected(self, websocket):
        await self._call_event_handler("on_client_disconnected", websocket)

    async def _on_session_timeout(self, websocket):
        await self._call_event_handler("on_session_timeout", websocket)

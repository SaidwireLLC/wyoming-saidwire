"""Wyoming event handler: one connection = one transcription."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
import logging

from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioChunkConverter, AudioStart, AudioStop
from wyoming.error import Error
from wyoming.event import Event
from wyoming.info import Describe, Info
from wyoming.server import AsyncEventHandler

from .api import SaidwireClient, SaidwireError, Transcript as SaidwireTranscript

_LOGGER = logging.getLogger(__name__)

RATE = 16000
WIDTH = 2
CHANNELS = 1


@dataclass
class Settings:
    """Options that apply to every request."""

    model: str | None = None
    language: str = "en"


class SaidwireEventHandler(AsyncEventHandler):
    """Streams audio to Saidwire while it arrives and answers with the transcript."""

    def __init__(
        self,
        info: Info,
        client: SaidwireClient,
        settings: Settings,
        *args,
        **kwargs,
    ) -> None:
        """Initialize the handler."""
        super().__init__(*args, **kwargs)
        self._info_event = info.event()
        self._client = client
        self._settings = settings
        self._converter = AudioChunkConverter(rate=RATE, width=WIDTH, channels=CHANNELS)
        self._language: str | None = None
        self._queue: asyncio.Queue[bytes | None] | None = None
        self._task: asyncio.Task[SaidwireTranscript] | None = None

    async def handle_event(self, event: Event) -> bool:
        """Handle one Wyoming event. Returning False closes the connection."""
        if Describe.is_type(event.type):
            await self.write_event(self._info_event)
            return True

        if Transcribe.is_type(event.type):
            self._language = Transcribe.from_event(event).language
            return True

        if AudioStart.is_type(event.type):
            self._start()
            return True

        if AudioChunk.is_type(event.type):
            if self._queue is None:
                self._start()
            assert self._queue is not None and self._task is not None
            if self._task.done():
                # The upstream request already failed (for example a bad key): say so
                # now instead of after the whole command.
                return await self._finish()
            chunk = self._converter.convert(AudioChunk.from_event(event))
            self._queue.put_nowait(chunk.audio)
            return True

        if AudioStop.is_type(event.type):
            if self._queue is None:
                await self.write_event(Transcript(text="").event())
                return False
            self._queue.put_nowait(None)
            return await self._finish()

        return True

    def _start(self) -> None:
        if self._task is not None:
            self._task.cancel()
        self._queue = asyncio.Queue()
        language = (self._language or self._settings.language).split("-")[0].split("_")[0]
        self._task = asyncio.create_task(
            self._client.transcribe_stream(
                self._audio(self._queue),
                sample_rate=RATE,
                channels=CHANNELS,
                language=language.lower() or "en",
                model=self._settings.model,
            )
        )

    @staticmethod
    async def _audio(queue: asyncio.Queue[bytes | None]) -> AsyncIterator[bytes]:
        while (chunk := await queue.get()) is not None:
            yield chunk

    async def _finish(self) -> bool:
        assert self._task is not None
        try:
            result = await self._task
        except SaidwireError as err:
            _LOGGER.error("Saidwire request failed: %s", err)
            await self.write_event(Error(text=str(err), code=type(err).__name__).event())
        else:
            _LOGGER.debug(
                "Transcript (request %s, %.2f s audio, final %s ms after Finalize)",
                result.request_id,
                result.audio_seconds,
                result.finalize_ms,
            )
            await self.write_event(Transcript(text=result.text).event())
        finally:
            self._task = None
            self._queue = None
        return False

    async def disconnect(self) -> None:
        """Cancel an unfinished request when the client goes away."""
        if self._task is not None and not self._task.done():
            self._task.cancel()

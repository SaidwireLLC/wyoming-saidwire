"""Tests for the Wyoming handler, driven like Home Assistant's Wyoming integration."""

from __future__ import annotations

import asyncio

from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient
from wyoming.error import Error
from wyoming.info import Describe, Info

from .conftest import TRANSCRIPT, Bridge

PCM = bytes(range(256)) * 62 + bytes(128)  # 0.5 s at 16 kHz


async def send_command(port: int, pcm: bytes, rate: int = 16000, language: str = "en-US"):
    async with AsyncTcpClient("127.0.0.1", port) as client:
        await client.write_event(Transcribe(language=language).event())
        await client.write_event(AudioStart(rate=rate, width=2, channels=1).event())
        step = rate * 2 // 100
        for i in range(0, len(pcm), step):
            await client.write_event(
                AudioChunk(rate=rate, width=2, channels=1, audio=pcm[i : i + step]).event()
            )
        await client.write_event(AudioStop().event())
        return await asyncio.wait_for(client.read_event(), 5)


async def test_describe(bridge: Bridge) -> None:
    async with AsyncTcpClient("127.0.0.1", bridge.port) as client:
        await client.write_event(Describe().event())
        info = Info.from_event(await client.read_event())
    (program,) = info.asr
    assert program.name == "saidwire"
    assert program.attribution.name == "Saidwire"
    assert program.models[0].languages == ["en"]


async def test_transcribe(bridge: Bridge) -> None:
    event = await send_command(bridge.port, PCM)
    assert Transcript.is_type(event.type)
    assert Transcript.from_event(event).text == TRANSCRIPT
    await asyncio.sleep(0.1)  # background close
    (stream,) = bridge.fake.streams
    assert bytes(stream["audio"]) == PCM
    assert stream["query"]["language"] == "en"
    assert stream["messages"][0] == "Finalize"


async def test_resamples_to_16k(bridge: Bridge) -> None:
    pcm_8k = b"\x00\x10" * 4000  # 0.5 s at 8 kHz
    event = await send_command(bridge.port, pcm_8k, rate=8000)
    assert Transcript.from_event(event).text == TRANSCRIPT
    assert abs(len(bridge.fake.streams[0]["audio"]) - 16000) <= 64  # ~0.5 s at 16 kHz


async def test_bad_key_returns_error(bad_key_bridge: Bridge) -> None:
    event = await send_command(bad_key_bridge.port, PCM)
    assert Error.is_type(event.type)
    assert Error.from_event(event).code == "SaidwireAuthError"

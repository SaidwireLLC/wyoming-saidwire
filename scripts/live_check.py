"""Drive a running wyoming-saidwire server the way Home Assistant does and time it.

    python scripts/live_check.py --port 10300 speech.wav [marks.json]

marks.json (optional): {"lines": [...], "marks": [[start_s, end_s], ...]}; each mark is
sent as one command (0.3 s lead-in, 0.7 s trailing silence), then the whole file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import wave

from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.client import AsyncTcpClient
from wyoming.error import Error


async def command(host: str, port: int, pcm: bytes) -> tuple[str, float]:
    async with AsyncTcpClient(host, port) as client:
        await client.write_event(Transcribe(language="en-US").event())
        await client.write_event(AudioStart(rate=16000, width=2, channels=1).event())
        start = time.monotonic()
        for n, i in enumerate(range(0, len(pcm), 320)):
            chunk = AudioChunk(rate=16000, width=2, channels=1, audio=pcm[i : i + 320])
            await client.write_event(chunk.event())
            delay = start + (n + 1) * 0.01 - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
        end = time.monotonic()
        await client.write_event(AudioStop().event())
        event = await asyncio.wait_for(client.read_event(), 15)
        took = (time.monotonic() - end) * 1000
    if Error.is_type(event.type):
        raise SystemExit(f"error: {Error.from_event(event).text}")
    return Transcript.from_event(event).text, round(took, 1)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wav")
    parser.add_argument("marks", nargs="?")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10300)
    args = parser.parse_args()
    with wave.open(args.wav, "rb") as wav:
        pcm = wav.readframes(wav.getnframes())
    pieces = []
    if args.marks:
        with open(args.marks, encoding="utf-8") as f:
            spec = json.load(f)
        for line, (a, b) in zip(spec["lines"], spec["marks"], strict=True):
            pieces.append(
                (line, pcm[int(max(0, a - 0.3) * 16000) * 2 : int((b + 0.7) * 16000) * 2])
            )
    pieces.append(("(whole file)", pcm))
    rows = []
    for expected, audio in pieces:
        text, ms = await command(args.host, args.port, audio)
        rows.append({"expected": expected, "text": text, "audio_stop_to_transcript_ms": ms})
    times = [r["audio_stop_to_transcript_ms"] for r in rows]
    print(
        json.dumps(
            {"commands": rows, "median_ms": statistics.median(times), "max_ms": max(times)},
            indent=1,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())

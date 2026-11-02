"""Minimal async client for the Saidwire speech-to-text API.

Copied from hass-saidwire (custom_components/saidwire/api.py); keep the code in step
until both use one PyPI library.

Streaming protocol, ``wss://api.saidwire.com/v1/listen``:

* binary messages carry 16-bit little-endian PCM;
* ``{"type": "Finalize"}`` asks the server to finish the current utterance now; the
  answer is a ``Results`` message with ``is_final`` and ``from_finalize`` set;
* ``{"type": "CloseStream"}`` flushes and closes the stream;
* ``{"type": "Error", "err_code": ..., "err_msg": ...}`` reports a problem.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
import contextlib
from dataclasses import dataclass, field
import json
import logging
import struct
import time
from typing import Any
from urllib.parse import urlencode

import aiohttp

_LOGGER = logging.getLogger(__name__)

DEFAULT_HTTP_URL = "https://api.saidwire.com"
DEFAULT_WS_URL = "wss://api.saidwire.com/v1/listen"

FINALIZE = json.dumps({"type": "Finalize"})
CLOSE_STREAM = json.dumps({"type": "CloseStream"})

# Coalesce Home Assistant's 10 ms chunks into at least 20 ms WebSocket messages.
MIN_SEND_BYTES = 640

_AUTH_CODES = {"INVALID_AUTH", "KEY_REVOKED", "FORBIDDEN"}
_QUOTA_CODES = {"PAYMENT_REQUIRED", "QUOTA_EXCEEDED"}
_BUSY_CODES = {"RATE_LIMITED", "TOO_MANY_STREAMS", "SERVICE_UNAVAILABLE", "STARTING"}


class SaidwireError(Exception):
    """Base class for Saidwire errors."""


class SaidwireAuthError(SaidwireError):
    """The API key was rejected."""


class SaidwireQuotaError(SaidwireError):
    """The account has no hours left or billing needs attention."""


class SaidwireBusyError(SaidwireError):
    """The service is starting up, busy, or rate limiting this key."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        """Initialize with an optional server hint in seconds."""
        super().__init__(message)
        self.retry_after = retry_after


class SaidwireRequestError(SaidwireError):
    """The server refused the request parameters or the audio."""


class SaidwireConnectionError(SaidwireError):
    """The service could not be reached or the connection dropped."""


class SaidwireTimeoutError(SaidwireError):
    """The service did not answer in time."""


@dataclass
class Transcript:
    """Result of one streamed utterance."""

    text: str
    request_id: str | None = None
    audio_seconds: float = 0.0
    # Time from sending Finalize to receiving the final transcript, in ms.
    finalize_ms: float | None = None
    segments: list[str] = field(default_factory=list)


def _error_for(
    status: int, code: str | None, message: str, retry_after: float | None
) -> SaidwireError:
    """Map an HTTP status or an API error code to an exception."""
    code = (code or "").upper()
    text = f"{message} (HTTP {status})" if status else message
    if status in (401, 403) or code in _AUTH_CODES or code == "INVALID_API_KEY":
        return SaidwireAuthError(text)
    if status == 402 or code in _QUOTA_CODES:
        return SaidwireQuotaError(text)
    if status in (429, 503) or code in _BUSY_CODES:
        return SaidwireBusyError(text, retry_after)
    if 400 <= status < 500 or code.startswith(("BAD_", "UNSUPPORTED_")):
        return SaidwireRequestError(text)
    return SaidwireConnectionError(text)


def _retry_after(headers: Any) -> float | None:
    try:
        value = headers.get("Retry-After") if headers is not None else None
        return max(0.5, min(30.0, float(value))) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _strip_wav_header(audio: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
    """Yield raw PCM, dropping a RIFF/WAV header if the stream starts with one.

    The Assist pipeline sends raw PCM. Clients of the ``/api/stt`` HTTP endpoint
    usually send a WAV file, header included.
    """
    head = b""
    checked = False
    async for chunk in audio:
        if checked:
            if chunk:
                yield chunk
            continue
        head += chunk
        if len(head) < 12:
            continue
        if head[:4] != b"RIFF" or head[8:12] != b"WAVE":
            checked = True
            yield head
            continue
        # Walk the RIFF chunks until the "data" chunk starts.
        pos = 12
        while pos + 8 <= len(head):
            chunk_id = head[pos : pos + 4]
            (size,) = struct.unpack("<I", head[pos + 4 : pos + 8])
            if chunk_id == b"data":
                checked = True
                rest = head[pos + 8 :]
                if rest:
                    yield rest
                break
            pos += 8 + size + (size & 1)
        if not checked and len(head) > 65536:
            # Not a header we understand: pass the audio through untouched.
            checked = True
            yield head
    if not checked and head:
        yield head


class SaidwireClient:
    """Talks to the Saidwire API over a shared aiohttp session."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        *,
        http_url: str = DEFAULT_HTTP_URL,
        ws_url: str = DEFAULT_WS_URL,
        user_agent: str = "saidwire-python",
    ) -> None:
        """Initialize the client."""
        self._session = session
        self._api_key = api_key
        self._http_url = http_url.rstrip("/")
        self._ws_url = ws_url
        self._user_agent = user_agent
        self._closing: set[asyncio.Future[None]] = set()

    async def _close(
        self, ws: aiohttp.ClientWebSocketResponse, recv_task: asyncio.Task[None] | None
    ) -> None:
        with contextlib.suppress(Exception):
            if not ws.closed:
                await ws.send_str(CLOSE_STREAM)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(ws.close(), 3.0)
        if recv_task is not None and not recv_task.done():
            recv_task.cancel()
            with contextlib.suppress(BaseException):
                await recv_task

    async def async_close(self) -> None:
        """Wait for streams that are still closing in the background."""
        if self._closing:
            await asyncio.gather(*self._closing, return_exceptions=True)

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self._api_key}", "User-Agent": self._user_agent}

    async def list_models(self, request_timeout: float = 10.0) -> list[str]:
        """Return the model ids this key can use (also validates the key)."""
        try:
            async with self._session.get(
                f"{self._http_url}/v1/models",
                headers=self._headers,
                timeout=aiohttp.ClientTimeout(total=request_timeout),
            ) as resp:
                if resp.status != 200:
                    code, message = None, f"Saidwire answered HTTP {resp.status}"
                    with contextlib.suppress(Exception):
                        body = await resp.json(content_type=None)
                        err = body.get("error") if isinstance(body, dict) else None
                        if isinstance(err, dict):
                            code, message = err.get("code"), err.get("message") or message
                        elif isinstance(body, dict):
                            code = body.get("err_code")
                            message = body.get("err_msg") or message
                    raise _error_for(resp.status, code, message, _retry_after(resp.headers))
                body = await resp.json(content_type=None)
        except SaidwireError:
            raise
        except TimeoutError as err:
            raise SaidwireTimeoutError(
                f"Saidwire did not answer within {request_timeout:.0f} s"
            ) from err
        except (aiohttp.ClientError, ValueError) as err:
            raise SaidwireConnectionError(f"Cannot reach Saidwire: {err}") from err
        return [m["id"] for m in body.get("data", []) if isinstance(m, dict) and "id" in m]

    def listen_url(
        self,
        *,
        sample_rate: int,
        channels: int = 1,
        language: str | None = "en",
        model: str | None = None,
        punctuate: bool = True,
        smart_format: bool = True,
    ) -> str:
        """Build the streaming URL."""
        query = {
            "encoding": "linear16",
            "sample_rate": str(sample_rate),
            "channels": str(channels),
            "punctuate": str(punctuate).lower(),
            "smart_format": str(smart_format).lower(),
            "interim_results": "false",
        }
        if language:
            query["language"] = language
        if model:
            query["model"] = model
        sep = "&" if "?" in self._ws_url else "?"
        return f"{self._ws_url}{sep}{urlencode(query)}"

    async def _connect(
        self, url: str, connect_timeout: float, busy_timeout: float
    ) -> aiohttp.ClientWebSocketResponse:
        """Open the stream, waiting while the service reports it is starting up."""
        deadline = time.monotonic() + busy_timeout
        while True:
            try:
                return await asyncio.wait_for(
                    self._session.ws_connect(
                        url,
                        headers=self._headers,
                        timeout=aiohttp.ClientWSTimeout(ws_receive=None, ws_close=5.0),
                        autoping=True,
                        max_msg_size=1 << 20,
                    ),
                    connect_timeout,
                )
            except aiohttp.WSServerHandshakeError as err:
                error = _error_for(
                    err.status,
                    None,
                    f"Saidwire refused the stream: {err.message or ''}".strip(),
                    _retry_after(err.headers),
                )
                if isinstance(error, SaidwireBusyError):
                    wait = error.retry_after or 2.0
                    if time.monotonic() + wait < deadline:
                        _LOGGER.debug("Saidwire is busy or starting up, retrying in %.1f s", wait)
                        await asyncio.sleep(wait)
                        continue
                raise error from err
            except TimeoutError as err:
                raise SaidwireTimeoutError(
                    f"Timed out connecting to Saidwire after {connect_timeout:.0f} s"
                ) from err
            except (aiohttp.ClientError, OSError) as err:
                raise SaidwireConnectionError(f"Cannot reach Saidwire: {err}") from err

    async def transcribe_stream(
        self,
        audio: AsyncIterable[bytes],
        *,
        sample_rate: int = 16000,
        channels: int = 1,
        language: str | None = "en",
        model: str | None = None,
        punctuate: bool = True,
        smart_format: bool = True,
        connect_timeout: float = 10.0,
        busy_timeout: float = 20.0,
        finalize_timeout: float = 8.0,
    ) -> Transcript:
        """Stream PCM audio while it arrives and return the final transcript.

        The connection is opened as soon as this is called; audio that arrives
        before it is ready is buffered. When ``audio`` ends, ``Finalize`` is sent
        and the call returns as soon as the server answers it.
        """
        url = self.listen_url(
            sample_rate=sample_rate,
            channels=channels,
            language=language,
            model=model,
            punctuate=punctuate,
            smart_format=smart_format,
        )
        bytes_per_second = sample_rate * 2 * channels
        result = Transcript(text="")
        finalized = asyncio.Event()
        state: dict[str, Any] = {"error": None, "finalize_sent": None, "closed": False}

        async def receive(ws: aiohttp.ClientWebSocketResponse) -> None:
            try:
                async for msg in ws:
                    if msg.type is not aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = json.loads(msg.data)
                    except ValueError:
                        continue
                    kind = data.get("type")
                    if kind == "Results":
                        if not data.get("is_final"):
                            continue
                        alts = (data.get("channel") or {}).get("alternatives") or []
                        text = (alts[0].get("transcript") or "").strip() if alts else ""
                        if text:
                            result.segments.append(text)
                        if data.get("from_finalize"):
                            if state["finalize_sent"] is not None:
                                result.finalize_ms = round(
                                    (time.monotonic() - state["finalize_sent"]) * 1000, 1
                                )
                            finalized.set()
                    elif kind == "Metadata":
                        result.request_id = result.request_id or data.get("request_id")
                    elif kind == "Error":
                        code = data.get("err_code") or ""
                        if code == "BAD_MESSAGE":
                            _LOGGER.debug("Saidwire ignored a message: %s", data.get("err_msg"))
                            continue
                        state["error"] = _error_for(
                            0,
                            code,
                            f"Saidwire error {code}: {data.get('err_msg', '')}".strip(),
                            None,
                        )
                        break
            except (aiohttp.ClientError, OSError) as err:
                state["error"] = SaidwireConnectionError(f"Connection to Saidwire dropped: {err}")
            finally:
                state["closed"] = True
                finalized.set()

        connect_task = asyncio.ensure_future(self._connect(url, connect_timeout, busy_timeout))
        ws: aiohttp.ClientWebSocketResponse | None = None
        recv_task: asyncio.Task[None] | None = None
        pending = bytearray()
        sent_bytes = 0

        async def start() -> None:
            nonlocal ws, recv_task
            ws = connect_task.result()
            response = getattr(ws, "_response", None)
            if response is not None:
                result.request_id = response.headers.get("x-request-id")
            recv_task = asyncio.create_task(receive(ws))

        async def flush(force: bool = False) -> None:
            nonlocal sent_bytes
            if ws is None or not pending or (len(pending) < MIN_SEND_BYTES and not force):
                return
            if state["error"] is not None:
                raise state["error"]
            if state["closed"]:
                raise SaidwireConnectionError("Saidwire closed the stream early")
            data = bytes(pending)
            pending.clear()
            try:
                await ws.send_bytes(data)
            except (aiohttp.ClientError, ConnectionError, OSError) as err:
                raise SaidwireConnectionError(f"Connection to Saidwire dropped: {err}") from err
            sent_bytes += len(data)

        try:
            async for chunk in _strip_wav_header(audio):
                pending.extend(chunk)
                if ws is None:
                    if not connect_task.done():
                        continue
                    await start()
                await flush()

            if ws is None:
                await connect_task
                await start()
            await flush(force=True)
            assert ws is not None
            result.audio_seconds = round(sent_bytes / bytes_per_second, 3)

            if state["error"] is not None:
                raise state["error"]
            state["finalize_sent"] = time.monotonic()
            try:
                await ws.send_str(FINALIZE)
            except (aiohttp.ClientError, ConnectionError, OSError) as err:
                raise SaidwireConnectionError(f"Connection to Saidwire dropped: {err}") from err

            try:
                await asyncio.wait_for(finalized.wait(), finalize_timeout)
            except TimeoutError:
                if not result.segments:
                    raise SaidwireTimeoutError(
                        f"Saidwire did not return a transcript within {finalize_timeout:.0f} s"
                    ) from None
                _LOGGER.debug("No answer to Finalize in time; using the finals received")

            if state["error"] is not None:
                raise state["error"]
            if state["closed"] and result.finalize_ms is None and not result.segments:
                code = ws.close_code
                if code in (1008, 4001, 4003):
                    raise SaidwireAuthError(f"Saidwire closed the stream (code {code})")
                raise SaidwireConnectionError(
                    f"Saidwire closed the stream before the transcript (code {code})"
                )
        finally:
            if not connect_task.done():
                connect_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await connect_task
            elif ws is None and not connect_task.cancelled():
                # Connected (or failed) but never used: close it / mark the error seen.
                if connect_task.exception() is None:
                    ws = connect_task.result()
            if ws is not None:
                # Close in the background so the transcript is returned without
                # waiting for the closing handshake.
                task = asyncio.ensure_future(self._close(ws, recv_task))
                self._closing.add(task)
                task.add_done_callback(self._closing.discard)
            elif recv_task is not None:
                recv_task.cancel()

        result.text = " ".join(result.segments).strip()
        return result

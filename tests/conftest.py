"""A local fake of the Saidwire API and a running Wyoming server for tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from functools import partial
import json
import socket
from typing import Any

import aiohttp
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestServer
import pytest
from wyoming.server import AsyncTcpServer

from wyoming_saidwire.__main__ import build_info
from wyoming_saidwire.api import SaidwireClient
from wyoming_saidwire.handler import SaidwireEventHandler, Settings

GOOD_KEY = "sw_test_good_key"
TRANSCRIPT = "Turn on the kitchen lights."


class FakeSaidwire:
    """Stand-in for api.saidwire.com."""

    def __init__(self) -> None:
        self.streams: list[dict[str, Any]] = []

    async def models(self, request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != f"Token {GOOD_KEY}":
            return web.json_response({"error": {"code": "invalid_api_key"}}, status=401)
        return web.json_response({"data": [{"id": "saidwire-en"}]})

    async def listen(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("Authorization") != f"Token {GOOD_KEY}":
            return web.json_response({"err_code": "INVALID_AUTH"}, status=401)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        stream: dict[str, Any] = {
            "query": dict(request.query),
            "audio": bytearray(),
            "messages": [],
        }
        self.streams.append(stream)
        async for msg in ws:
            if msg.type is WSMsgType.BINARY:
                stream["audio"] += msg.data
            elif msg.type is WSMsgType.TEXT:
                kind = json.loads(msg.data)["type"]
                stream["messages"].append(kind)
                if kind == "Finalize":
                    await ws.send_json(
                        {
                            "type": "Results",
                            "is_final": True,
                            "from_finalize": True,
                            "channel": {"alternatives": [{"transcript": TRANSCRIPT}]},
                        }
                    )
                elif kind == "CloseStream":
                    await ws.close()
                    break
        return ws


@dataclass
class Bridge:
    fake: FakeSaidwire
    port: int


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _bridge(key: str) -> AsyncGenerator[Bridge]:
    fake = FakeSaidwire()
    app = web.Application()
    app.router.add_get("/v1/models", fake.models)
    app.router.add_get("/v1/listen", fake.listen)
    api_server = TestServer(app)
    await api_server.start_server()
    base = str(api_server.make_url("")).rstrip("/")
    port = _free_port()
    async with aiohttp.ClientSession() as session:
        client = SaidwireClient(
            session, key, http_url=base, ws_url=base.replace("http", "ws", 1) + "/v1/listen"
        )
        server = AsyncTcpServer("127.0.0.1", port)
        await server.start(partial(SaidwireEventHandler, build_info(None), client, Settings()))
        yield Bridge(fake, port)
        await server.stop()
        await client.async_close()
    await api_server.close()


@pytest.fixture
async def bridge() -> AsyncGenerator[Bridge]:
    async for b in _bridge(GOOD_KEY):
        yield b


@pytest.fixture
async def bad_key_bridge() -> AsyncGenerator[Bridge]:
    async for b in _bridge("sw_wrong"):
        yield b

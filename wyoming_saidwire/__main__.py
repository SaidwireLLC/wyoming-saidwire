"""Run the Saidwire Wyoming server.

    python -m wyoming_saidwire --uri tcp://0.0.0.0:10300

The API key comes from --api-key-file, or the SAIDWIRE_API_KEY environment variable.
"""

from __future__ import annotations

import argparse
import asyncio
from functools import partial
import logging
import os
from pathlib import Path
import sys
from urllib.parse import urlparse

import aiohttp
from wyoming.info import AsrModel, AsrProgram, Attribution, Info
from wyoming.server import AsyncServer

from . import __version__
from .api import SaidwireAuthError, SaidwireClient, SaidwireError
from .handler import SaidwireEventHandler, Settings

_LOGGER = logging.getLogger("wyoming_saidwire")


def build_info(model: str | None) -> Info:
    """Describe this server to Wyoming clients."""
    attribution = Attribution(name="Saidwire", url="https://saidwire.com")
    return Info(
        asr=[
            AsrProgram(
                name="saidwire",
                description="Saidwire cloud speech-to-text (English)",
                attribution=attribution,
                installed=True,
                version=__version__,
                models=[
                    AsrModel(
                        name=model or "saidwire-en",
                        description="Saidwire English",
                        attribution=attribution,
                        installed=True,
                        version=None,
                        languages=["en"],
                    )
                ],
            )
        ]
    )


def read_key(args: argparse.Namespace) -> str:
    if args.api_key_file:
        return Path(args.api_key_file).read_text(encoding="utf-8").strip()
    return os.environ.get("SAIDWIRE_API_KEY", "").strip()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Saidwire speech-to-text over Wyoming")
    parser.add_argument(
        "--uri", default="tcp://0.0.0.0:10300", help="tcp://host:port or unix://path"
    )
    parser.add_argument("--api-key-file", help="File holding the API key (else SAIDWIRE_API_KEY)")
    parser.add_argument("--model", help="Model id (default: Saidwire's default model)")
    parser.add_argument("--language", default="en", help="Language when the client sends none")
    parser.add_argument(
        "--zeroconf",
        nargs="?",
        const="saidwire",
        help="Announce the server for Home Assistant discovery (optional name)",
    )
    parser.add_argument("--debug", action="store_true", help="Log debug messages")
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    api_key = read_key(args)
    if not api_key:
        sys.exit("Set SAIDWIRE_API_KEY or pass --api-key-file (keys: https://saidwire.com/dev)")

    async with aiohttp.ClientSession() as session:
        client = SaidwireClient(session, api_key, user_agent=f"wyoming-saidwire/{__version__}")
        try:
            models = await client.list_models()
        except SaidwireAuthError:
            sys.exit("Saidwire rejected the API key. Get a key at https://saidwire.com/dev")
        except SaidwireError as err:
            _LOGGER.warning("Could not check the key with Saidwire yet (%s); starting anyway", err)
        else:
            _LOGGER.info("Saidwire key OK (models: %s)", ", ".join(models))

        server = AsyncServer.from_uri(args.uri)
        if args.zeroconf:
            from wyoming.zeroconf import HomeAssistantZeroconf

            port = urlparse(args.uri).port
            if port is None:
                sys.exit("--zeroconf needs a tcp:// URI with a port")
            zeroconf = HomeAssistantZeroconf(name=args.zeroconf, port=port)
            await zeroconf.register_server()
            _LOGGER.info("Zeroconf discovery enabled")

        _LOGGER.info("Ready on %s", args.uri)
        settings = Settings(model=args.model, language=args.language)
        await server.run(partial(SaidwireEventHandler, build_info(args.model), client, settings))


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()

# wyoming-saidwire

A [Wyoming protocol](https://github.com/OHF-Voice/wyoming) speech-to-text server that
uses [Saidwire](https://saidwire.com). Run it next to Home Assistant and add it with the
Wyoming Protocol integration; Assist then sends your voice commands through it to
Saidwire.

If you can install custom integrations, the
[Saidwire integration](https://github.com/Saidwire/hass-saidwire) is simpler. Use this
server when you want everything to go through Wyoming (for example, to share one
speech-to-text service between several Home Assistant installs or satellites).

- About 35 ms median to final transcript after Finalize, measured in US East; add your
  network round trip.
- 120 free hours, no card ([get a key](https://saidwire.com/dev)). Then $0.30 per audio
  hour, billed per second of audio sent.
- English only.

This server is built and maintained by Saidwire, the company that runs the service.

## Run with Docker

```bash
docker build -t wyoming-saidwire .
docker run -d --name wyoming-saidwire --restart unless-stopped \
  -p 10300:10300 -e SAIDWIRE_API_KEY=your_key wyoming-saidwire
```

Docker Compose:

```yaml
services:
  wyoming-saidwire:
    build: .
    restart: unless-stopped
    ports:
      - "10300:10300"
    environment:
      SAIDWIRE_API_KEY: your_key
```

## Run with Python

```bash
pip install .
SAIDWIRE_API_KEY=your_key wyoming-saidwire --uri tcp://0.0.0.0:10300
```

| Option | Default | What it does |
|--------|---------|--------------|
| `--uri` | `tcp://0.0.0.0:10300` | Where to listen (`tcp://host:port` or `unix://path`). |
| `--api-key-file` | | Read the key from a file instead of `SAIDWIRE_API_KEY`. |
| `--model` | Saidwire default | Model id. Leave unset unless support asks you to. |
| `--language` | `en` | Used when the client does not say. |
| `--zeroconf [NAME]` | off | Announce the server so Home Assistant discovers it. |
| `--debug` | off | Debug logging (each request's `request_id` and timing). |

The server checks the key with Saidwire at start-up and exits if it is rejected.

## Add it to Home Assistant

1. **Settings > Devices & services > Add integration > Wyoming Protocol**.
2. Host: the machine running the server; port: `10300`.
3. **Settings > Voice assistants**: open your assistant and set **Speech-to-text** to
   **saidwire**. Set the assistant's language to English.

## How it works

Home Assistant opens one Wyoming connection per voice command and streams 16 kHz mono
audio. The server forwards it to `wss://api.saidwire.com/v1/listen` as it arrives
(other sample rates are converted first). When Home Assistant sends `audio-stop`, the
server sends `Finalize` and returns the final transcript. Errors (bad key, no hours left,
network) come back as Wyoming `error` events, which Home Assistant logs.

Audio is only sent while Assist is listening for a command. Your key is only sent to
`api.saidwire.com`.

## Development

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[test]"
pytest
# against the real API, with a server running on port 10300:
python scripts/live_check.py --port 10300 speech.wav
```

## License

MIT

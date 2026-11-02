FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY wyoming_saidwire ./wyoming_saidwire
RUN pip install --no-cache-dir ".[zeroconf]" \
    && useradd --system --uid 10300 saidwire

USER saidwire
EXPOSE 10300
ENTRYPOINT ["wyoming-saidwire", "--uri", "tcp://0.0.0.0:10300"]

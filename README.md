# RPC Health Monitor

A read-only RPC endpoint monitor that measures latency and chain coherence and publishes ranked endpoints.

## Features

- Endpoint probes compare chain identity, synchronization and block freshness.
- Latency distributions support ranked selection.
- A local dashboard and consumer example expose monitor output.

## Stack

Python · aiohttp · websockets · Redis · Docker

A JSON seed defines candidates; Redis stores node status and network rankings.

## Getting started

Install requirements.txt in a virtual environment or review docker-compose.yml. Supply your own endpoint seed; do not assume the original local file paths exist.

## How it works

```text
Endpoint seed → bounded RPC probes → health and latency metrics → Redis rankings → dashboard/consumer
```

## Current limitations

No live RPC endpoint was probed in this review. Endpoint availability and archive/WebSocket support are provider-specific. Redis configuration and seed access require local setup.

## License

No open-source license has been granted. Redistribution terms remain to be defined.

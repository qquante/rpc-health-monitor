# RPC Health Monitor

> **Local private review:** publication is paused. See [security review and remaining release conditions](docs/SECURITY_REVIEW.md). Local fixes have not been pushed.


A read-only RPC endpoint monitor that measures latency and chain coherence and publishes ranked endpoints.

![review](https://img.shields.io/badge/review-private-374151) ![primary](https://img.shields.io/badge/primary-Python-2563EB) [![LinkedIn](https://img.shields.io/badge/LinkedIn-Douglas%20Silva-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/douglas-silva-data-analyst/)

[Overview](#overview) · [Architecture](#architecture) · [Setup](#setup) · [Validation](#validation) · [Roadmap](#roadmap) · [Review notes](docs/REVIEW.md)

## Overview

- Endpoint probes compare chain identity, synchronization and block freshness.
- Latency distributions support ranked selection.
- A local dashboard and consumer example expose monitor output.

**Technology:** Python · aiohttp · websockets · Redis · Docker

**Persistence and data:** A JSON seed defines candidates; Redis stores node status and network rankings.

## Architecture

```text
Endpoint seed → bounded RPC probes → health and latency metrics → Redis rankings → dashboard/consumer
```

| Location | Review focus |
| --- | --- |
| [`monitor.py`](monitor.py) | Source/configuration inspected for scope and structure |
| [`dashboard.py`](dashboard.py) | Source/configuration inspected for scope and structure |
| [`auditoria.py`](auditoria.py) | Source/configuration inspected for scope and structure |
| [`exemplo_consumo.py`](exemplo_consumo.py) | Source/configuration inspected for scope and structure |
| [`docker-compose.yml`](docker-compose.yml) | Source/configuration inspected for scope and structure |

Internal package and module names are retained where changing them would affect imports or compatibility. This descriptive repository name does not imply a completed package/API migration.

## Setup

Install requirements.txt in a virtual environment or review docker-compose.yml. Supply your own endpoint seed; do not assume the original local file paths exist.

Start from an isolated environment. Use `.env.example` when present and inspect the configuration loader: an example file is not necessarily loaded automatically. Runtime databases, real account sessions and private credentials are not bundled. Live integration prerequisites are separate from local tests.

## Validation

Static parsing checked 4 Python files and 1 strict JSON files. Python syntax errors: 0; JSON errors: 0. No functional test/build result is claimed for this preparation.

Available test scripts are inventoried in [the review notes](docs/REVIEW.md). Choose offline checks before operating any integration.

These results apply to the selected checks on the preparation copy. They do not establish deployment readiness, full coverage, live provider availability or measured performance. CI definitions, when present under `docs/ci-templates/`, are inactive review templates; no remote CI badge is claimed.

## Limits and current status

No live RPC endpoint was probed in this review. Endpoint availability and archive/WebSocket support are provider-specific. Redis configuration and seed access require local setup.

This private snapshot is for code and portfolio review. Generated runtime material and unreviewed binary assets may be excluded. [Review notes](docs/REVIEW.md) record preparation scope, findings and outstanding validation.

## Roadmap

- [ ] Create a synthetic seed and mocked RPC server.
- [ ] Add offline tests for ranking, failure and chain mismatch.
- [ ] Review probe concurrency and provider rate limits.

## History and attribution

No recoverable original Git history was verified for this snapshot. Import and preparation commits use their actual current dates; filesystem timestamps are not treated as development dates.

No open-source license is granted by this preparation. A license and attribution review is required before public redistribution.

## Maintainer and feedback

Douglas de Oliveira Silva · [GitHub](https://github.com/qquante) · [LinkedIn](https://www.linkedin.com/in/douglas-silva-data-analyst/)

For this private review, report reproducible issues with a minimal fixture and the exact check output. Keep account data, credentials and generated sensitive reports out of issues and commits. Existing contribution guidelines apply where present.

**Topics:** `python`, `redis`, `rpc`, `monitoring`, `blockchain`, `asyncio`

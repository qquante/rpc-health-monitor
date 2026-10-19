> Historical project notes. Paths, operational status, badges and test claims below are retained as archival reference and have not all been revalidated. Use the root README and docs/REVIEW.md for this review snapshot.

# Node Monitor - README forense

Este subprojeto e o runtime atual para escolher nodes RPC por rede. Ele mede endpoints Ethereum/BSC/Base, calcula percentis, filtra nodes incoerentes e publica um ranking no Redis para bots ou agents consumirem.

Para o mapa completo do repositorio, incluindo o pipeline antigo de discovery/tester, veja `../README.md`.

## Como ligar

```powershell
cd C:\Users\PC\Desktop\nodes\node-monitor

# criar config local se ainda nao existir
copy .env.example .env

# seed principal esperado dentro do volume ./data
copy D:\nodes_resultado.json .\data\nodes_resultado.json

docker compose up -d --build
docker compose logs -f monitor
```

Dashboard:

```text
http://localhost:8080
```

Checagens:

```powershell
docker compose ps
docker exec nodes-redis redis-cli ping
docker exec nodes-redis redis-cli --raw GET rpc:meta:eth
docker exec nodes-monitor python auditoria.py
```

Parar:

```powershell
docker compose down
```

## Arquitetura

```text
./data/nodes_resultado.json
        |
        v
nodes-monitor
  - detecta chainId, cliente, archive, WS e syncing
  - mede eth_blockNumber por HTTP ou WS
  - calcula p50/p90/p95/p99
  - filtra lag, success_rate, syncing e coerencia de bloco
        |
        v
nodes-redis
  - rpc:rank:{net}
  - rpc:node:{net}:{endpoint}
  - rpc:best:{net}
  - rpc:meta:{net}
        |
        +--> nodes-dashboard em http://localhost:8080
        +--> bot/agent via Redis
        +--> auditoria.py para prova de chainId
```

## Servicos Docker

| Servico | Container | Funcao | Porta host |
|---|---|---|---|
| `redis` | `nodes-redis` | Guarda ranking e payloads JSON. | `6380` |
| `monitor` | `nodes-monitor` | Mede e publica nodes continuamente. | nenhuma |
| `dashboard` | `nodes-dashboard` | UI web e API `/api/data`. | `8080` |

Detalhes importantes:

| Item | Valor | Motivo |
|---|---|---|
| Volume do monitor | `./data:/data:ro` | Seed entra somente leitura. |
| Redis URL interna | `redis://redis:6379/0` | Nome `redis` resolve na rede do Compose. |
| Redis URL no host | `redis://localhost:6380/0` | Porta 6380 esta exposta para bots locais. |
| Healthcheck do Redis | `redis-cli ping` | Garante Redis pronto antes dos servicos. |
| Healthcheck do monitor | `rpc:meta:eth.updated_at < 480s` | Detecta publicacao travada. |

## Variaveis do `.env`

| Chave | Default exemplo | Uso |
|---|---:|---|
| `REDIS_URL` | `redis://redis:6379/0` | Redis usado pelo monitor. |
| `NODES_FILE` | `/data/nodes_resultado.json` | Seed JSON principal. |
| `SEED_FALLBACK` | `/data/nodes_seed.txt` | Seed texto fallback com `ip:porta`. |
| `PROBE_INTERVAL` | `15` | Segundos entre ciclos. |
| `SAMPLES_PER_CYCLE` | `3` | Probes por node em cada ciclo. |
| `WINDOW` | `600` | Maximo de amostras retidas por node. |
| `CONCURRENCY` | `80` | Nodes testados em paralelo. |
| `TIMEOUT` | `5` | Timeout por chamada RPC. |
| `RECHECK_META` | `600` | Redetecta metadados a cada N segundos. |
| `RANK_WINDOW_SEC` | `900` | Janela temporal dos percentis e success-rate. |
| `MIN_WINDOW_SEC` | `900` | Tempo minimo para `mature=true`. |
| `MIN_SAMPLES` | `3` | Amostras minimas para entrar no ranking. |
| `MAX_LAG` | `50` | Lag maximo aceito. |
| `COHERENCE_BLOCKS` | `15000000` | Tolerancia contra bloco fora da mediana da rede. |
| `MIN_SUCCESS` | `0.5` | Taxa minima de sucesso para saudavel. |
| `TOP_N` | `20` | Tamanho de `rpc:best:{net}`. |
| `PUBLISH_EVERY` | `1` | Frequencia de publicacao em ciclos. |
| `TARGET_NETS` | `eth,bsc,base` | Redes publicadas. |
| `SESSION_RECYCLE_SEC` | `1800` | Recicla sessao HTTP. |
| `STALE_RESET_CYCLES` | `5` | Recicla sessao apos ciclos sem amostras. |
| `LOG_EVERY_N_ERRORS` | `200` | Reduz spam de erro repetido. |

## O que o monitor mede

| RPC | Transporte | Uso |
|---|---|---|
| `eth_chainId` | HTTP/WS | Detectar rede real. |
| `web3_clientVersion` | HTTP | Registrar cliente. |
| `eth_syncing` | HTTP | Excluir node em sync. |
| `eth_getBalance` no bloco `0x1` | HTTP | Heuristica de archive. |
| `eth_subscribe newHeads` | WS | Detectar push de blocos. |
| `eth_blockNumber` | HTTP/WS | Latencia e bloco atual. |

O monitor e read-only. Nao usa private key, seed phrase, assinatura, transacao ou broadcast.

## Chaves Redis

| Chave | Tipo | TTL | Uso |
|---|---|---:|---|
| `rpc:rank:{net}` | ZSET | sem TTL direto | Ranking por `p50`, `member=endpoint`. |
| `rpc:node:{net}:{endpoint}` | STRING JSON | 3600s | Payload completo do node. |
| `rpc:best:{net}` | STRING JSON | 3600s | Array dos `TOP_N` melhores. |
| `rpc:meta:{net}` | STRING JSON | 3600s | Resumo da rede. |

Comandos:

```powershell
docker exec nodes-redis redis-cli ZRANGE rpc:rank:eth 0 9 WITHSCORES
docker exec nodes-redis redis-cli --raw GET rpc:best:eth
docker exec nodes-redis redis-cli --raw GET rpc:meta:eth
```

## Payload de node

Campos publicados em `rpc:node:{net}:{endpoint}` e dentro de `rpc:best:{net}`:

| Campo | Significado |
|---|---|
| `endpoint` | `ip:porta`. |
| `ip` / `porta` | Endereco e porta testados. |
| `network` | `eth`, `bsc` ou `base`. |
| `chainId` | Chain real detectada. |
| `http_url` / `ws_url` | URLs disponiveis. |
| `transport` | `http`, `ws` ou `http/ws`. |
| `ws_subscribe` | Aceita `eth_subscribe(newHeads)`. |
| `archive` | Responde leitura historica no bloco `0x1`. |
| `syncing` | Resultado de `eth_syncing`. |
| `client` | Cliente RPC. |
| `block` | Ultimo bloco lido. |
| `lag` | Diferenca contra `block_ref`. |
| `coerente` | Bloco perto da mediana da rede. |
| `success_rate` | Sucesso recente das tentativas. |
| `observed_sec` | Tempo observado. |
| `latency_ms` | `n`, `p50`, `p90`, `p95`, `p99`, `min`, `max`, `media`. |
| `healthy` | Passou filtros operacionais. |
| `mature` | Observado por `MIN_WINDOW_SEC` e com pelo menos 20 amostras. |
| `provisional` | Saudavel, mas ainda warm-up. |
| `updated_at` | Timestamp da publicacao. |

Campos de `rpc:meta:{net}`:

| Campo | Significado |
|---|---|
| `updated_at` | Ultima publicacao. |
| `healthy` | Quantidade de nodes no ranking. |
| `total` | Nodes com amostras recentes. |
| `block_ref` | Bloco usado para lag. |
| `block_median` | Mediana dos blocos. |
| `incoerentes` | Nodes descartados por bloco fora da tolerancia. |

## Escolha segura de node por agent

Para um agent escolher um node:

1. Ler `rpc:best:{net}`.
2. Exigir `healthy == true`.
3. Exigir `coerente == true`.
4. Conferir `chainId`: ETH `1`, BSC `56`, Base `8453`.
5. Preferir `mature == true` para uso critico.
6. Ordenar por `latency_ms.p50`.
7. Desempatar por `latency_ms.p99`.
8. Se precisa evento ao vivo, exigir `ws_subscribe == true`.
9. Se precisa historico, exigir `archive == true`.
10. Para dinheiro/trade, subir `MIN_SUCCESS` no `.env` ou filtrar `success_rate >= 0.9` no consumidor.

## Auditoria

```powershell
docker exec nodes-monitor python auditoria.py
```

O script le os rankings publicados, chama `eth_chainId` em cada endpoint e falha se algum node estiver na rede errada. Isso e uma prova pontual do estado atual; o monitor continua revalidando depois via `RECHECK_META`.

## Troubleshooting rapido

```powershell
docker compose config --quiet
docker compose logs --tail=200 monitor
docker exec nodes-redis redis-cli --raw GET rpc:meta:eth
docker exec nodes-redis redis-cli ZRANGE rpc:rank:eth 0 10 WITHSCORES
docker exec nodes-monitor ls -lah /data
```

| Sintoma | Checar |
|---|---|
| Sem containers | Docker Desktop ligado e `docker compose up -d --build`. |
| Sem seed | `./data/nodes_resultado.json` ou `./data/nodes_seed.txt`. |
| Redis vazio | Logs do `monitor` e conexao `REDIS_URL`. |
| `healthy=0` | `lag`, `success_rate`, `syncing`, `coerente`, chainId. |
| Dashboard vazio | `/api/data`, container `nodes-dashboard`, rebuild. |

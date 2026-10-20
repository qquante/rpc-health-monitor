#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor continuo de nodes RPC (ETH / BSC / BASE).

Fica rodando, mede latencia de cada node numa JANELA DESLIZANTE de amostras,
calcula percentis (p50/p90/p95/p99), ranqueia por rede e publica no Redis.

Como mede (resumo das boas praticas):
  - probe = 1 chamada eth_blockNumber cronometrada (round-trip ponta-a-ponta)
  - guarda as amostras CRUAS numa janela (deque) -> percentil correto
  - dispara em agenda fixa (mitiga "coordinated omission")
  - so ranqueia node saudavel: success_rate alto, baixo lag de bloco, nao-sync
  - ordena por p50 (tipico), desempata por p99 (cauda)

Saida -> Redis (por rede: eth / bsc / base):
  rpc:rank:{net}            ZSET  score=p50_ms  member=endpoint   (top-N rapido)
  rpc:node:{net}:{endpoint} STRING json com a config completa do node
  rpc:best:{net}            STRING json array dos TOP_N melhores
  rpc:meta:{net}            STRING json {updated_at, healthy, total}
"""

import os
import re
import json
import math
import time
import asyncio
from collections import deque, defaultdict

import aiohttp
import websockets
import redis.asyncio as aioredis

# Config (tudo via variavel de ambiente -> ver .env)
REDIS_URL       = os.getenv("REDIS_URL", "redis://redis:6379/0")
NODES_FILE      = os.getenv("NODES_FILE", "/data/nodes_resultado.json")
SEED_FALLBACK   = os.getenv("SEED_FALLBACK", "/data/nodes_seed.txt")
PROBE_INTERVAL  = float(os.getenv("PROBE_INTERVAL", "15"))   # seg entre ciclos
SAMPLES_CYCLE   = int(os.getenv("SAMPLES_PER_CYCLE", "3"))   # probes/node/ciclo
WINDOW          = int(os.getenv("WINDOW", "600"))            # amostras mantidas (cap)
RANK_WINDOW_SEC = int(os.getenv("RANK_WINDOW_SEC", "900"))   # janela do percentil: 15 min
MIN_WINDOW_SEC  = int(os.getenv("MIN_WINDOW_SEC", "900"))    # so ranqueia apos observar 15 min
CONCURRENCY     = int(os.getenv("CONCURRENCY", "80"))
TIMEOUT         = float(os.getenv("TIMEOUT", "5"))
RECHECK_META    = float(os.getenv("RECHECK_META", "600"))    # re-detecta meta
TOP_N           = int(os.getenv("TOP_N", "20"))
MIN_SAMPLES     = int(os.getenv("MIN_SAMPLES", "3"))         # entra cedo no ranking
MAX_LAG         = int(os.getenv("MAX_LAG", "50"))            # lag max de bloco
COHERENCE_BLOCKS = int(os.getenv("COHERENCE_BLOCKS", "10000"))  # bloco fora disso da mediana = chain errada
MIN_SUCCESS     = float(os.getenv("MIN_SUCCESS", "0.5"))     # success_rate min p/ aparecer
PUBLISH_EVERY   = int(os.getenv("PUBLISH_EVERY", "1"))       # publica a cada N ciclos
TARGET_NETS     = set(os.getenv("TARGET_NETS", "eth,bsc,base").split(","))

# ---- Auto-heal / antitravamento ----
SESSION_RECYCLE_SEC  = int(os.getenv("SESSION_RECYCLE_SEC", "1800"))  # recria session a cada 30 min
STALE_RESET_CYCLES   = int(os.getenv("STALE_RESET_CYCLES", "5"))      # se N ciclos sem amostras, recria session
LOG_EVERY_N_ERRORS   = int(os.getenv("LOG_EVERY_N_ERRORS", "200"))    # loga 1 a cada N erros do mesmo tipo

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
NETS = {1: "eth", 56: "bsc", 8453: "base"}   # chainId -> slug do redis
RE_IPPORT = re.compile(r'(?:(?:wss?|https?)://)?((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})')


def now():
    return time.time()


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# contador de erros por tipo, pra logar 1 a cada N (nao inundar log)
_err_counters = defaultdict(int)


def log_err(scope, e):
    """Loga uma excecao com rate limit (1 a cada LOG_EVERY_N_ERRORS por escopo)."""
    key = f"{scope}:{type(e).__name__}"
    _err_counters[key] += 1
    if _err_counters[key] == 1 or _err_counters[key] % LOG_EVERY_N_ERRORS == 0:
        log(f"[{scope}] {type(e).__name__}: {str(e)[:120]}  (#{_err_counters[key]})")


def parse_hex(v):
    try:
        return int(v, 16) if isinstance(v, str) and v.startswith("0x") else None
    except (ValueError, TypeError):
        return None


def recentes(deq, win_sec):
    """Valores do deque [(ts, valor), ...] dentro dos ultimos win_sec segundos."""
    lim = now() - win_sec
    return [v for (t, v) in deq if t >= lim]


def registra(st, ok, lat=None):
    """Registra 1 tentativa (com timestamp) na janela do node."""
    t = now()
    if st.get("first_ts") is None:
        st["first_ts"] = t
    st["attempts"].append((t, 1 if ok else 0))
    if ok and lat is not None:
        st["samples"].append((t, lat))


# Percentis (metodo nearest-rank, em cima das amostras cruas)
def percentil(ordenado, p):
    n = len(ordenado)
    if n == 0:
        return None
    k = math.ceil(p / 100 * n)
    return ordenado[min(n - 1, max(0, k - 1))]


def estatisticas(amostras):
    s = sorted(amostras)
    n = len(s)
    if n == 0:
        return {"n": 0, "p50": None, "p90": None, "p95": None,
                "p99": None, "min": None, "max": None, "media": None}
    rnd = lambda v: round(v, 1) if v is not None else None
    return {
        "n": n,
        "p50": rnd(percentil(s, 50)), "p90": rnd(percentil(s, 90)),
        "p95": rnd(percentil(s, 95)), "p99": rnd(percentil(s, 99)),
        "min": rnd(s[0]), "max": rnd(s[-1]),
        "media": round(sum(s) / n, 1),
    }


# JSON-RPC helpers
def rpc_body(metodo, params=None, _id=1):
    return {"jsonrpc": "2.0", "id": _id, "method": metodo, "params": params or []}


async def http_call(session, url, metodo, params=None):
    async with session.post(url, json=rpc_body(metodo, params),
                            timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
        data = await r.json(content_type=None)
        return data.get("result")


# Deteccao de metadados (chainId, cliente, archive, ws, syncing, transporte)
# roda de vez em quando (RECHECK_META), nao todo ciclo
async def detecta_http(session, ip, porta):
    url = f"http://{ip}:{porta}"
    try:
        cid = parse_hex(await http_call(session, url, "eth_chainId"))
        if cid is None:
            return None
        out = {"http_url": url, "chainId": cid, "http": True}
        try:
            out["client"] = (await http_call(session, url, "web3_clientVersion") or "")[:80]
        except Exception:
            out["client"] = ""
        try:
            sy = await http_call(session, url, "eth_syncing")
            out["syncing"] = bool(sy) and sy is not False
        except Exception:
            out["syncing"] = None
        try:
            bal = await http_call(session, url, "eth_getBalance", [ZERO_ADDR, "0x1"])
            out["archive"] = bal is not None
        except Exception:
            out["archive"] = False
        return out
    except Exception:
        return None


async def _ws_meta(url):
    async with websockets.connect(url, close_timeout=2, ping_timeout=None) as ws:
        await ws.send(json.dumps(rpc_body("eth_chainId")))
        cid = parse_hex(json.loads(await ws.recv()).get("result"))
        if cid is None:
            return None
        out = {"ws_url": url, "chainId": cid, "ws": True, "ws_subscribe": False}
        try:
            await ws.send(json.dumps(rpc_body("eth_subscribe", ["newHeads"], 2)))
            sub = json.loads(await asyncio.wait_for(ws.recv(), timeout=TIMEOUT))
            out["ws_subscribe"] = isinstance(sub.get("result"), str)
        except Exception:
            pass
        return out


async def detecta_ws(ip, porta):
    url = f"ws://{ip}:{porta}"
    try:
        return await asyncio.wait_for(_ws_meta(url), timeout=TIMEOUT + 3)
    except Exception:
        return None


async def recheca_meta(session, st):
    h, w = await asyncio.gather(
        detecta_http(session, st["ip"], st["porta"]),
        detecta_ws(st["ip"], st["porta"]),
        return_exceptions=True,
    )
    h = h if isinstance(h, dict) else None
    w = w if isinstance(w, dict) else None

    st["http"] = bool(h)
    st["ws"] = bool(w)
    st["ws_subscribe"] = w.get("ws_subscribe", False) if w else False
    # SEMPRE re-valida o chainId pelo transporte que respondeu (nunca confia no seed).
    # HTTP tem prioridade; se so o WS responde, usa o do WS. Isso evita node WS-only
    # ficar travado num chainId errado vindo do seed.
    novo_chain = h["chainId"] if h else (w["chainId"] if w else None)
    if novo_chain is not None:
        if st.get("chainId") is not None and st["chainId"] != novo_chain:
            log(f"[chainId] {st['endpoint']} mudou {st['chainId']} -> {novo_chain} "
                f"(corrigindo classificacao)")
        st["chainId"] = novo_chain
    if h:
        st.update({k: h[k] for k in ("http_url", "client", "syncing", "archive") if k in h})
    if w:
        st["ws_url"] = w["ws_url"]
    # transporte primario pra medir latencia: HTTP de preferencia, senao WS
    st["primary"] = "http" if st["http"] else ("ws" if st["ws"] else None)
    st["meta_ok"] = bool(st["primary"])
    st["last_meta"] = now()


# Probe de latencia (1 ciclo) -> adiciona SAMPLES_CYCLE amostras
async def probe_http(session, st):
    url = st["http_url"]
    for _ in range(SAMPLES_CYCLE):
        t0 = time.perf_counter()
        try:
            res = await http_call(session, url, "eth_blockNumber")
            lat = (time.perf_counter() - t0) * 1000
            if res is not None:
                st["block"] = parse_hex(res)
                registra(st, True, lat)
            else:
                registra(st, False)
        except Exception as e:
            log_err("probe_http", e)
            registra(st, False)


async def probe_ws(st):
    url = st["ws_url"]

    async def _inner():
        async with websockets.connect(url, close_timeout=2, ping_timeout=None) as ws:
            for _ in range(SAMPLES_CYCLE):
                t0 = time.perf_counter()
                await ws.send(json.dumps(rpc_body("eth_blockNumber")))
                res = json.loads(await ws.recv()).get("result")
                lat = (time.perf_counter() - t0) * 1000
                if res is not None:
                    st["block"] = parse_hex(res)
                    registra(st, True, lat)
                else:
                    registra(st, False)
    try:
        await asyncio.wait_for(_inner(), timeout=TIMEOUT + 3)
    except Exception as e:
        log_err("probe_ws", e)
        for _ in range(SAMPLES_CYCLE):
            registra(st, False)


async def probe_node(session, st, sem):
    async with sem:
        # re-detecta metadados se passou do intervalo
        if not st.get("last_meta") or (now() - st["last_meta"]) >= RECHECK_META:
            await recheca_meta(session, st)
        if not st.get("primary"):
            registra(st, False)
            return
        if st["primary"] == "http":
            await probe_http(session, st)
        else:
            await probe_ws(st)


# Publicacao no Redis
def success_rate(st):
    a = recentes(st["attempts"], RANK_WINDOW_SEC)
    return round(sum(a) / len(a), 3) if a else 0.0


async def publica(r, nodes):
    por_rede = defaultdict(list)

    # 1) calcula stats (janela de tempo de 15 min) e agrupa por rede
    for st in nodes.values():
        net = NETS.get(st.get("chainId"))
        if net not in TARGET_NETS:
            continue
        lats = recentes(st["samples"], RANK_WINDOW_SEC)
        if not lats:
            continue
        stt = estatisticas(lats)
        por_rede[net].append((st, stt))

    pipe = r.pipeline(transaction=False)
    resumo = {}

    for net in TARGET_NETS:
        itens = por_rede.get(net, [])
        # --- referencia de bloco ROBUSTA: mediana + descarte de outlier ---
        # 1 impostor de outra chain (bloco absurdamente diferente) NAO pode virar
        # a referencia e derrubar a rede inteira. Usa mediana e descarta incoerentes.
        blocos = sorted(st["block"] for st, _ in itens if st.get("block"))
        if blocos:
            mediana = blocos[len(blocos) // 2]
            coerentes = [b for b in blocos if abs(b - mediana) <= COHERENCE_BLOCKS]
            bloco_ref = max(coerentes) if coerentes else mediana
        else:
            mediana = bloco_ref = None

        saudaveis = []
        incoerentes = 0
        for st, stt in itens:
            sr = success_rate(st)
            observed = now() - (st.get("first_ts") or now())
            blk = st.get("block")
            # node coerente = bloco bate com a mediana da rede (mesma chain de verdade)
            coerente = (blk is not None and mediana is not None
                        and abs(blk - mediana) <= COHERENCE_BLOCKS)
            if not coerente and blk is not None and mediana is not None:
                incoerentes += 1
                log_err("incoerente",
                        Exception(f"{st['endpoint']} bloco {blk} vs mediana {mediana} "
                                  f"da rede {net} (chainId {st.get('chainId')})"))
            lag = (bloco_ref - blk) if (bloco_ref and blk and coerente) else None
            # entra no ranking JA no 1o ciclo: basta ter MIN_SAMPLES, ser coerente e responder
            ok = (coerente
                  and stt["n"] >= MIN_SAMPLES
                  and sr >= MIN_SUCCESS
                  and lag is not None and lag <= MAX_LAG
                  and not st.get("syncing"))
            # "mature" = confiavel pra producao (15 min observado, mais amostras)
            mature = (observed >= MIN_WINDOW_SEC and stt["n"] >= 20)
            cfg = {
                "endpoint": st["endpoint"],
                "ip": st["ip"], "porta": st["porta"],
                "network": net, "chainId": st.get("chainId"),
                "http_url": st.get("http_url"), "ws_url": st.get("ws_url"),
                "transport": "/".join([t for t, on in
                                       (("http", st["http"]), ("ws", st["ws"])) if on]),
                "ws_subscribe": st.get("ws_subscribe", False),
                "archive": st.get("archive", False),
                "syncing": st.get("syncing"),
                "client": st.get("client", ""),
                "block": blk, "lag": lag, "coerente": coerente,
                "success_rate": sr,
                "observed_sec": int(observed),
                "latency_ms": stt,
                "healthy": ok,
                "mature": mature,                       # true = ja tem 15 min de hist (confiavel pra prod)
                "provisional": ok and not mature,       # ainda em "warm-up", mas ja serve
                "updated_at": int(now()),
            }
            if ok:
                saudaveis.append((stt["p50"], stt["p99"], cfg))

        # ordena por p50, desempata por p99
        saudaveis.sort(key=lambda x: (x[0], x[1] if x[1] is not None else 9e9))

        rank_key = f"rpc:rank:{net}"
        pipe.delete(rank_key)
        best = []
        for p50, _p99, cfg in saudaveis:
            ep = cfg["endpoint"]
            pipe.set(f"rpc:node:{net}:{ep}", json.dumps(cfg), ex=3600)
            pipe.zadd(rank_key, {ep: p50})
            best.append(cfg)
        pipe.set(f"rpc:best:{net}", json.dumps(best[:TOP_N]), ex=3600)
        pipe.set(f"rpc:meta:{net}", json.dumps(
            {"updated_at": int(now()), "healthy": len(saudaveis),
             "total": len(itens), "block_ref": bloco_ref,
             "block_median": mediana, "incoerentes": incoerentes}), ex=3600)
        resumo[net] = (len(saudaveis), len(itens),
                       saudaveis[0][0] if saudaveis else None)

    await pipe.execute()
    return resumo


# Carrega a lista inicial de nodes (json do teste OU txt de ip:porta)
def carrega_seed():
    pares = {}  # endpoint -> (ip, porta, chainId opcional)

    def add(ip, porta, chain=None):
        ep = f"{ip}:{porta}"
        if ep not in pares:
            pares[ep] = (ip, int(porta), chain)

    if os.path.exists(NODES_FILE) and NODES_FILE.endswith(".json"):
        try:
            with open(NODES_FILE, "r", encoding="utf-8") as f:
                for r in json.load(f):
                    ip = r.get("ip")
                    porta = r.get("porta")
                    if ip and porta:
                        add(ip, porta, r.get("chainId"))
            log(f"seed: {len(pares)} nodes de {NODES_FILE}")
        except Exception as e:
            log("falha lendo json seed:", e)

    for caminho in (NODES_FILE, SEED_FALLBACK):
        if os.path.exists(caminho) and not caminho.endswith(".json"):
            with open(caminho, "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    for ip, porta in RE_IPPORT.findall(linha):
                        add(ip, porta)

    nodes = {}
    for ep, (ip, porta, chain) in pares.items():
        nodes[ep] = {
            "endpoint": ep, "ip": ip, "porta": porta,
            "chainId": chain,
            "samples": deque(maxlen=WINDOW),
            "attempts": deque(maxlen=WINDOW),
            "http": False, "ws": False, "ws_subscribe": False,
            "primary": None, "last_meta": 0, "block": None,
            "first_ts": None,
        }
    return nodes


async def main():
    nodes = carrega_seed()
    if not nodes:
        log(f"Nenhum node em {NODES_FILE} / {SEED_FALLBACK}. Monte o seed antes.")
        return
    log(f"Iniciando monitor: {len(nodes)} nodes | intervalo={PROBE_INTERVAL}s "
        f"| amostras/ciclo={SAMPLES_CYCLE} | janela={WINDOW}")

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.ping()
        log("Redis conectado:", REDIS_URL)
    except Exception as e:
        log("ERRO conectando no Redis:", e)
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    ciclo = 0
    session = None
    session_started = 0.0
    stale_streak = 0   # ciclos seguidos com 0 amostras totais

    def nova_sessao():
        conn = aiohttp.TCPConnector(
            limit=CONCURRENCY * 2, ttl_dns_cache=300,
            force_close=False, enable_cleanup_closed=True,
        )
        return aiohttp.ClientSession(connector=conn), now()

    session, session_started = nova_sessao()
    log("ClientSession criada")

    try:
        while True:
            t0 = now()

            # --- auto-heal: recicla a session de tempos em tempos ou se travou ---
            if (now() - session_started) >= SESSION_RECYCLE_SEC or stale_streak >= STALE_RESET_CYCLES:
                motivo = "stale" if stale_streak >= STALE_RESET_CYCLES else "agendado"
                log(f"[auto-heal] recriando ClientSession ({motivo}, streak={stale_streak})")
                try:
                    await session.close()
                except Exception as e:
                    log_err("session-close", e)
                session, session_started = nova_sessao()
                stale_streak = 0
                _err_counters.clear()  # zera contagem de erros pra ver os novos

            # --- ciclo de medicao ---
            try:
                await asyncio.gather(
                    *(probe_node(session, st, sem) for st in nodes.values()),
                    return_exceptions=True,
                )
            except Exception as e:
                log_err("gather", e)

            # detecta travamento: ninguem registrou amostra nova nos ultimos N seg?
            recent_total = sum(len(recentes(st["samples"], PROBE_INTERVAL * 2))
                               for st in nodes.values())
            if recent_total == 0:
                stale_streak += 1
            else:
                stale_streak = 0

            if ciclo % PUBLISH_EVERY == 0:
                try:
                    resumo = await publica(r, nodes)
                    partes = []
                    for net in ("eth", "bsc", "base"):
                        if net in resumo:
                            h, t, melhor = resumo[net]
                            ml = f"{melhor:.0f}ms" if melhor else "-"
                            partes.append(f"{net}:{h}/{t} (p50min {ml})")
                    extra = f" | recent={recent_total}"
                    if stale_streak:
                        extra += f" | STALE={stale_streak}"
                    log(f"ciclo {ciclo} | " + " | ".join(partes) + extra)
                except Exception as e:
                    log_err("publica", e)

            ciclo += 1
            resto = PROBE_INTERVAL - (now() - t0)
            if resto > 0:
                await asyncio.sleep(resto)
    finally:
        if session:
            try:
                await session.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

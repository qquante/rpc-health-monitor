#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auditoria do pool: para cada node publicado no Redis, pergunta o chainId REAL
e confere se ele esta na rede certa. E a PROVA de que o ranking esta confiavel.

Saida esperada (tudo certo):
    ETH : 118 certos | 0 errados | 0 sem resposta
    BSC :   7 certos | 0 errados | ...
    BASE:  24 certos | 0 errados | ...

Sai com codigo != 0 se achar QUALQUER node na rede errada (da pra usar em alerta/cron).

Rodar de dentro do container:
    docker exec nodes-monitor python auditoria.py
Ou do seu PC (precisa: pip install redis websockets):
    python auditoria.py            # usa redis://localhost:6380
"""
import os
import sys
import json
import asyncio
import urllib.request

import websockets
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380/0")
NETS = {"eth": 1, "bsc": 56, "base": 8453}
NOMES = {1: "ETH", 56: "BSC", 8453: "BASE", None: "morto/sem resposta"}

r = redis.from_url(REDIS_URL, decode_responses=True)
sem = asyncio.Semaphore(40)


async def chainid_ws(ip, porta):
    try:
        async with websockets.connect(f"ws://{ip}:{porta}",
                                      close_timeout=2, ping_timeout=None) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1,
                                      "method": "eth_chainId", "params": []}))
            cid = json.loads(await asyncio.wait_for(ws.recv(), timeout=5)).get("result")
            return int(cid, 16) if cid else None
    except Exception:
        return None


def chainid_http(ip, porta):
    try:
        req = urllib.request.Request(
            f"http://{ip}:{porta}",
            data=json.dumps({"jsonrpc": "2.0", "id": 1,
                             "method": "eth_chainId", "params": []}).encode(),
            headers={"Content-Type": "application/json"})
        cid = json.loads(urllib.request.urlopen(req, timeout=5).read()).get("result")
        return int(cid, 16) if cid else None
    except Exception:
        return None


async def check(ep):
    async with sem:
        ip, porta = ep.rsplit(":", 1)
        real = await asyncio.get_event_loop().run_in_executor(None, chainid_http, ip, porta)
        if real is None:
            real = await chainid_ws(ip, porta)
        return ep, real


async def main():
    total_errados = 0
    print(f"Auditoria do pool (Redis: {REDIS_URL})\n" + "=" * 50)
    for net, esperado in NETS.items():
        eps = r.zrange(f"rpc:rank:{net}", 0, -1)
        if not eps:
            print(f"{net.upper():4}: (vazio)")
            continue
        res = await asyncio.gather(*(check(e) for e in eps))
        certos = [e for e, c in res if c == esperado]
        errados = [(e, c) for e, c in res if c is not None and c != esperado]
        mortos = [e for e, c in res if c is None]
        total_errados += len(errados)
        print(f"{net.upper():4}: {len(certos):3} certos | {len(errados):3} ERRADOS "
              f"| {len(mortos):3} sem resposta")
        for e, c in errados:
            print(f"       !! {e} -> na verdade e {NOMES.get(c, f'chainId {c}')}")
    print("=" * 50)
    if total_errados == 0:
        print("OK: todos os nodes estao na rede certa.")
    else:
        print(f"FALHA: {total_errados} node(s) na rede ERRADA.")
    return total_errados


if __name__ == "__main__":
    sys.exit(1 if asyncio.run(main()) else 0)

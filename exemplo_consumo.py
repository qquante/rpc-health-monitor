#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exemplo: como um bot pega o MELHOR node de uma rede no Redis.

Rode (no seu PC, com o stack de pe):
    pip install redis
    python exemplo_consumo.py eth
"""
import sys
import json
import os
import redis

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6380/0"), decode_responses=True)

net = sys.argv[1] if len(sys.argv) > 1 else "eth"   # eth | bsc | base

# --- Forma 1: top-N rapido por latencia (ZSET, score = p50 em ms) ---
print(f"== TOP 5 {net.upper()} por p50 (ZSET) ==")
for ep, p50 in r.zrange(f"rpc:rank:{net}", 0, 4, withscores=True):
    print(f"  {ep:<24} p50={p50:.0f}ms")

# --- Forma 2: config completa do melhor node (pra conectar de verdade) ---
melhor_ep = r.zrange(f"rpc:rank:{net}", 0, 0)
if melhor_ep:
    cfg = json.loads(r.get(f"rpc:node:{net}:{melhor_ep[0]}"))
    print(f"\n== MELHOR {net.upper()} ==")
    print(f"  endpoint : {cfg['endpoint']}")
    print(f"  http_url : {cfg['http_url']}")
    print(f"  ws_url   : {cfg['ws_url']}")
    print(f"  p50/p99  : {cfg['latency_ms']['p50']}ms / {cfg['latency_ms']['p99']}ms")
    print(f"  bloco    : {cfg['block']}  (lag {cfg['lag']})")
    print(f"  ws_push  : {cfg['ws_subscribe']}  archive: {cfg['archive']}")
    print(f"  cliente  : {cfg['client']}")

# --- Forma 3: lista inteira dos melhores de uma vez (1 GET) ---
best = r.get(f"rpc:best:{net}")
if best:
    print(f"\n== {net.upper()}: {len(json.loads(best))} nodes saudaveis em rpc:best:{net} ==")

meta = r.get(f"rpc:meta:{net}")
if meta:
    print("meta:", meta)

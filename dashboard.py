#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard web do ranking de nodes (le do Redis, mostra ao vivo).

Sobe um servidor HTTP simples (aiohttp):
  GET /            -> pagina HTML que atualiza sozinha
  GET /api/data    -> JSON com o ranking de cada rede (eth/bsc/base)

Acesse no navegador:  http://localhost:8080
"""
import os
import json

from aiohttp import web
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
PORT = int(os.getenv("PORT", "8080"))
NETS = ["eth", "bsc", "base"]

r = aioredis.from_url(REDIS_URL, decode_responses=True)


async def api_data(request):
    out = {}
    for net in NETS:
        # ZSET ja vem ordenado por p50 (menor latencia primeiro)
        eps = await r.zrange(f"rpc:rank:{net}", 0, -1)
        nodes = []
        if eps:
            pipe = r.pipeline()
            for ep in eps:
                pipe.get(f"rpc:node:{net}:{ep}")
            vals = await pipe.execute()
            nodes = [json.loads(v) for v in vals if v]
        meta = await r.get(f"rpc:meta:{net}")
        out[net] = {"nodes": nodes, "meta": json.loads(meta) if meta else {}}
    return web.json_response(out)


async def index(request):
    return web.Response(text=HTML, content_type="text/html")


HTML = r"""<!DOCTYPE html>
<html lang="pt-br"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Node Ranking</title>
<style>
 :root{--bg:#0d1117;--card:#161b22;--line:#21262d;--mut:#8b949e;--blue:#58a6ff}
 *{box-sizing:border-box}
 body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:var(--bg);color:#e6edf3}
 header{padding:18px 24px;border-bottom:1px solid var(--line);display:flex;
        align-items:center;gap:16px;flex-wrap:wrap;position:sticky;top:0;background:var(--bg);z-index:5}
 h1{margin:0;font-size:20px}
 .upd{color:var(--mut);font-size:13px}
 .tabs{display:flex;gap:8px;margin-left:auto}
 .tab{padding:7px 16px;border:1px solid var(--line);border-radius:8px;cursor:pointer;
      background:var(--card);font-weight:600;font-size:14px}
 .tab.on{border-color:var(--blue);color:var(--blue)}
 main{padding:20px 24px}
 .cards{display:flex;gap:14px;margin-bottom:18px;flex-wrap:wrap}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 18px;min-width:130px}
 .card .n{font-size:24px;font-weight:700}.card .l{color:var(--mut);font-size:12px}
 table{border-collapse:collapse;width:100%;font-size:13px}
 th,td{padding:6px 9px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
 th{background:var(--card);position:sticky;top:64px;cursor:pointer;user-select:none}
 th:hover{color:var(--blue)}
 tr:hover td{background:var(--card)}
 .ep{font-family:Consolas,monospace;color:var(--blue)}
 .cli{color:var(--mut);font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis}
 .pill{padding:1px 7px;border-radius:10px;font-size:11px;font-weight:600}
 .net-empty{color:var(--mut);padding:40px;text-align:center}
 .dot{height:8px;width:8px;border-radius:50%;display:inline-block;margin-right:5px}
</style></head><body>
<header>
  <h1>🛰️ Node Ranking</h1>
  <span class="upd" id="upd">carregando…</span>
  <div class="tabs" id="tabs"></div>
</header>
<main>
  <div class="cards" id="cards"></div>
  <div id="tbl"></div>
</main>
<script>
const NETS=["eth","bsc","base"];
let cur="eth", data={}, sortKey="p50", sortAsc=true;

function color(p50){ if(p50==null)return"#8b949e"; if(p50<150)return"#1a7f37"; if(p50<400)return"#9a6700"; return"#b42318"; }
function sim(b){ return b?"✅":"—"; }
function ago(ts){ if(!ts)return"-"; const s=Math.max(0,Math.floor(Date.now()/1000-ts));
  return s<60?s+"s atrás":Math.floor(s/60)+"min atrás"; }

function val(n,k){
  const L=n.latency_ms||{};
  if(["p50","p90","p95","p99","min","max","n"].includes(k))return L[k];
  if(k==="success")return n.success_rate;
  if(k==="observed")return n.observed_sec;
  if(k==="lag")return n.lag;
  if(k==="block")return n.block;
  return n[k];
}

function render(){
  // tabs
  document.getElementById("tabs").innerHTML=NETS.map(net=>{
    const c=(data[net]&&data[net].nodes)?data[net].nodes.length:0;
    return `<div class="tab ${net===cur?'on':''}" onclick="sel('${net}')">${net.toUpperCase()} <span style="color:#8b949e">${c}</span></div>`;
  }).join("");

  const d=data[cur]||{nodes:[],meta:{}};
  const m=d.meta||{};
  document.getElementById("upd").textContent="atualizado "+ago(m.updated_at);

  // cards
  const ns=d.nodes||[];
  const wsp=ns.filter(n=>n.ws_subscribe).length;
  const arc=ns.filter(n=>n.archive).length;
  const mat=ns.filter(n=>n.mature).length;
  const warm=ns.length-mat;
  const best=ns.length?val(ns[0],"p50"):null;
  document.getElementById("cards").innerHTML=`
    <div class="card"><div class="n">${m.healthy??ns.length}</div><div class="l">saudáveis</div></div>
    <div class="card"><div class="n" style="color:#7ee787">${mat}</div><div class="l">mature (15min+)</div></div>
    <div class="card"><div class="n" style="color:#f0b429">${warm}</div><div class="l">warm (provisório)</div></div>
    <div class="card"><div class="n" style="color:${color(best)}">${best!=null?best+'ms':'-'}</div><div class="l">melhor p50</div></div>
    <div class="card"><div class="n">${wsp}</div><div class="l">WS push</div></div>
    <div class="card"><div class="n">${arc}</div><div class="l">archive</div></div>
    <div class="card"><div class="n">${m.block_ref??'-'}</div><div class="l">bloco ref.</div></div>`;

  if(!ns.length){ document.getElementById("tbl").innerHTML='<div class="net-empty">Sem nodes saudáveis ainda — aguarde 1-2 ciclos (~30s) e os primeiros aparecem como WARM. Após 15 min viram MATURE.</div>'; return; }

  const cols=[["#",null],["Endpoint","endpoint"],["status",null],["p50","p50"],["p90","p90"],["p95","p95"],["p99","p99"],
    ["min","min"],["max","max"],["amostras","n"],["sucesso","success"],["obs","observed"],["bloco","block"],["lag","lag"],
    ["WS push","ws_subscribe"],["archive","archive"],["transp","transport"],["cliente","client"]];

  let rows=[...ns];
  if(sortKey){ rows.sort((a,b)=>{ let x=val(a,sortKey),y=val(b,sortKey);
    if(x==null)x=Infinity; if(y==null)y=Infinity;
    if(typeof x==="string")return sortAsc?(""+x).localeCompare(y):(""+y).localeCompare(x);
    return sortAsc?x-y:y-x; }); }

  let h='<table><thead><tr>'+cols.map(c=>`<th onclick="setSort('${c[1]}')">${c[0]}</th>`).join("")+'</tr></thead><tbody>';
  rows.forEach((n,i)=>{
    const L=n.latency_ms||{};
    const badge = n.mature
        ? '<span class="pill" style="background:#1f3d2a;color:#7ee787">MATURE</span>'
        : '<span class="pill" style="background:#3b2e0e;color:#f0b429">WARM</span>';
    h+=`<tr>
      <td>${i+1}</td>
      <td class="ep">${n.endpoint}</td>
      <td>${badge}</td>
      <td><b style="color:${color(L.p50)}">${L.p50??'-'}</b></td>
      <td>${L.p90??'-'}</td><td>${L.p95??'-'}</td>
      <td style="color:#d29922">${L.p99??'-'}</td>
      <td>${L.min??'-'}</td><td>${L.max??'-'}</td>
      <td>${L.n??'-'}</td>
      <td>${n.success_rate!=null?(n.success_rate*100).toFixed(0)+'%':'-'}</td>
      <td>${n.observed_sec!=null?Math.floor(n.observed_sec/60)+'m':'-'}</td>
      <td>${n.block??'-'}</td>
      <td>${n.lag===0?'0':(n.lag!=null?'+'+n.lag:'-')}</td>
      <td>${sim(n.ws_subscribe)}</td>
      <td>${sim(n.archive)}</td>
      <td>${n.transport||'-'}</td>
      <td class="cli" title="${(n.client||'').replace(/"/g,'')}">${n.client||''}</td>
    </tr>`;
  });
  h+='</tbody></table>';
  document.getElementById("tbl").innerHTML=h;
}

function sel(net){cur=net;render();}
function setSort(k){ if(!k)return; if(sortKey===k)sortAsc=!sortAsc; else{sortKey=k;sortAsc=true;} render(); }

async function load(){
  try{ const res=await fetch("/api/data"); data=await res.json(); render(); }
  catch(e){ document.getElementById("upd").textContent="erro ao ler /api/data"; }
}
load(); setInterval(load,5000);
</script>
</body></html>"""


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/api/data", api_data)

if __name__ == "__main__":
    print(f"Dashboard em http://localhost:{PORT}  (Redis: {REDIS_URL})", flush=True)
    web.run_app(app, port=PORT, print=None)

"""Transaction submission over several routes at once (same signed bytes => at most one lands).

Every route speaks JSON-RPC `sendTransaction` (Triton RPC/Cascade, Jito block
engine, Helius Sender, most third-party relays), so one class covers them;
per-route URL/headers come from config. Latency and errors are recorded per
route so the live report can show which route actually lands our trades.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

log = logging.getLogger("send")


@dataclass
class Route:
    name: str
    url: str
    headers: dict = field(default_factory=dict)
    # "jsonrpc" (sendTransaction) or "jito_bundle" (sendBundle with one tx)
    kind: str = "jsonrpc"
    enabled: bool = True
    sides: tuple = ("buy", "sell")
    warm_url: Optional[str] = None  # GET endpoint for keep-alive warming (e.g. Helius /ping)
    stats: dict = field(default_factory=lambda: {"sent": 0, "ok": 0, "err": 0, "lat_ms": []})


class MultiSender:
    def __init__(self, routes: list[Route], timeout: float = 3.0):
        self.routes = [r for r in routes if r.enabled]
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        conn = aiohttp.TCPConnector(limit=64, ttl_dns_cache=300, keepalive_timeout=120)
        self.session = aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=self.timeout),
                                             trust_env=True)
        await self.warm()

    async def warm(self):
        """Open keep-alive connections so the first real send does not pay TLS setup."""
        async def ping(r: Route):
            try:
                if r.warm_url:
                    async with self.session.get(r.warm_url) as resp:
                        await resp.read()
                elif r.kind == "jsonrpc" and "helius" not in r.url:
                    async with self.session.post(r.url, json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
                                                 headers=r.headers) as resp:
                        await resp.read()
            except Exception:
                pass
        await asyncio.gather(*[ping(r) for r in self.routes])

    async def close(self):
        if self.session:
            await self.session.close()

    async def _send_one(self, r: Route, b64: str) -> tuple[str, bool, float, str]:
        if r.kind == "jito_bundle":
            body = {"jsonrpc": "2.0", "id": 1, "method": "sendBundle", "params": [[b64], {"encoding": "base64"}]}
        else:
            body = {"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                    "params": [b64, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}]}
        t0 = time.perf_counter()
        r.stats["sent"] += 1
        try:
            async with self.session.post(r.url, json=body, headers=r.headers) as resp:
                txt = await resp.text()
            lat = (time.perf_counter() - t0) * 1000
            ok = resp.status == 200 and '"error"' not in txt
            r.stats["ok" if ok else "err"] += 1
            r.stats["lat_ms"].append(lat)
            if len(r.stats["lat_ms"]) > 500:
                r.stats["lat_ms"] = r.stats["lat_ms"][-500:]
            return r.name, ok, lat, txt[:300]
        except Exception as e:
            r.stats["err"] += 1
            return r.name, False, (time.perf_counter() - t0) * 1000, repr(e)[:300]

    async def send(self, tx_bytes: bytes, side: str = "buy") -> list[tuple[str, bool, float, str]]:
        b64 = base64.b64encode(tx_bytes).decode()
        routes = [r for r in self.routes if side in r.sides]
        return await asyncio.gather(*[self._send_one(r, b64) for r in routes])

    async def keepalive(self, every_s: float = 25.0):
        while True:
            await asyncio.sleep(every_s)
            await self.warm()

    def summary(self) -> dict:
        out = {}
        for r in self.routes:
            lat = sorted(r.stats["lat_ms"])
            p50 = lat[len(lat) // 2] if lat else None
            out[r.name] = {"sent": r.stats["sent"], "ok": r.stats["ok"], "err": r.stats["err"],
                           "p50_ms": round(p50, 1) if p50 else None}
        return out

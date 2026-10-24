"""Async JSON-RPC client: several endpoints, per-endpoint pacing, retries."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

import aiohttp


class RpcError(RuntimeError):
    pass


class RPC:
    def __init__(self, endpoints: list[str], rps_per_endpoint: float | list[float] = 10.0, concurrency: int = 24,
                 timeout: float = 40.0):
        rps = rps_per_endpoint if isinstance(rps_per_endpoint, list) else [rps_per_endpoint] * len(endpoints)
        self.eps = [{"url": u, "rps": r, "next": 0.0, "ok": 0, "err": 0} for u, r in zip(endpoints, rps)]
        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None
        self.calls = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout), trust_env=True)
        return self

    async def __aexit__(self, *a):
        if self.session:
            await self.session.close()

    async def _pick(self):
        while True:
            now = time.monotonic()
            ep = min(self.eps, key=lambda e: e["next"])
            if ep["next"] <= now:
                ep["next"] = now + 1.0 / ep["rps"]
                return ep
            await asyncio.sleep(ep["next"] - now)

    async def call(self, method: str, params: list, retries: int = 8) -> Any:
        last = None
        async with self.sem:
            for att in range(retries):
                ep = await self._pick()
                try:
                    async with self.session.post(ep["url"], json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                                  "params": params}) as resp:
                        if resp.status in (429, 502, 503, 504):
                            ep["next"] = time.monotonic() + 1.5 + att
                            last = f"http {resp.status}"
                            continue
                        j = await resp.json(content_type=None)
                    self.calls += 1
                    if "error" in j:
                        msg = json.dumps(j["error"])
                        low = msg.lower()
                        if "429" in msg or "rate" in low or "too many" in low or "timeout" in low:
                            ep["next"] = time.monotonic() + 1.5 + att
                            last = msg
                            continue
                        raise RpcError(msg)
                    ep["ok"] += 1
                    return j["result"]
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, ValueError) as ex:
                    ep["err"] += 1
                    ep["next"] = time.monotonic() + 0.5 + att
                    last = repr(ex)
                    continue
        raise RpcError(f"{method} failed after {retries} attempts: {last}")

    async def get_tx(self, sig: str, commitment: str = "confirmed") -> Optional[dict]:
        return await self.call("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1,
                                                        "commitment": commitment}])

    async def sigs(self, addr: str, limit: int = 1000, before: Optional[str] = None, until: Optional[str] = None,
                   commitment: str = "confirmed") -> list[dict]:
        p: dict = {"limit": limit, "commitment": commitment}
        if before:
            p["before"] = before
        if until:
            p["until"] = until
        return await self.call("getSignaturesForAddress", [addr, p])

    async def sigs_window(self, addr: str, since_ts: float, max_pages: int = 200, before: Optional[str] = None,
                          page: int = 1000) -> list[dict]:
        """Newest-first signatures back to `since_ts` (unix)."""
        out: list[dict] = []
        for _ in range(max_pages):
            batch = await self.sigs(addr, limit=page, before=before)
            if not batch:
                break
            out.extend(batch)
            before = batch[-1]["signature"]
            bt = batch[-1].get("blockTime")
            if bt is not None and bt < since_ts:
                break
            if len(batch) < page:
                break
        return [s for s in out if s.get("blockTime") is None or s["blockTime"] >= since_ts]

    async def account(self, addr: str, encoding: str = "base64") -> Optional[dict]:
        r = await self.call("getAccountInfo", [addr, {"encoding": encoding}])
        return r.get("value") if r else None

    async def multiple_accounts(self, addrs: list[str], encoding: str = "base64") -> list[Optional[dict]]:
        out = []
        for i in range(0, len(addrs), 100):
            r = await self.call("getMultipleAccounts", [addrs[i:i + 100], {"encoding": encoding}])
            out.extend(r["value"])
        return out

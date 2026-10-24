"""Backfill launches, creator funding and per-token trade tapes over RPC.

    python -m rugsnipe.research.backfill launches --hours 6 --out data/bf
    python -m rugsnipe.research.backfill funding  --out data/bf
    python -m rugsnipe.research.backfill tapes    --out data/bf --mints-file data/bf/candidates.txt --minutes 15

Resumable: every stage appends JSONL and skips work already on disk.
Endpoints come from --rpc (repeatable) or $RPC_URLS (comma separated).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from ..constants import PUMP_MINT_AUTHORITY
from ..decode import from_rpc_json, pump_events
from ..rpc import RPC, RpcError
from .launches import extract_launches, funding_record, outbound_transfers


def _load_jsonl(p: Path) -> list[dict]:
    if not p.exists():
        return []
    out = []
    with p.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


class Appender:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = path.open("a")

    def write(self, obj: dict):
        self.f.write(json.dumps(obj, separators=(",", ":")) + "\n")

    def flush(self):
        self.f.flush()

    def close(self):
        self.f.close()


def _endpoints(args) -> tuple[list[str], list[float]]:
    eps = args.rpc or [e for e in os.environ.get("RPC_URLS", "").split(",") if e]
    if not eps:
        eps = ["https://solana-rpc.publicnode.com", "https://api.mainnet-beta.solana.com"]
    rps = []
    for e in eps:
        if "publicnode" in e:
            rps.append(25.0)
        elif "api.mainnet-beta" in e:
            rps.append(3.5)
        else:
            rps.append(args.rps)
    return eps, rps


async def _gather_limited(coros, limit: int):
    sem = asyncio.Semaphore(limit)

    async def run(c):
        async with sem:
            return await c

    return await asyncio.gather(*[run(c) for c in coros], return_exceptions=True)


# ------------------------------------------------------------------------------ launches


async def cmd_launches(args):
    out = Path(args.out)
    sig_path = out / "create_sigs.jsonl"
    launch_path = out / "launches.jsonl"
    eps, rps = _endpoints(args)
    since = time.time() - args.hours * 3600
    async with RPC(eps, rps, concurrency=args.concurrency) as rpc:
        have_sigs = {s["signature"] for s in _load_jsonl(sig_path)}
        if not have_sigs or args.refresh:
            t0 = time.time()
            sigs = await rpc.sigs_window(PUMP_MINT_AUTHORITY, since_ts=since, max_pages=args.max_pages)
            app = Appender(sig_path)
            n_new = 0
            for s in sigs:
                if s["signature"] not in have_sigs:
                    app.write({"signature": s["signature"], "slot": s["slot"], "blockTime": s.get("blockTime"),
                               "err": s.get("err")})
                    n_new += 1
            app.close()
            print(f"[launches] {n_new} new create signatures ({len(sigs)} in window) in {time.time()-t0:.1f}s",
                  flush=True)
        sigs = [s for s in _load_jsonl(sig_path) if not s.get("err")]
        done = {l["sig"] for l in _load_jsonl(launch_path)} | {l["sig"] for l in _load_jsonl(out / "launch_skips.jsonl")}
        todo = [s for s in sigs if s["signature"] not in done and (s.get("blockTime") or since) >= since]
        print(f"[launches] {len(todo)} txs to fetch", flush=True)
        app = Appender(launch_path)
        skip = Appender(out / "launch_skips.jsonl")
        t0 = time.time()
        n = 0

        async def one(s):
            nonlocal n
            try:
                tx = await rpc.get_tx(s["signature"])
            except RpcError as e:
                return
            if not tx:
                return
            ntx = from_rpc_json(tx)
            recs = extract_launches(ntx)
            if not recs:
                skip.write({"sig": s["signature"], "reason": "no_create_event"})
            for r in recs:
                app.write(r)
            n += 1
            if n % 500 == 0:
                app.flush()
                print(f"[launches] {n}/{len(todo)} {n/(time.time()-t0):.1f} tx/s", flush=True)

        for i in range(0, len(todo), 2000):
            await _gather_limited([one(s) for s in todo[i:i + 2000]], args.concurrency)
            app.flush()
            skip.flush()
        app.close()
        skip.close()
        print(f"[launches] done {n} in {time.time()-t0:.1f}s, rpc calls={rpc.calls}", flush=True)


# ------------------------------------------------------------------------------ funding


async def cmd_funding(args):
    out = Path(args.out)
    launches = _load_jsonl(out / "launches.jsonl")
    fpath = out / "funding.jsonl"
    done = {r["creator"] for r in _load_jsonl(fpath)}
    first_create_slot: dict[str, int] = {}
    for l in launches:
        c = l.get("user") or l.get("creator")
        if not c:
            continue
        first_create_slot[c] = min(first_create_slot.get(c, 1 << 62), l["slot"])
    todo = [c for c in first_create_slot if c not in done]
    if args.limit:
        todo = todo[: args.limit]
    print(f"[funding] {len(todo)} creators to trace ({len(done)} done)", flush=True)
    eps, rps = _endpoints(args)
    app = Appender(fpath)
    t0 = time.time()
    n = 0
    async with RPC(eps, rps, concurrency=args.concurrency) as rpc:

        async def one(creator):
            nonlocal n
            try:
                sl = await rpc.sigs(creator, limit=1000)
                first = None
                if sl and len(sl) < 1000:
                    tx = await rpc.get_tx(sl[-1]["signature"])
                    first = from_rpc_json(tx) if tx else None
                rec = funding_record(creator, sl or [], first, first_create_slot[creator])
                # newest activity: who did the creator send its SOL to afterwards (chain link)
                rec["last_sig"] = sl[0]["signature"] if sl else None
                rec["last_slot"] = sl[0]["slot"] if sl else None
                app.write(rec)
            except RpcError as e:
                app.write({"creator": creator, "error": str(e)[:200]})
            n += 1
            if n % 500 == 0:
                app.flush()
                print(f"[funding] {n}/{len(todo)} {n/(time.time()-t0):.1f}/s calls={rpc.calls}", flush=True)

        for i in range(0, len(todo), 2000):
            await _gather_limited([one(c) for c in todo[i:i + 2000]], args.concurrency)
            app.flush()
    app.close()
    print(f"[funding] done {n} in {time.time()-t0:.1f}s", flush=True)


# ------------------------------------------------------------------------------ tapes


async def cmd_tapes(args):
    """Trade tape (all pump trades) for each mint's bonding curve, first N minutes after creation."""
    out = Path(args.out)
    tdir = out / "tapes"
    tdir.mkdir(parents=True, exist_ok=True)
    launches = {l["mint"]: l for l in _load_jsonl(out / "launches.jsonl")}
    mints = [m.strip() for m in Path(args.mints_file).read_text().split() if m.strip()]
    todo = [m for m in mints if m in launches and not (tdir / f"{m}.jsonl").exists()]
    print(f"[tapes] {len(todo)} mints to fetch", flush=True)
    eps, rps = _endpoints(args)
    t0 = time.time()
    async with RPC(eps, rps, concurrency=args.concurrency) as rpc:
        for k, m in enumerate(todo):
            l = launches[m]
            bc = l["bonding_curve"]
            horizon = (l.get("block_time") or 0) + args.minutes * 60
            # all signatures newest-first; keep those inside the horizon (reuse sigscan output if present)
            sigs, before = [], None
            cached = out / "sigs" / f"{m}.json"
            if cached.exists():
                j = json.loads(cached.read_text())
                if j.get("complete"):
                    sigs = [{"signature": r[0], "slot": r[1], "blockTime": r[2], "err": r[3] or None} for r in j["sigs"]]
            if not sigs:
                for _ in range(args.max_pages):
                    b = await rpc.sigs(bc, limit=1000, before=before)
                    if not b:
                        break
                    sigs.extend(b)
                    before = b[-1]["signature"]
                    if len(b) < 1000:
                        break
            sigs = [s for s in sigs if s.get("blockTime") is None or s["blockTime"] <= horizon]
            sigs = [s for s in sigs if not s.get("err")] if args.skip_failed else sigs
            sigs.sort(key=lambda s: s["slot"])
            if len(sigs) > args.max_txs:
                sigs = sigs[: args.max_txs]
            txs = await _gather_limited([rpc.get_tx(s["signature"]) for s in sigs], args.concurrency)
            rows = []
            for s, tx in zip(sigs, txs):
                if not tx or isinstance(tx, Exception):
                    continue
                ntx = from_rpc_json(tx)
                evs = [e for kk, e, _ in pump_events(ntx) if kk == "trade" and e.get("mint") == m]
                rows.append({
                    "sig": ntx.signature, "slot": ntx.slot, "idx": ntx.tx_index, "t": ntx.block_time,
                    "err": ntx.err is not None, "payer": ntx.fee_payer, "signers": ntx.signers,
                    "prio": ntx.priority_fee_lamports(), "cu": ntx.cu_consumed,
                    "tips": outbound_transfers(ntx, ntx.fee_payer)[:4],
                    "trades": [{"u": e["user"], "b": e["is_buy"], "sol": e["sol_amount"], "tok": e["token_amount"],
                                "vsr": e["vsr"], "vtr": e["vtr"], "fee": e.get("fee", 0), "cfee": e.get("creator_fee", 0),
                                "ix": e.get("ix_name")} for e in evs],
                    "tok_xfers": _token_transfers(ntx, m),
                })
            with (tdir / f"{m}.jsonl").open("w") as f:
                for r in rows:
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
            if (k + 1) % 10 == 0:
                print(f"[tapes] {k+1}/{len(todo)} mints, calls={rpc.calls}, {time.time()-t0:.0f}s", flush=True)
    print(f"[tapes] done in {time.time()-t0:.1f}s", flush=True)


async def cmd_tapes_gtfa(args):
    """Same output as `tapes`, via Triton's getTransactionsForAddress (full txs, ascending, 100/call)."""
    out = Path(args.out)
    tdir = out / "tapes"
    tdir.mkdir(parents=True, exist_ok=True)
    launches = {l["mint"]: l for l in _load_jsonl(out / "launches.jsonl")}
    mints = [m.strip() for m in Path(args.mints_file).read_text().split() if m.strip()]
    todo = [m for m in mints if m in launches and not (tdir / f"{m}.jsonl").exists()]
    print(f"[tapes-gtfa] {len(todo)} mints to fetch", flush=True)
    eps, rps = _endpoints(args)
    t0 = time.time()
    n_done = 0
    async with RPC(eps, rps, concurrency=args.concurrency) as rpc:

        async def one(m):
            nonlocal n_done
            l = launches[m]
            bc = l["bonding_curve"]
            horizon = (l.get("block_time") or 0) + int(args.minutes * 60)
            rows, token = [], None
            try:
                while len(rows) < args.max_txs:
                    opts = {"transactionDetails": "full", "sortOrder": "asc", "limit": 100, "encoding": "json",
                            "maxSupportedTransactionVersion": 1, "commitment": "confirmed",
                            "filters": {"blockTime": {"lte": horizon}, "status": "any"}}
                    if token:
                        opts["paginationToken"] = token
                    res = await rpc.call("getTransactionsForAddress", [bc, opts])
                    data = res.get("data") or []
                    for tx in data:
                        ntx = from_rpc_json(tx)
                        evs = [e for kk, e, _ in pump_events(ntx) if kk == "trade" and e.get("mint") == m]
                        rows.append(_tape_row(ntx, m, evs))
                    token = res.get("paginationToken")
                    if not data or not token or len(data) < 100:
                        break
            except RpcError as e:
                print(f"[tapes-gtfa] {m} error {str(e)[:120]}", flush=True)
                return
            with (tdir / f"{m}.jsonl").open("w") as f:
                for r in rows:
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
            n_done += 1
            if n_done % 50 == 0:
                print(f"[tapes-gtfa] {n_done}/{len(todo)} calls={rpc.calls} {time.time()-t0:.0f}s", flush=True)

        await _gather_limited([one(m) for m in todo], args.concurrency)
    print(f"[tapes-gtfa] done {n_done} in {time.time()-t0:.1f}s calls={rpc.calls}", flush=True)


def _tape_row(ntx, mint: str, evs: list[dict]) -> dict:
    return {
        "sig": ntx.signature, "slot": ntx.slot, "idx": ntx.tx_index, "t": ntx.block_time,
        "err": ntx.err is not None, "payer": ntx.fee_payer, "signers": ntx.signers,
        "prio": ntx.priority_fee_lamports(), "cu": ntx.cu_consumed,
        "tips": outbound_transfers(ntx, ntx.fee_payer)[:4],
        "trades": [{"u": e["user"], "b": e["is_buy"], "sol": e["sol_amount"], "tok": e["token_amount"],
                    "vsr": e["vsr"], "vtr": e["vtr"], "fee": e.get("fee", 0), "cfee": e.get("creator_fee", 0),
                    "ix": e.get("ix_name")} for e in evs],
        "tok_xfers": _token_transfers(ntx, mint),
    }


async def cmd_sigscan(args):
    """Per launch: signature list of the bonding curve (activity screen; reused by `tapes`)."""
    out = Path(args.out)
    sdir = out / "sigs"
    sdir.mkdir(parents=True, exist_ok=True)
    launches = _load_jsonl(out / "launches.jsonl")
    todo = [l for l in launches if (l.get("sol_quoted") or args.all_quotes) and not (sdir / f"{l['mint']}.json").exists()]
    print(f"[sigscan] {len(todo)} curves to scan", flush=True)
    eps, rps = _endpoints(args)
    t0 = time.time()
    n = 0
    async with RPC(eps, rps, concurrency=args.concurrency) as rpc:

        async def one(l):
            nonlocal n
            bc = l["bonding_curve"]
            sigs, before = [], None
            try:
                for _ in range(args.max_pages):
                    b = await rpc.sigs(bc, limit=1000, before=before)
                    if not b:
                        break
                    sigs.extend(b)
                    before = b[-1]["signature"]
                    if len(b) < 1000:
                        break
            except RpcError:
                return
            complete = len(sigs) < 1000 * args.max_pages or (sigs and sigs[-1]["slot"] <= l["slot"])
            rows = [[s["signature"], s["slot"], s.get("blockTime"), 1 if s.get("err") else 0] for s in sigs]
            (sdir / f"{l['mint']}.json").write_text(json.dumps({"mint": l["mint"], "complete": bool(complete),
                                                                "sigs": rows}, separators=(",", ":")))
            n += 1
            if n % 1000 == 0:
                print(f"[sigscan] {n}/{len(todo)} {n/(time.time()-t0):.1f}/s", flush=True)

        for i in range(0, len(todo), 2000):
            await _gather_limited([one(l) for l in todo[i:i + 2000]], args.concurrency)
    print(f"[sigscan] done {n} in {time.time()-t0:.1f}s", flush=True)


def _token_transfers(ntx, mint: str) -> list[tuple[str, int]]:
    """Owner-level token balance changes for `mint` in this tx (catches consolidation transfers)."""
    pre: dict[str, int] = {}
    post: dict[str, int] = {}
    for t in ntx.pre_token:
        if t.get("mint") == mint:
            pre[t.get("owner")] = pre.get(t.get("owner"), 0) + int(t["uiTokenAmount"]["amount"])
    for t in ntx.post_token:
        if t.get("mint") == mint:
            post[t.get("owner")] = post.get(t.get("owner"), 0) + int(t["uiTokenAmount"]["amount"])
    owners = set(pre) | set(post)
    return [(o, post.get(o, 0) - pre.get(o, 0)) for o in owners if post.get(o, 0) != pre.get(o, 0)]


def main(argv=None):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("launches", "funding", "tapes", "sigscan", "tapes-gtfa"):
        p = sub.add_parser(name)
        p.add_argument("--out", default="data/bf")
        p.add_argument("--rpc", action="append")
        p.add_argument("--rps", type=float, default=30.0)
        p.add_argument("--concurrency", type=int, default=24)
        if name == "launches":
            p.add_argument("--hours", type=float, default=6)
            p.add_argument("--max-pages", type=int, default=400)
            p.add_argument("--refresh", action="store_true")
        if name == "funding":
            p.add_argument("--limit", type=int, default=0)
        if name == "sigscan":
            p.add_argument("--max-pages", type=int, default=2)
            p.add_argument("--all-quotes", action="store_true")
        if name in ("tapes", "tapes-gtfa"):
            p.add_argument("--mints-file", required=True)
            p.add_argument("--minutes", type=float, default=20)
            p.add_argument("--max-pages", type=int, default=20)
            p.add_argument("--max-txs", type=int, default=3000)
            p.add_argument("--skip-failed", action="store_true")
    args = ap.parse_args(argv)
    fn = {"launches": cmd_launches, "funding": cmd_funding, "tapes": cmd_tapes, "sigscan": cmd_sigscan, "tapes-gtfa": cmd_tapes_gtfa}[args.cmd]
    asyncio.run(fn(args))


if __name__ == "__main__":
    main()

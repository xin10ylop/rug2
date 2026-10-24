"""Automated operator discovery + out-of-sample validation (what the video does by hand).

    python -m rugsnipe.research.pipeline --hours 8 --out data/discovery/run1 --rpc $TRITON_RPC_URL

1. all launches in the window (getTransactionsForAddress on the mint authority: 100 creates/call)
2. creator funding: the funding right before each creator's first launch in the window, attributed
   by balance deltas (routing through temp accounts still resolves to the paying wallet). Fresh means
   the wallet held 0 SOL when funded, as in the live engine: operators recycle aged wallets drained
   to 0, so a wallet's age or history says nothing.
3. candidate class: fresh at funding, launched within 2 txs of it, SOL quote, not mayhem, default
   creator fee, dev buy >= --min-dev
4. trade tapes for candidates, fill-realistic simulation (entry end of slot+2, all costs)
5. funders whose candidates are positive in-sample -> 72h history (out-of-sample) via operator_history
6. candidates.json: ranked funders with in-sample + out-of-sample stats and a suggested operator
   profile (funding range, dev-buy range, first-launch rule) ready for config/operators.json.

Nothing here trades. Promotion to live is a human decision (see RUNBOOK.md).
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import math
import os
import statistics
import time
from pathlib import Path

from ..constants import PUMP_MINT_AUTHORITY
from ..decode import from_rpc_json, pump_events
from ..rpc import RPC, RpcError
from ..sim import SimParams, build_tape, simulate
from .backfill import _gather_limited, _tape_row
from .launches import extract_launches, funding_source
from .operator_history import evaluate_dir, gtfa

KNOWN_PLATFORMS = {"AxiomRXZAq1Jgjj9pHmNqVP7Lhu67wLXZJZbaK87TTSk"}


def _stats(v):
    if not v:
        return {"n": 0}
    m = statistics.fmean(v)
    sd = statistics.pstdev(v) if len(v) > 1 else 0
    return {"n": len(v), "mean": round(m, 4), "median": round(statistics.median(v), 4),
            "win": round(sum(1 for x in v if x > 0) / len(v), 3),
            "t": round(m / (sd / math.sqrt(len(v))), 2) if sd > 0 and len(v) > 1 else None}


async def run(a):
    out = Path(a.out)
    (out / "tapes").mkdir(parents=True, exist_ok=True)
    eps = a.rpc or [os.environ["TRITON_RPC_URL"]]
    now = int(time.time())
    since = now - int(a.hours * 3600)
    async with RPC(eps, a.rps, concurrency=a.concurrency) as rpc:
        # 1 launches
        lp = out / "launches.jsonl"
        if not lp.exists():
            txs = await gtfa(rpc, PUMP_MINT_AUTHORITY, since, now, full=True, max_pages=int(a.hours * 40) + 20)
            launches = [l for tx in txs for l in extract_launches(from_rpc_json(tx))]
            lp.write_text("\n".join(json.dumps(l) for l in launches))
        launches = [json.loads(x) for x in lp.read_text().splitlines() if x.strip()]
        # a cached run keeps its own window: out-of-sample must end where these launches begin
        since = min((l["block_time"] for l in launches if l.get("block_time")), default=since)
        print(f"[1] {len(launches)} launches in {a.hours}h", flush=True)
        first_slot, first_time = {}, {}
        for l in launches:
            if l["slot"] < first_slot.get(l["user"], 1 << 62):
                first_slot[l["user"]], first_time[l["user"]] = l["slot"], l["block_time"]
        pre = [l for l in launches if l.get("sol_quoted") and not l.get("mayhem") and (l.get("creator_fee_bps") or 0) <= 30
               and a.min_dev * 1e9 <= sum(d["sol"] for d in l["dev_buys"]) < 80e9]
        print(f"[3] {len(pre)} launches in the candidate class (dev buy >= {a.min_dev} SOL)", flush=True)
        # 2 funding of their creators
        fp = out / "funding.json"
        fund = json.loads(fp.read_text()) if fp.exists() else {}

        async def trace(creator):
            if creator in fund:
                return
            t0 = first_time[creator]
            try:
                res = await rpc.call("getTransactionsForAddress", [creator, {
                    "transactionDetails": "full", "sortOrder": "desc", "limit": 8, "encoding": "json",
                    "maxSupportedTransactionVersion": 1, "commitment": "confirmed",
                    "filters": {"blockTime": {"gte": t0 - 48 * 3600, "lte": t0}, "status": "succeeded"}}])
            except RpcError:
                return
            rec = {"between": 0}
            for tx in res.get("data") or []:  # newest first, walking back from the first launch
                ntx = from_rpc_json(tx)
                if ntx.slot > first_slot[creator] or any(k == "create" for k, _, _ in pump_events(ntx)):
                    continue
                src = funding_source(ntx, creator)
                if src and src[1] >= 100_000_000:
                    i = ntx.keys.index(creator)
                    rec.update(funder=src[0], lamports=src[1], t=ntx.block_time, slot=ntx.slot,
                               fresh=ntx.pre_balances[i] == 0)
                    break
                rec["between"] += 1
            fund[creator] = rec

        await _gather_limited([trace(c) for c in {l["user"] for l in pre}], a.concurrency)
        fp.write_text(json.dumps(fund))
        cands = [l for l in pre if fund.get(l["user"], {}).get("fresh") and fund[l["user"]]["between"] <= 2
                 and fund[l["user"]]["funder"] not in KNOWN_PLATFORMS]
        print(f"[2] {len(cands)} candidate launches by fresh creators", flush=True)

        # 4 tapes
        async def tape(l):
            p = out / "tapes" / f"{l['mint']}.jsonl"
            if p.exists():
                return
            try:
                hist = await gtfa(rpc, l["bonding_curve"], l["block_time"] - 2, l["block_time"] + 600, full=True,
                                  max_pages=7)
            except RpcError:
                return
            rows = []
            for tx in hist:
                ntx = from_rpc_json(tx)
                evs = [e for k, e, _ in pump_events(ntx) if k == "trade" and e.get("mint") == l["mint"]]
                rows.append(_tape_row(ntx, l["mint"], evs))
            p.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in rows))

        await _gather_limited([tape(l) for l in cands], a.concurrency)
        # 5 in-sample per funder
        groups = collections.defaultdict(list)
        for l in cands:
            p = out / "tapes" / f"{l['mint']}.jsonl"
            if not p.exists():
                continue
            t = build_tape(l, [json.loads(x) for x in p.read_text().splitlines() if x.strip()])
            if not t:
                continue
            r = simulate(t, SimParams(position="slot+2_end", tp_pct=30, max_hold_s=90, size_sol=0.25))
            funder = fund.get(l["user"], {}).get("funder")
            if r.entered and funder:
                groups[funder].append((l, r.ret, l["slot"] == first_slot[l["user"]]))
        ins = []
        for f, items in groups.items():
            if len(items) < a.min_launches:
                continue
            st = _stats([x[1] for x in items])
            ins.append((f, st, items))
        ins.sort(key=lambda x: -(x[1].get("mean") or -9))
        print(f"[5] {len(ins)} funders with >= {a.min_launches} candidate launches; in-sample:", flush=True)
        for f, st, items in ins[:15]:
            print(f"     {f} {st} first={_stats([x[1] for x in items if x[2]])}", flush=True)
    # 6 out-of-sample for the in-sample positive ones
    results = []
    from .operator_history import run as op_run
    for f, st, items in ins:
        if (st.get("mean") or -1) < a.min_insample_mean or len(results) >= a.max_oos:
            continue
        od = out / "oos" / f[:8]
        ns = argparse.Namespace(funder=f, hours=a.oos_hours, out=str(od), rpc=a.rpc, rps=a.rps,
                                concurrency=a.concurrency, min_sol=0.5, minutes=10, max_creator_pages=3,
                                max_tape_pages=7)
        try:
            await op_run(ns)
        except Exception as e:
            print(f"     oos {f[:8]} failed: {e}", flush=True)
            continue
        rep = json.loads((od / "report.json").read_text())
        L = sorted([json.loads(x) for x in (od / "launches.jsonl").read_text().splitlines() if x.strip()],
                   key=lambda l: l["slot"])
        first_mints = {l["mint"] for l in L if l.get("first_after_funding")}
        k = "slot+2_end|30|60"
        # strictly out-of-sample: only launches before the discovery window
        oos = [d for d in rep["launches"] if k in d["sims"] and (d.get("t") or now) < since]
        all_r = [d["sims"][k] for d in oos]
        first_r = [d["sims"][k] for d in oos if d["mint"] in first_mints]
        devs = sorted(sum(d["sol"] for d in l["dev_buys"]) / 1e9 for l in L if l.get("dev_buys"))
        fl = sorted(l.get("fund_lamports", 0) / 1e9 for l in L)
        prof = {
            "id": f[:6].lower(), "label": f"auto-discovered funder {f[:8]}", "funders": [f],
            "fund_min_sol": round(max(0.5, fl[int(0.05 * len(fl))] * 0.8), 3) if fl else 1.0,
            "fund_max_sol": round(fl[int(0.95 * len(fl)) - 1] * 1.2, 3) if fl else 50.0,
            "dev_buy_min_sol": round(devs[int(0.05 * len(devs))] * 0.9, 3) if devs else 0.0,
            "dev_buy_max_sol": round(devs[int(0.95 * len(devs)) - 1] * 1.1, 3) if devs else 80.0,
            "policy": {"enabled": False, "size_sol": 0.1, "tp_pct": 30, "max_hold_s": 90, "first_launch_only": True},
        }
        results.append({"funder": f, "in_sample": st, "oos_all": _stats(all_r), "oos_first": _stats(first_r),
                        "oos_launches": len(L), "suggested_operator": prof})
        print(f"     OOS {f[:8]}: all={_stats(all_r)} first={_stats(first_r)}", flush=True)
    results.sort(key=lambda r: -(r["oos_first"].get("mean") or -9))
    (out / "candidates.json").write_text(json.dumps(results, indent=1))
    print(f"[6] wrote {out / 'candidates.json'} ({len(results)} validated funders)")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=8)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rpc", action="append")
    ap.add_argument("--rps", type=float, default=30)
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--min-dev", type=float, default=3.0)
    ap.add_argument("--min-launches", type=int, default=3)
    ap.add_argument("--min-insample-mean", type=float, default=0.0)
    ap.add_argument("--oos-hours", type=float, default=72)
    ap.add_argument("--max-oos", type=int, default=8)
    asyncio.run(run(ap.parse_args(argv)))


if __name__ == "__main__":
    main()

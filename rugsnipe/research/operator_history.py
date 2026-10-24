"""Deep-dive one operator: follow a funder (mother) wallet over a time window.

    python -m rugsnipe.research.operator_history --funder <addr> --hours 48 --out data/ops/<name>

1. outgoing SOL transfers of the funder in the window (recipient, amount, time)
2. for every recipient: did it create pump.fun tokens? (creator's own history)
3. launches -> trade tapes -> fill-realistic evaluation (same simulator as discovery)
Writes launches.jsonl / tapes / report.json under --out. Use it for out-of-sample
checks: discover an operator in one window, validate it on another.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from pathlib import Path

from ..decode import from_rpc_json, pump_events
from ..rpc import RPC, RpcError
from ..sim import SimParams, build_tape, describe, simulate
from .backfill import _gather_limited, _tape_row
from .launches import balance_recipients, extract_launches


async def gtfa(rpc: RPC, addr: str, since: int, until: int | None = None, full: bool = True, max_pages: int = 50,
               asc: bool = True) -> list[dict]:
    rows, token = [], None
    flt: dict = {"blockTime": {"gte": since}, "status": "succeeded"}
    if until:
        flt["blockTime"]["lte"] = until
    for _ in range(max_pages):
        opts = {"transactionDetails": "full" if full else "signatures", "sortOrder": "asc" if asc else "desc",
                "limit": 100 if full else 1000, "encoding": "json", "maxSupportedTransactionVersion": 1,
                "commitment": "confirmed", "filters": flt}
        if token:
            opts["paginationToken"] = token
        res = await rpc.call("getTransactionsForAddress", [addr, opts])
        data = res.get("data") or []
        rows.extend(data)
        token = res.get("paginationToken")
        if not data or not token or len(data) < opts["limit"]:
            break
    return rows


async def run(args):
    out = Path(args.out)
    (out / "tapes").mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    since = now - int(args.hours * 3600)
    eps = args.rpc or [os.environ["TRITON_RPC_URL"]]
    async with RPC(eps, args.rps, concurrency=args.concurrency) as rpc:
        txs = await gtfa(rpc, args.funder, since)
        recips: dict[str, dict] = {}
        fundings: dict[str, list] = {}
        for tx in txs:
            ntx = from_rpc_json(tx)
            for to, lam in balance_recipients(ntx, args.funder, int(args.min_sol * 1e9)):
                r = recips.setdefault(to, {"to": to, "lamports": lam, "t": ntx.block_time, "slot": ntx.slot, "n": 0})
                r["n"] += 1
                i = ntx.keys.index(to)
                fundings.setdefault(to, []).append([ntx.slot, ntx.block_time, lam, ntx.pre_balances[i]])
        (out / "fundings.json").write_text(json.dumps(fundings))
        print(f"[op] {len(txs)} funder txs, {len(recips)} recipients >= {args.min_sol} SOL", flush=True)

        launches: list[dict] = []

        async def scan_recipient(r):
            try:
                hist = await gtfa(rpc, r["to"], r["t"] - 5, now, full=True, max_pages=args.max_creator_pages)
            except RpcError:
                return
            r["n_txs"] = len(hist)
            for tx in hist:
                ntx = from_rpc_json(tx)
                for l in extract_launches(ntx):
                    if l.get("user") == r["to"]:
                        l["funder"] = args.funder
                        l["fund_lamports"] = r["lamports"]
                        launches.append(l)

        await _gather_limited([scan_recipient(r) for r in recips.values()], args.concurrency)
        launches.sort(key=lambda l: l["slot"])
        firsts = first_after_funding(launches, fundings)
        for l in launches:
            l["first_after_funding"] = l["mint"] in firsts
        with (out / "launches.jsonl").open("w") as f:
            for l in launches:
                f.write(json.dumps(l) + "\n")
        print(f"[op] {len(launches)} launches by funded creators", flush=True)

        async def tape(l):
            p = out / "tapes" / f"{l['mint']}.jsonl"
            if p.exists():
                return
            try:
                hist = await gtfa(rpc, l["bonding_curve"], l["block_time"] - 2, l["block_time"] + int(args.minutes * 60),
                                  full=True, max_pages=args.max_tape_pages)
            except RpcError:
                return
            rows = []
            for tx in hist:
                ntx = from_rpc_json(tx)
                evs = [e for k, e, _ in pump_events(ntx) if k == "trade" and e.get("mint") == l["mint"]]
                rows.append(_tape_row(ntx, l["mint"], evs))
            with p.open("w") as f:
                for row in rows:
                    f.write(json.dumps(row, separators=(",", ":")) + "\n")

        await _gather_limited([tape(l) for l in launches if l.get("sol_quoted") and not l.get("mayhem")],
                              args.concurrency)
    report = evaluate_dir(out, launches)
    (out / "report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report["summary"], indent=1))


def first_after_funding(launches: list[dict], fundings: dict[str, list]) -> set[str]:
    """Mints that are their creator's first launch after its latest funding.

    This is the live engine's "first launch": a recycled wallet (drained to 0, funded again later)
    starts a new count at each funding; top-ups (balance not 0 when funded) do not.
    fundings: creator -> [[slot, block_time, lamports, pre_balance], ...]."""
    last: dict[str, int] = {}
    out = set()
    for l in sorted(launches, key=lambda l: (l["slot"], l.get("tx_index") or 0)):
        u = l["user"]
        fs = [f[0] for f in fundings.get(u, []) if f[0] <= l["slot"] and f[3] == 0]
        if fs and (u not in last or last[u] < max(fs)):
            out.add(l["mint"])
        last[u] = l["slot"]
    return out


def evaluate_dir(out: Path, launches: list[dict], configs=None) -> dict:
    configs = configs or [("slot+1_end", 30, 60), ("slot+1_end", 30, 180), ("slot+2_end", 30, 60),
                          ("slot+1_end", 50, 180), ("slot+1_end", 100, 180), ("slot+3_end", 30, 60)]
    per = []
    for l in launches:
        p = out / "tapes" / f"{l['mint']}.jsonl"
        if not p.exists():
            continue
        t = build_tape(l, [json.loads(x) for x in p.open()])
        if not t:
            continue
        d = describe(t)
        d["t"] = l["block_time"]
        d["creator"] = l["user"]
        d["first"] = l.get("first_after_funding")
        d["sims"] = {}
        for pos, tp, hold in configs:
            r = simulate(t, SimParams(position=pos, tp_pct=tp, max_hold_s=hold, size_sol=0.25))
            if r.entered:
                d["sims"][f"{pos}|{tp}|{hold}"] = round(r.ret, 4)
        per.append(d)
    summary = {"n_launches": len(launches), "n_eval": len(per)}
    for pos, tp, hold in configs:
        k = f"{pos}|{tp}|{hold}"
        v = [d["sims"][k] for d in per if k in d["sims"]]
        if v:
            summary[k] = {"n": len(v), "mean": round(statistics.fmean(v), 4), "median": round(statistics.median(v), 4),
                          "win": round(sum(1 for x in v if x > 0) / len(v), 3)}
        v = [d["sims"][k] for d in per if k in d["sims"] and d.get("first")]
        if v:
            summary[k + "|first"] = {"n": len(v), "mean": round(statistics.fmean(v), 4),
                                     "median": round(statistics.median(v), 4),
                                     "win": round(sum(1 for x in v if x > 0) / len(v), 3)}
    return {"summary": summary, "launches": per}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--funder", required=True)
    ap.add_argument("--hours", type=float, default=48)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rpc", action="append")
    ap.add_argument("--rps", type=float, default=30)
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--min-sol", type=float, default=0.5)
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--max-creator-pages", type=int, default=3)
    ap.add_argument("--max-tape-pages", type=int, default=7)
    asyncio.run(run(ap.parse_args(argv)))


if __name__ == "__main__":
    main()

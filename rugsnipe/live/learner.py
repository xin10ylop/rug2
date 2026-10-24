"""Rolling re-validation + live-vs-model reconciliation.

For every launch of a watched operator that passed the operator's signature
filters ("eligible": traded, paper, shadow or risk-blocked) the learner:
  1. fetches the complete trade tape after the fact (getTransactionsForAddress),
  2. simulates our policy at realistic entry positions (end of slot+1 / +3),
  3. compares with what actually happened to our own orders on the same launches.
Outputs data/learner.json and (for policy.auto operators) enables/disables trading.

The key check against the old failure mode ("the dry run scores races we never
win"): simulated EV on launches we *filled* vs launches we *missed* (expired /
guard-failed). If fills are systematically the worse launches, the edge is not
real for us, whatever the simulator says on all launches.
"""
from __future__ import annotations

import json
import logging
import math
import statistics
import time
from pathlib import Path
from typing import Optional

from ..decode import from_rpc_json, pump_events
from ..research.backfill import _tape_row
from ..rpc import RPC, RpcError
from ..sim import SimParams, build_tape, simulate
from .operators import OperatorBook
from .store import Store

log = logging.getLogger("learner")
LAMPORTS = 1_000_000_000


async def fetch_tape(rpc: RPC, mint: str, bonding_curve: str, block_time: int, minutes: float) -> list[dict]:
    rows, token = [], None
    for _ in range(8):
        opts = {"transactionDetails": "full", "sortOrder": "asc", "limit": 100, "encoding": "json",
                "maxSupportedTransactionVersion": 1, "commitment": "confirmed",
                "filters": {"blockTime": {"gte": block_time - 2, "lte": block_time + int(minutes * 60)}, "status": "any"}}
        if token:
            opts["paginationToken"] = token
        res = await rpc.call("getTransactionsForAddress", [bonding_curve, opts])
        data = res.get("data") or []
        for tx in data:
            ntx = from_rpc_json(tx)
            evs = [e for k, e, _ in pump_events(ntx) if k == "trade" and e.get("mint") == mint]
            rows.append(_tape_row(ntx, mint, evs))
        token = res.get("paginationToken")
        if not data or not token or len(data) < 100:
            break
    return rows


def _stats(v: list[float]) -> dict:
    if not v:
        return {"n": 0}
    m = statistics.fmean(v)
    sd = statistics.pstdev(v) if len(v) > 1 else 0.0
    se = sd / math.sqrt(len(v)) if len(v) > 1 else float("inf")
    return {"n": len(v), "mean": round(m, 4), "median": round(statistics.median(v), 4),
            "win": round(sum(1 for x in v if x > 0) / len(v), 3), "lcb": round(m - se, 4) if se != float("inf") else None}


async def run_learner(store: Store, book: OperatorBook, rpc: RPC, tape_dir: str = "data/tapes_live",
                      hours: float = 24.0, tape_minutes: float = 10.0, apply: bool = True,
                      out_path: str = "data/learner.json") -> dict:
    from ..txbuild import PK, bonding_curve_pda
    since = time.time() - hours * 3600
    tdir = Path(tape_dir)
    tdir.mkdir(parents=True, exist_ok=True)
    rows = store.q("SELECT mint, operator, slot, tx_index, creator, block_time, decision, reason, sig, "
                   "creator_fee_bps, t_deshred, t_processed FROM launches WHERE operator IS NOT NULL AND "
                   "decision IN ('buy','paper','shadow','blocked') AND COALESCE(t_deshred, t_processed) > ?", (since,))
    report = {"t": time.time(), "operators": {}}
    by_op: dict[str, list] = {}
    for (mint, op, slot, txi, creator, bt, decision, reason, sig, cfee, td, tp) in rows:
        if not bt or time.time() - (td or tp or 0) < tape_minutes * 60 + 30:
            continue  # tape not complete yet
        p = tdir / f"{mint}.jsonl"
        if not p.exists():
            try:
                tape_rows = await fetch_tape(rpc, mint, str(bonding_curve_pda(PK(mint))), bt, tape_minutes)
            except RpcError as e:
                log.warning("tape fetch %s failed: %s", mint[:8], str(e)[:100])
                continue
            p.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in tape_rows))
        tape_rows = [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
        launch = {"mint": mint, "slot": slot, "tx_index": txi, "sig": sig, "user": creator,
                  "creator_fee_bps": cfee or 0, "block_time": bt}
        tape = build_tape(launch, tape_rows)
        if not tape:
            continue
        by_op.setdefault(op, []).append((mint, decision, reason, tape))
    for op_id, items in by_op.items():
        op = book.ops.get(op_id)
        if not op:
            continue
        pol = op.policy
        sims = {"slot+1_end": [], "slot+3_end": []}
        filled_sim, missed_sim, realized = [], [], []
        for mint, decision, reason, tape in items:
            for pos in sims:
                r = simulate(tape, SimParams(size_sol=pol.size_sol, position=pos, tp_pct=pol.tp_pct,
                                             max_hold_s=pol.max_hold_s, sl_pct=pol.sl_pct))
                if r.entered:
                    sims[pos].append(r.ret)
            if decision == "buy":
                att = store.q("SELECT status, slot_offset FROM attempts WHERE mint=? AND side='buy' ORDER BY id DESC "
                              "LIMIT 1", (mint,))
                status, off = (att[0] if att else (None, None))
                s3 = simulate(tape, SimParams(size_sol=pol.size_sol, position="slot+3_end", tp_pct=pol.tp_pct,
                                              max_hold_s=pol.max_hold_s, sl_pct=pol.sl_pct))
                if status == "filled":
                    filled_sim.append(s3.ret)
                    prow = store.q("SELECT pnl, cost, status FROM positions WHERE mint=?", (mint,))
                    if prow and prow[0][2] == "closed" and prow[0][1]:
                        realized.append(prow[0][0] / prow[0][1])
                elif status in ("failed", "expired", "sent"):
                    missed_sim.append(s3.ret)
        st = {"eligible": len(items), "sim_slot1": _stats(sims["slot+1_end"]), "sim_slot3": _stats(sims["slot+3_end"]),
              "sim_on_filled": _stats(filled_sim), "sim_on_missed": _stats(missed_sim), "realized": _stats(realized)}
        # adverse selection flag: missed launches clearly better than filled ones
        if len(filled_sim) >= 5 and len(missed_sim) >= 5:
            st["adverse_selection_gap"] = round(statistics.fmean(missed_sim) - statistics.fmean(filled_sim), 4)
        action = None
        if pol.auto and apply:
            s1 = st["sim_slot3"]
            if s1["n"] >= 10 and s1["mean"] < pol.min_live_ev and pol.enabled:
                pol.enabled, action = False, f"disable: sim mean {s1['mean']} < {pol.min_live_ev}"
            elif st["realized"]["n"] >= 8 and st["realized"]["mean"] < -0.05 and pol.enabled:
                pol.enabled, action = False, f"disable: realized mean {st['realized']['mean']}"
            elif st.get("adverse_selection_gap", 0) > 0.15 and pol.enabled:
                pol.enabled, action = False, f"disable: adverse selection gap {st['adverse_selection_gap']}"
        st["action"] = action
        op.stats = {"learner": st, "t": time.time()}
        report["operators"][op_id] = st
        if action:
            log.warning("operator %s: %s", op_id, action)
    if apply:
        book.save()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(report, indent=1))
    return report

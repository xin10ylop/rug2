"""Per-operator, fill-realistic evaluation of launch tapes.

    python -m rugsnipe.research.evaluate --data data/bf [--groups operators.json] [--out eval.json]

For every launch with a tape:
  * descriptors (bundle size, outsiders in the first blocks, peak, rug drop/timing)
  * simulated trade for a grid of (entry position x TP x max hold), all costs in
  * PnL of the real early outsiders (what snipers actually made)
Aggregated per operator group and for the random baseline, with the
adverse-selection split: launches where outsiders sniped early vs. not.
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import random
import statistics
from pathlib import Path

from ..sim import SimParams, build_tape, describe, simulate, wallet_pnl

POSITIONS = ["after_bundle", "slot0_end", "slot+1_end", "slot+2_end", "slot+3_end"]
TPS = [30, 50, 100, 200]
HOLDS = [30, 90, 240]


def load_jsonl(p: Path):
    return [json.loads(l) for l in p.open() if l.strip()] if p.exists() else []


def eval_launch(launch: dict, rows: list[dict], size_sol: float, entry_fee: int, exit_fee: int,
                exit_delay: int) -> dict | None:
    tape = build_tape(launch, rows)
    if tape is None or not tape.trades:
        return None
    d = describe(tape)
    sims = {}
    for pos in POSITIONS:
        for tp in TPS:
            for hold in HOLDS:
                r = simulate(tape, SimParams(size_sol=size_sol, position=pos, tp_pct=tp, max_hold_s=hold,
                                             exit_delay_slots=exit_delay, entry_fee_lamports=entry_fee,
                                             exit_fee_lamports=exit_fee))
                if r.entered:
                    sims[f"{pos}|{tp}|{hold}"] = (round(r.ret, 4), r.exit_reason[:1], round(r.entry_mcap, 2))
    # real outsiders that bought in the first 3 blocks (not part of the creator bundle)
    wp = wallet_pnl(tape)
    early = []
    for w, st in wp.items():
        if w in tape.bundle_wallets or st["spent"] <= 0:
            continue
        if st["first_slot"] <= tape.create_slot + 2:
            early.append({"w": w, "slot_off": st["first_slot"] - tape.create_slot, "spent": st["spent"],
                          "pnl": st["pnl"]})
    d["early_snipers"] = early
    d["sims"] = sims
    d["creator"] = launch["user"]
    d["slot"] = launch["slot"]
    d["t"] = launch.get("block_time")
    return d


def summarize(rows: list[dict], key: str) -> dict:
    vals = [r["sims"][key][0] for r in rows if key in r["sims"]]
    if not vals:
        return {}
    wins = sum(1 for v in vals if v > 0)
    mean = statistics.fmean(vals)
    sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    return {"n": len(vals), "mean": round(mean, 4), "median": round(statistics.median(vals), 4),
            "win": round(wins / len(vals), 3), "t": round(mean / (sd / math.sqrt(len(vals))), 2) if sd > 0 else None,
            "sum": round(sum(vals), 3)}


def bootstrap_mean_ci(vals: list[float], n: int = 2000, seed: int = 1) -> tuple[float, float]:
    if len(vals) < 3:
        return (float("nan"), float("nan"))
    rnd = random.Random(seed)
    means = []
    for _ in range(n):
        s = [vals[rnd.randrange(len(vals))] for _ in vals]
        means.append(sum(s) / len(s))
    means.sort()
    return (means[int(0.05 * n)], means[int(0.95 * n)])


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/bf")
    ap.add_argument("--groups", default=None)
    ap.add_argument("--size", type=float, default=0.25)
    ap.add_argument("--entry-fee", type=int, default=2_000_000)
    ap.add_argument("--exit-fee", type=int, default=1_000_000)
    ap.add_argument("--exit-delay", type=int, default=1)
    ap.add_argument("--baseline", default=None, help="file with baseline mints")
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    d = Path(a.data)
    launches = {l["mint"]: l for l in load_jsonl(d / "launches.jsonl")}
    groups = json.loads(Path(a.groups or d / "operators.json").read_text())
    per_mint = {}
    for p in sorted((d / "tapes").glob("*.jsonl")):
        m = p.stem
        if m not in launches:
            continue
        rows = load_jsonl(p)
        r = eval_launch(launches[m], rows, a.size, a.entry_fee, a.exit_fee, a.exit_delay)
        if r:
            per_mint[m] = r
    print(f"evaluated {len(per_mint)} launches")
    report = {"params": vars(a), "groups": {}}
    sets = {}
    for k, g in groups.items():
        ms = [m for m in g["mints"] if m in per_mint]
        if len(ms) >= 3:
            sets[k] = ms
    if a.baseline:
        sets["BASELINE"] = [m for m in Path(a.baseline).read_text().split() if m in per_mint]
    for k, ms in sets.items():
        rows = [per_mint[m] for m in ms]
        g = {"n": len(rows)}
        g["bundle_sol_med"] = round(statistics.median(r["bundle_sol"] for r in rows), 3)
        g["mcap_after_bundle_med"] = round(statistics.median(r["mcap_after_bundle"] for r in rows), 1)
        g["slot0_outsider_frac"] = round(sum(1 for r in rows if r["slot0_outsider_buys"] > 0) / len(rows), 3)
        g["early_outsiders_med"] = statistics.median(r["early_outsiders"] for r in rows)
        g["peak_x_vs_slot1_med"] = round(statistics.median(r["peak_x_vs_slot1"] for r in rows), 3)
        g["peak_x_ge2_frac"] = round(sum(1 for r in rows if r["peak_x_vs_slot1"] >= 2.0) / len(rows), 3)
        g["rug_after_s_med"] = statistics.median([r["rug_after_s"] for r in rows if r["rug_after_s"] is not None] or [0])
        snip = [s for r in rows for s in r["early_snipers"]]
        if snip:
            g["real_snipers"] = {"n": len(snip), "win": round(sum(1 for s in snip if s["pnl"] > 0) / len(snip), 3),
                                 "pnl_sol": round(sum(s["pnl"] for s in snip) / 1e9, 3),
                                 "spent_sol": round(sum(s["spent"] for s in snip) / 1e9, 3)}
        best = None
        g["grid"] = {}
        for pos in POSITIONS:
            for tp in TPS:
                for hold in HOLDS:
                    key = f"{pos}|{tp}|{hold}"
                    s = summarize(rows, key)
                    if s:
                        g["grid"][key] = s
                        if pos in ("slot+1_end", "slot+2_end") and (best is None or s["mean"] > best[1]["mean"]):
                            best = (key, s)
        if best:
            vals = [r["sims"][best[0]][0] for r in rows if best[0] in r["sims"]]
            g["best_realistic"] = {"key": best[0], **best[1], "ci90": [round(x, 4) for x in bootstrap_mean_ci(vals)]}
        # adverse selection: compare launches with / without early outsiders (realistic config)
        key = "slot+1_end|100|90"
        with_o = [r["sims"][key][0] for r in rows if key in r["sims"] and r["early_outsiders"] > 0]
        without = [r["sims"][key][0] for r in rows if key in r["sims"] and r["early_outsiders"] == 0]
        g["adverse"] = {"with_outsiders": (len(with_o), round(statistics.fmean(with_o), 4) if with_o else None),
                        "without": (len(without), round(statistics.fmean(without), 4) if without else None)}
        report["groups"][k] = g
    outp = Path(a.out) if a.out else d / "eval.json"
    outp.write_text(json.dumps(report, indent=1))
    (d / "eval_per_mint.json").write_text(json.dumps(per_mint))
    print(f"wrote {outp}")


if __name__ == "__main__":
    main()

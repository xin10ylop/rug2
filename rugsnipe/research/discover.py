"""Operator discovery: group launches into operators by how their creator wallets are funded.

Keys (one launch can match several; the most specific wins):
  creator:<addr>                    creator address reused for several launches
  fund:<funder>:<lamports>          hub/exchange funder + exact amount (the video's "Binance 2 + 2.955")
  mother:<funder>                   private funder that only funds this operator's creators

A funder is treated as a *hub* (exchange-like, needs the amount to disambiguate)
when it funds many creators with many different amounts, or is a known exchange.
"""
from __future__ import annotations

import collections
import json
import statistics
from pathlib import Path
from typing import Iterable

# Known CEX hot wallets (labels are cosmetic; hub-ness is also inferred from data).
KNOWN_HUBS = {
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance 2",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance 1",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Binance 3",
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6": "KuCoin",
    "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ": "MEXC",
    "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w": "Gate",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Bybit",
    "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD": "OKX",
    "9un5wqE3q4oCjyrDkwsdD48KteCJitQX5978Vh7KKxHo": "OKX 2",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase 1",
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm": "Coinbase 2",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Coinbase hot",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    "A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR": "Bitget",
    "88xTWZMeKfiTgbfEmPLdsUCQcZinwUfk25EBQZ21XMAZ": "HTX",
    "AobVSwdW9BbpMdJvTqeCN4hPAmh4rHm7vwLnQ5ATSyrS": "Crypto.com",
    "AxiomRXZAq1Jgjj9pHmNqVP7Lhu67wLXZJZbaK87TTSk": "Axiom (platform)",
}


def load_jsonl(p: Path) -> list[dict]:
    if not p.exists():
        return []
    return [json.loads(l) for l in p.open() if l.strip()]


def classify_funders(fund: dict[str, dict], min_creators: int = 5, min_distinct_amounts: int = 5) -> dict[str, str]:
    """funder -> 'hub' | 'mother'."""
    by_funder: dict[str, list[int]] = collections.defaultdict(list)
    for r in fund.values():
        if r.get("funder"):
            by_funder[r["funder"]].append(r.get("fund_lamports") or 0)
    kinds = {}
    for f, amts in by_funder.items():
        distinct = len({a // 1_000_000 for a in amts})
        if f in KNOWN_HUBS or (len(amts) >= min_creators and distinct >= min_distinct_amounts):
            kinds[f] = "hub"
        else:
            kinds[f] = "mother"
    return kinds


def operator_keys(launch: dict, fund: dict[str, dict], kinds: dict[str, str], creator_counts: dict[str, int],
                  reuse_min: int = 3) -> list[str]:
    keys = []
    c = launch.get("user")
    if creator_counts.get(c, 0) >= reuse_min:
        keys.append(f"creator:{c}")
    r = fund.get(c)
    if r and r.get("funder") and not r.get("saturated"):
        f = r["funder"]
        if kinds.get(f) == "hub":
            keys.append(f"fund:{f}:{r.get('fund_lamports', 0)}")
        else:
            keys.append(f"mother:{f}")
    return keys


def discover(launches: list[dict], fund: dict[str, dict], min_launches: int = 3) -> dict[str, dict]:
    creator_counts = collections.Counter(l.get("user") for l in launches)
    kinds = classify_funders(fund)
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for l in launches:
        for k in operator_keys(l, fund, kinds, creator_counts):
            groups[k].append(l)
    out = {}
    for k, ls in groups.items():
        if len(ls) < min_launches:
            continue
        ls.sort(key=lambda x: x["slot"])
        times = [x["block_time"] for x in ls if x.get("block_time")]
        gaps = [b - a for a, b in zip(times, times[1:])]
        dev = [sum(d["sol"] for d in x["dev_buys"]) / 1e9 for x in ls]
        creators = {x["user"] for x in ls}
        fresh = [fund.get(c, {}).get("n_before_create", 99) for c in creators]
        out[k] = {
            "key": k,
            "kind": k.split(":")[0],
            "funder_label": KNOWN_HUBS.get(k.split(":")[1], "") if k.split(":")[0] in ("fund", "mother") else "",
            "n_launches": len(ls),
            "n_creators": len(creators),
            "first": times[0] if times else None,
            "last": times[-1] if times else None,
            "median_gap_s": statistics.median(gaps) if gaps else None,
            "sol_quoted_frac": sum(1 for x in ls if x.get("sol_quoted")) / len(ls),
            "mayhem_frac": sum(1 for x in ls if x.get("mayhem")) / len(ls),
            "creator_fee_bps": collections.Counter(x.get("creator_fee_bps") for x in ls).most_common(2),
            "dev_buy_median": statistics.median(dev),
            "dev_buy_values": collections.Counter(round(d, 3) for d in dev).most_common(3),
            "uri_hosts": collections.Counter(x.get("uri_host") for x in ls).most_common(2),
            "median_fresh_txs": statistics.median(fresh) if fresh else None,
            "mints": [x["mint"] for x in ls],
        }
    return out


def main(argv: Iterable[str] | None = None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/bf")
    ap.add_argument("--min-launches", type=int, default=3)
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    d = Path(a.data)
    launches = load_jsonl(d / "launches.jsonl")
    fund = {r["creator"]: r for r in load_jsonl(d / "funding.jsonl") if "error" not in r}
    ops = discover(launches, fund, a.min_launches)
    outp = Path(a.out) if a.out else d / "operators.json"
    outp.write_text(json.dumps(ops, indent=1))
    print(f"{len(ops)} operator groups with >= {a.min_launches} launches -> {outp}")


if __name__ == "__main__":
    main()

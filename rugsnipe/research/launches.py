"""Extract launch / funding records from decoded transactions (shared by backfill and live recorder)."""
from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from ..constants import (
    COMPUTE_BUDGET_PROGRAM,
    DEFAULT_PUBKEY,
    PUMP_PROGRAM,
    SYSTEM_PROGRAM,
    WSOL_MINT,
)
from ..decode import NormTx, pump_events, pump_instructions, system_transfers

SOL_QUOTES = {DEFAULT_PUBKEY, WSOL_MINT, None, ""}


def uri_host(uri: str) -> str:
    try:
        if uri.startswith("ipfs://"):
            return "ipfs"
        h = urlparse(uri).netloc.lower()
        return h or uri[:24]
    except Exception:
        return "?"


def extract_launches(ntx: NormTx) -> list[dict]:
    """One record per CreateEvent in the tx (executed transactions only)."""
    evs = pump_events(ntx)
    creates = [(e, oi) for k, e, oi in evs if k == "create"]
    if not creates:
        return []
    trades = [(e, oi) for k, e, oi in evs if k == "trade"]
    outer_progs = []
    for ix in ntx.outer:
        if ix.program not in outer_progs:
            outer_progs.append(ix.program)
    pix = pump_instructions(ntx)
    via_cpi = not any(name in ("create", "create_v2") and not ix.inner for name, ix in pix)
    launcher = None
    if via_cpi:
        for oi_e in creates:
            oi = oi_e[1]
            if 0 <= oi < len(ntx.outer):
                launcher = ntx.outer[oi].program
    xfers = system_transfers(ntx, include_inner=False)
    out = []
    for c, oi in creates:
        mint = c.get("mint")
        dev = [t for t, _ in trades if t.get("mint") == mint]
        creator = c.get("creator") or c.get("user")
        user = c.get("user")
        pre = post = None
        if user in ntx.keys:
            i = ntx.keys.index(user)
            if i < len(ntx.pre_balances):
                pre, post = ntx.pre_balances[i], ntx.post_balances[i]
        out.append({
            "sig": ntx.signature, "slot": ntx.slot, "tx_index": ntx.tx_index, "block_time": ntx.block_time,
            "mint": mint, "bonding_curve": c.get("bonding_curve"), "user": user, "creator": creator,
            "fee_payer": ntx.fee_payer, "signers": ntx.signers,
            "name": c.get("name"), "symbol": c.get("symbol"), "uri": c.get("uri"), "uri_host": uri_host(c.get("uri") or ""),
            "quote_mint": c.get("quote_mint"), "sol_quoted": c.get("quote_mint") in SOL_QUOTES,
            "creator_fee_bps": c.get("creator_fee_bps"), "mayhem": c.get("mayhem"), "cashback": c.get("cashback"),
            "holder_reward": c.get("holder_reward"), "depth": c.get("depth"), "token_program": c.get("token_program"),
            "vsr0": c.get("vsr"), "vtr0": c.get("vtr"),
            "dev_buys": [{"user": t["user"], "sol": t["sol_amount"], "tok": t["token_amount"], "ix": t.get("ix_name"),
                          "fee": t.get("fee", 0), "cfee": t.get("creator_fee", 0), "bbfee": t.get("buyback_fee", 0),
                          "vsr": t["vsr"], "vtr": t["vtr"]} for t in dev],
            "pump_ixs": [n for n, _ in pix], "via_cpi": via_cpi, "launcher": launcher,
            "outer_programs": [p for p in outer_progs if p not in (COMPUTE_BUDGET_PROGRAM,)],
            "prio_fee": ntx.priority_fee_lamports(), "cu_limit": ntx.cu_limit, "cu_used": ntx.cu_consumed,
            "transfers_out": [(to, lam) for frm, to, lam, _ in xfers if frm == ntx.fee_payer],
            "creator_pre": pre, "creator_post": post,
        })
    return out


def inbound_transfers(ntx: NormTx, account: str) -> list[tuple[str, int]]:
    agg: dict[str, int] = {}
    for frm, to, lam, _ in system_transfers(ntx, include_inner=True):
        if to == account and frm and frm != account:
            agg[frm] = agg.get(frm, 0) + lam
    if not agg:
        # fall back to balance deltas (e.g. program-mediated payouts)
        d = ntx.sol_delta(account)
        if d and d > 0:
            # credit the account(s) with the largest negative delta
            best, bestd = None, 0
            for i, k in enumerate(ntx.keys):
                if i < len(ntx.pre_balances) and k != account:
                    dd = ntx.post_balances[i] - ntx.pre_balances[i]
                    if dd < bestd:
                        best, bestd = k, dd
            if best:
                agg[best] = d
    return sorted(agg.items(), key=lambda kv: -kv[1])


def balance_recipients(ntx: NormTx, source: str, min_lamports: int = 0) -> list[tuple[str, int]]:
    """Accounts credited in a tx where `source` lost SOL (robust to routing through
    temp accounts / programs: uses pre/post balances, not transfer instructions)."""
    if source not in ntx.keys or not ntx.pre_balances:
        return []
    i = ntx.keys.index(source)
    if ntx.post_balances[i] >= ntx.pre_balances[i]:
        return []
    out = []
    for j, k in enumerate(ntx.keys):
        if j == i or j >= len(ntx.pre_balances):
            continue
        d = ntx.post_balances[j] - ntx.pre_balances[j]
        if d >= max(1, min_lamports):
            out.append((k, d))
    return sorted(out, key=lambda kv: -kv[1])


def outbound_transfers(ntx: NormTx, account: str) -> list[tuple[str, int]]:
    agg: dict[str, int] = {}
    for frm, to, lam, _ in system_transfers(ntx, include_inner=True):
        if frm == account and to and to != account:
            agg[to] = agg.get(to, 0) + lam
    return sorted(agg.items(), key=lambda kv: -kv[1])


def funding_record(creator: str, sig_list: list[dict], first_tx: Optional[NormTx], create_slot: int) -> dict:
    """sig_list: newest-first signatures of the creator (as returned by RPC, <=1000)."""
    n = len(sig_list)
    rec = {"creator": creator, "n_sigs": n, "saturated": n >= 1000}
    before_create = [s for s in sig_list if s["slot"] < create_slot]
    rec["n_before_create"] = len(before_create)
    if not first_tx:
        return rec
    rec["first_sig"] = first_tx.signature
    rec["first_slot"] = first_tx.slot
    rec["first_time"] = first_tx.block_time
    rec["first_fee_payer"] = first_tx.fee_payer
    inb = inbound_transfers(first_tx, creator)
    rec["inbound"] = inb[:5]
    if inb:
        rec["funder"], rec["fund_lamports"] = inb[0]
    rec["first_tx_n_transfers"] = len(system_transfers(first_tx))
    rec["first_tx_programs"] = sorted({ix.program for ix in first_tx.outer} - {COMPUTE_BUDGET_PROGRAM})
    rec["first_is_create"] = any(k == "create" for k, _, _ in pump_events(first_tx))
    return rec

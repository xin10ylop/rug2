"""Fill-realistic replay of a pump.fun launch.

Design rules (lessons from the previous strategy that scored races it never won):
  * We never assume we are first. Entry is evaluated at several *positions*:
      after_bundle  - immediately behind the creator's atomic bundle (best case,
                      only reachable when we land in the creation block ahead of
                      every other sniper)
      slot0_end     - after every trade of the creation block
      slot+k_end    - after every trade of block S0+k
    The live system measures which position we actually get; validation uses
    that measured distribution, not the best case.
  * Exits trigger on what we would have *seen* (end of a slot) and execute
    `exit_delay_slots` later, after everything that happened in between - so a
    rug that lands in the next block is taken in full.
  * Every trade after our entry is re-applied to a curve that includes our
    own buy (SOL size held fixed for buys, token size for sells), so our
    price impact and the other traders' impact compound correctly.
  * All costs: pump protocol + creator fee (per curve), priority fee + tip on
    both legs, base fees. ATA rent is refunded on close and excluded.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .curve import Curve

LAMPORTS = 1_000_000_000


@dataclass(slots=True)
class Trade:
    slot: int
    idx: int  # tx index in block (or large number if unknown)
    seq: int  # order inside tx
    t: Optional[int]
    user: str
    payer: str
    is_buy: bool
    sol: int  # net SOL into/out of curve (event sol_amount)
    tok: int
    vsr: int  # post-trade reserves as recorded
    vtr: int
    sig: str
    ix: str = ""


@dataclass
class Tape:
    mint: str
    create_slot: int
    create_idx: int
    create_time: Optional[int]
    creator: str
    creator_fee_bps: int
    trades: list[Trade]
    bundle_end: int = 0  # index into trades: first trade NOT in the creator's atomic bundle
    bundle_wallets: set[str] = field(default_factory=set)
    failed_slot0: int = 0  # failed txs touching the curve in S0..S0+2 (competition signal)


def build_tape(launch: dict, rows: list[dict]) -> Optional[Tape]:
    """rows: backfill tape rows (one per tx touching the bonding curve)."""
    s0 = launch["slot"]
    cidx = launch.get("tx_index")
    trades: list[Trade] = []
    failed = 0
    for r in rows:
        if r.get("err"):
            if s0 <= r["slot"] <= s0 + 2:
                failed += 1
            continue
        for j, e in enumerate(r.get("trades") or []):
            trades.append(Trade(r["slot"], r["idx"] if r.get("idx") is not None else 1 << 20, j, r.get("t"), e["u"],
                                r.get("payer") or e["u"], e["b"], e["sol"], e["tok"], e["vsr"], e["vtr"], r["sig"],
                                e.get("ix") or ""))
    if not trades:
        return None
    trades.sort(key=lambda x: (x.slot, x.idx, x.seq))
    cfee = launch.get("creator_fee_bps") or 0
    tape = Tape(launch["mint"], s0, cidx if cidx is not None else -1, launch.get("block_time"), launch["user"],
                cfee if cfee else 30, trades, failed_slot0=failed)
    _mark_bundle(tape, launch)
    return tape


def _mark_bundle(tape: Tape, launch: dict) -> None:
    """Creator's atomic bundle = create tx + txs at consecutive indexes right after it in S0.

    Jito bundles execute back to back, so the bundle occupies create_idx..create_idx+k
    with no gap. Outsiders that land in the same block appear later with gaps.
    """
    s0, cidx = tape.create_slot, tape.create_idx
    end = 0
    expected = cidx
    wallets = {tape.creator}
    for i, tr in enumerate(tape.trades):
        if tr.slot != s0:
            break
        if cidx < 0:
            # unknown index: only the create tx itself is treated as bundle
            if tr.sig == launch["sig"]:
                end = i + 1
                wallets.add(tr.user)
                continue
            break
        if tr.sig == launch["sig"] or tr.idx in (expected, expected + 1):
            expected = max(expected, tr.idx)
            end = i + 1
            wallets.add(tr.user)
            wallets.add(tr.payer)
        else:
            break
    tape.bundle_end = end
    tape.bundle_wallets = wallets


def entry_index(tape: Tape, position: str) -> Optional[int]:
    """Index into tape.trades *before which* our buy executes."""
    s0 = tape.create_slot
    if position == "after_bundle":
        return tape.bundle_end
    if position == "slot0_end" or position.startswith("slot+"):
        k = 0 if position == "slot0_end" else int(position.split("+")[1].split("_")[0])
        target = s0 + k
        for i, tr in enumerate(tape.trades):
            if tr.slot > target:
                return i
        return len(tape.trades)
    raise ValueError(position)


@dataclass
class SimParams:
    size_sol: float = 0.25
    position: str = "slot+1_end"
    tp_pct: float = 100.0  # take profit on net position value vs cost
    sl_pct: Optional[float] = None  # stop loss on net value (None = ride to the floor)
    max_hold_s: float = 180.0  # time stop
    exit_delay_slots: int = 1  # our sell lands this many slots after the trigger slot
    entry_fee_lamports: int = 2_000_000  # priority + tip + base for the buy (0.002 SOL)
    exit_fee_lamports: int = 1_000_000  # priority + tip + base for the sell
    slot_seconds: float = 0.4
    max_entry_mcap_sol: Optional[float] = None  # skip if entry would be above this
    trail_pct: Optional[float] = None  # trailing stop from peak value after TP armed (optional)


@dataclass
class SimResult:
    entered: bool
    reason: str = ""
    entry_slot: int = 0
    entry_mcap: float = 0.0
    cost: int = 0  # lamports spent incl. fees
    proceeds: int = 0
    pnl: int = 0
    ret: float = 0.0
    exit_reason: str = ""
    hold_s: float = 0.0
    peak_ret: float = 0.0  # best net return available on seen slot boundaries
    tp_hit: bool = False


def _apply(c: Curve, tr: Trade) -> None:
    if tr.is_buy:
        if tr.sol <= 0:
            return
        tok = min(c.vtr * tr.sol // (c.vsr + tr.sol), c.rtr)
        c.apply_buy_sol(tr.sol, tok)
    else:
        if tr.tok <= 0:
            return
        gross = c.vsr * tr.tok // (c.vtr + tr.tok)
        c.apply_sell(tr.tok, gross)


def curve_before(tape: Tape, i: int) -> Curve:
    """Curve state right before trade i, from recorded post-trade reserves."""
    c = Curve.fresh(tape.creator_fee_bps)
    if i > 0:
        prev = tape.trades[i - 1]
        c.vsr, c.vtr = prev.vsr, prev.vtr
        c.rtr = c.vtr - (1_073_000_000_000_000 - 793_100_000_000_000)
    return c


def simulate(tape: Tape, p: SimParams) -> SimResult:
    i0 = entry_index(tape, p.position)
    if i0 is None:
        return SimResult(False, "no_entry_point")
    c = curve_before(tape, i0)
    entry_slot = tape.trades[i0 - 1].slot if i0 > 0 else tape.create_slot
    if p.position.startswith("slot") and i0 < len(tape.trades):
        entry_slot = max(entry_slot, tape.trades[i0].slot - 1)
    mcap = c.mcap_sol
    if p.max_entry_mcap_sol and mcap > p.max_entry_mcap_sol:
        return SimResult(False, "entry_mcap_too_high", entry_mcap=mcap)
    size = int(p.size_sol * LAMPORTS)
    tokens = c.buy_sol(size)
    if tokens <= 0:
        return SimResult(False, "no_tokens")
    cost = size + p.entry_fee_lamports
    t_entry = tape.trades[i0 - 1].t if i0 > 0 else tape.create_time
    res = SimResult(True, entry_slot=entry_slot, entry_mcap=mcap, cost=cost)

    def net_value(cv: Curve) -> int:
        return cv.sell_quote(tokens) - p.exit_fee_lamports

    peak = -1.0
    trigger_slot = None
    trigger_reason = ""
    trades = tape.trades
    n = len(trades)
    i = i0
    armed_peak = None
    while i < n:
        s = trades[i].slot
        # apply every trade of slot s
        while i < n and trades[i].slot == s:
            _apply(c, trades[i])
            i += 1
        v = net_value(c)
        r = v / cost - 1.0
        peak = max(peak, r)
        if trigger_slot is None:
            elapsed = (s - entry_slot) * p.slot_seconds
            if r >= p.tp_pct / 100.0:
                if p.trail_pct is None:
                    trigger_slot, trigger_reason = s, "tp"
                    res.tp_hit = True
                else:
                    res.tp_hit = True
                    armed_peak = max(armed_peak or r, r)
            if armed_peak is not None and trigger_slot is None:
                armed_peak = max(armed_peak, r)
                if (1 + r) <= (1 + armed_peak) * (1 - p.trail_pct / 100.0):
                    trigger_slot, trigger_reason = s, "trail"
            if trigger_slot is None and p.sl_pct is not None and r <= -p.sl_pct / 100.0:
                trigger_slot, trigger_reason = s, "sl"
            if trigger_slot is None and elapsed >= p.max_hold_s:
                trigger_slot, trigger_reason = s, "time"
        if trigger_slot is not None and s >= trigger_slot + p.exit_delay_slots:
            break
        if trigger_slot is not None and i < n and trades[i].slot > trigger_slot + p.exit_delay_slots:
            break
    if trigger_slot is None:
        trigger_reason = "tape_end"
    proceeds = max(0, net_value(c))
    res.proceeds = proceeds
    res.pnl = proceeds - cost
    res.ret = res.pnl / cost
    res.exit_reason = trigger_reason
    res.peak_ret = peak
    last_slot = trades[i - 1].slot if i > 0 else entry_slot
    res.hold_s = (last_slot - entry_slot) * p.slot_seconds
    return res


# --------------------------------------------------------------------------- launch descriptors


def describe(tape: Tape, horizon_s: float = 600.0) -> dict:
    """Shape features of a launch (no entry assumptions)."""
    tr = tape.trades
    s0 = tape.create_slot
    b_end = tape.bundle_end
    bundle_sol = sum(t.sol for t in tr[:b_end] if t.is_buy)
    bundle_tok = sum(t.tok for t in tr[:b_end] if t.is_buy)
    after_bundle = curve_before(tape, b_end)
    slot0 = [t for t in tr[b_end:] if t.slot == s0]
    s1 = [t for t in tr if t.slot in (s0 + 1, s0 + 2)]
    outsiders_early = {t.user for t in tr[b_end:] if t.is_buy and t.slot <= s0 + 2 and t.user not in tape.bundle_wallets}
    peak_mcap = max((t.vsr / t.vtr * 1e6 for t in tr), default=0.0)
    end_s1 = curve_before(tape, entry_index(tape, "slot+1_end"))
    # biggest single-slot drop (the rug candle)
    by_slot: dict[int, tuple[float, float]] = {}
    prev_price = None
    best_drop, rug_slot = 0.0, None
    for t in tr:
        price = t.vsr / t.vtr
        if prev_price is not None and t.slot not in by_slot:
            by_slot[t.slot] = (prev_price, price)
        if t.slot in by_slot:
            by_slot[t.slot] = (by_slot[t.slot][0], price)
        prev_price = price
    for s, (a, b) in by_slot.items():
        if a > 0 and (1 - b / a) > best_drop:
            best_drop, rug_slot = 1 - b / a, s
    return {
        "mint": tape.mint,
        "bundle_trades": b_end,
        "bundle_sol": bundle_sol / LAMPORTS,
        "bundle_supply_pct": bundle_tok / 1e15 * 100,
        "mcap_after_bundle": after_bundle.mcap_sol,
        "mcap_slot1_end": end_s1.mcap_sol,
        "slot0_outsider_buys": sum(1 for t in slot0 if t.is_buy),
        "slot0_outsider_sol": sum(t.sol for t in slot0 if t.is_buy) / LAMPORTS,
        "s1_s2_buy_sol": sum(t.sol for t in s1 if t.is_buy) / LAMPORTS,
        "early_outsiders": len(outsiders_early),
        "failed_early_txs": tape.failed_slot0,
        "peak_mcap": peak_mcap,
        "peak_x_vs_slot1": peak_mcap / end_s1.mcap_sol if end_s1.mcap_sol else 0,
        "rug_drop": best_drop,
        "rug_after_s": (rug_slot - s0) * 0.4 if rug_slot else None,
        "n_trades": len(tr),
        "n_traders": len({t.user for t in tr}),
    }


def wallet_pnl(tape: Tape, final_curve: Optional[Curve] = None) -> dict[str, dict]:
    """Realized + marked PnL per wallet over the tape (lamports, before priority fees)."""
    c_final = final_curve or curve_before(tape, len(tape.trades))
    out: dict[str, dict] = {}
    for t in tape.trades:
        w = out.setdefault(t.user, {"spent": 0, "recv": 0, "tok": 0, "first_slot": t.slot, "first_idx": t.idx,
                                    "n_buys": 0, "n_sells": 0})
        fee_mult = 1 + (95 + tape.creator_fee_bps) / 10_000
        if t.is_buy:
            w["spent"] += int(t.sol * fee_mult)
            w["tok"] += t.tok
            w["n_buys"] += 1
        else:
            w["recv"] += int(t.sol / fee_mult)
            w["tok"] -= t.tok
            w["n_sells"] += 1
    for w in out.values():
        mark = c_final.sell_quote(max(0, w["tok"])) if w["tok"] > 0 else 0
        w["pnl"] = w["recv"] + mark - w["spent"]
    return out

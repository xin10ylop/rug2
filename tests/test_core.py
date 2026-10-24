"""Regression tests on real mainnet transactions (fixtures fetched 2026-10-24)."""
import asyncio
import json
import os
import tempfile
from pathlib import Path

import pytest

from rugsnipe.curve import Curve
from rugsnipe.decode import decode_create_ix, dev_buys_preexec, from_rpc_json, pump_events, pump_instructions
from rugsnipe.research.launches import balance_recipients, extract_launches
from rugsnipe.sim import SimParams, build_tape, describe, entry_index, simulate
from rugsnipe import txbuild as T

FX = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FX / name).read_text())


def test_create_ix_matches_create_event():
    ntx = from_rpc_json(load("create_v2_devbuy.json"))
    ev = [e for k, e, _ in pump_events(ntx) if k == "create"][0]
    ix = [ix for n, ix in pump_instructions(ntx, include_inner=False) if n in ("create", "create_v2")][0]
    c = decode_create_ix(ntx, ix)
    assert c["token_program"] == ev["token_program"]
    assert c["mint"] == ev["mint"] and c["user"] == ev["user"]
    assert c["mayhem"] == ev["mayhem"] and c["sol_quoted"]
    curve_creator = str(T.holder_rewards_pda(T.PK(c["mint"]))) if c["holder_reward"] else c["creator"]
    assert curve_creator == ev["creator"]
    assert c["creator_fee_bps"] == ev["creator_fee_bps"]


def test_dev_buy_preexec_matches_trade_event_and_curve_math():
    ntx = from_rpc_json(load("create_v2_devbuy.json"))
    mint = [e for k, e, _ in pump_events(ntx) if k == "create"][0]["mint"]
    trades = [e for k, e, _ in pump_events(ntx) if k == "trade"]
    devs = dev_buys_preexec(ntx, mint)
    assert devs and trades
    assert devs[0]["token_amount"] == trades[0]["token_amount"]
    c = Curve.fresh()
    cost = c.sol_for_tokens(trades[0]["token_amount"])
    assert abs(cost - trades[0]["sol_amount"]) <= 2  # lamport rounding
    c.apply_buy_sol(cost, trades[0]["token_amount"])
    assert abs(c.vsr - trades[0]["vsr"]) <= 2 and c.vtr == trades[0]["vtr"]


def test_fee_structure_from_events():
    for name in ("buy.json", "sell.json"):
        ntx = from_rpc_json(load(name))
        e = [e for k, e, _ in pump_events(ntx) if k == "trade"][0]
        assert e["fee_bps"] == 95
        assert e["creator_fee_bps"] in (30, 0) or e["creator_fee_bps"] <= 300
        # buyback is a slice of the protocol fee, not an extra charge
        assert e["buyback_fee"] <= e["fee"]


def test_extract_launch_record():
    ntx = from_rpc_json(load("create_v2_devbuy.json"))
    recs = extract_launches(ntx)
    assert len(recs) == 1 and recs[0]["sol_quoted"] and recs[0]["dev_buys"]


def test_funding_detected_through_temp_account_routing():
    tx = load("funding_djtd.json")
    ntx = from_rpc_json(tx)
    rec = balance_recipients(ntx, "DJTDXMc7BuN4t2PPTEnfmAPSJrwMgsEXrvdbWbxkvga8", 2_000_000_000)
    fresh = [[a, lam] for a, lam in rec if ntx.pre_balances[ntx.keys.index(a)] == 0]
    assert fresh == tx["_expected_fresh_recipients"]


def test_sim_positions_are_ordered_and_costs_applied():
    launch = load("launch_djtd.json")
    rows = [json.loads(x) for x in (FX / "tape_djtd.jsonl").read_text().splitlines() if x.strip()]
    tape = build_tape(launch, rows)
    assert tape is not None and tape.bundle_end >= 1
    i_b = entry_index(tape, "after_bundle")
    i_0 = entry_index(tape, "slot0_end")
    i_1 = entry_index(tape, "slot+1_end")
    assert i_b <= i_0 <= i_1
    d = describe(tape)
    assert d["mcap_after_bundle"] > 28.0


def test_sim_roundtrip_costs_on_quiet_tape():
    """Buy then sell with no other trades: loses both 1.25% fee legs + fixed fees, nothing else."""
    from rugsnipe.sim import Tape, Trade
    c = Curve.fresh()
    tok = c.tokens_for_sol(1_000_000_000)
    c.buy_sol(1_000_000_000)
    dev = Trade(100, 5, 0, 0, "dev", "dev", True, 987_654_320, tok, c.vsr, c.vtr, "sig0")
    later = Trade(200, 5, 0, 80, "x", "x", True, 1, 0, c.vsr, c.vtr, "sig1")  # no-op trade far later
    tape = Tape("m", 100, 5, 0, "dev", 30, [dev, later], bundle_end=1, bundle_wallets={"dev"})
    p = SimParams(size_sol=0.25, position="slot+1_end", tp_pct=1000, max_hold_s=0.1, exit_delay_slots=0,
                  entry_fee_lamports=2_000_000, exit_fee_lamports=1_000_000)
    r = simulate(tape, p)
    fees = 0.25 * 0.0125 * 2 + 0.003  # both fee legs + fixed costs, in SOL
    assert r.entered and abs(r.ret + fees / 0.252) < 0.003


def test_min_tokens_guard():
    size = 100_000_000
    c = Curve.fresh()
    tok = c.tokens_for_sol(size)
    # guard at current mcap * 1.15 must accept the current fill and reject a fill 30% worse
    mt = T.min_tokens_for_max_mcap(size, c.mcap_sol * 1.15, c.fee_bps)
    assert tok >= mt
    assert int(tok / 1.3) < mt


def test_buy_and_sell_tx_build():
    from solders.hash import Hash
    from solders.keypair import Keypair
    kp = Keypair()
    k = T.CurveKeys(T.PK("GdZ3u6ofnYDnChAzyENbjZuPB34sgHKpTybuM6g6pump"), kp.pubkey())
    bh = Hash.default()
    btx = T.build_buy_tx(kp, k, 100_000_000, 1, bh, T.Fees(tip_lamports=10_000, cu_price_micro=1000))
    stx = T.build_sell_tx(kp, k, 1000, 0, bh, T.Fees(tip_lamports=10_000))
    assert len(bytes(btx)) < 1232 and len(bytes(stx)) < 1232


def _engine(tmp, me, **op):
    from rugsnipe.config import Config
    from rugsnipe.live.engine import Engine
    ops = {"operators": [{"id": "t", "funders": [], "policy": {"enabled": True}, **op}]}
    Path(tmp, "ops.json").write_text(json.dumps(ops))
    cfg = Config(rpc_url="http://127.0.0.1:1", grpc_url="http://127.0.0.1:1", grpc_token=None,
                 keypair_path=os.path.join(tmp, "none.json"), db_path=os.path.join(tmp, "db.sqlite"),
                 operators_path=os.path.join(tmp, "ops.json"), mode="paper", status_path=os.path.join(tmp, "s.json"))
    e = Engine(cfg)
    e.me = me
    return e


def test_engine_parses_own_buy_and_sell():
    from rugsnipe.live.engine import Position
    buy = from_rpc_json(load("buy.json"))
    sell = from_rpc_json(load("sell.json"))
    eb = [e for k, e, _ in pump_events(buy) if k == "trade"][0]
    with tempfile.TemporaryDirectory() as tmp:
        eng = _engine(tmp, eb["user"])
        mint = eb["mint"]
        k = T.CurveKeys(T.PK(mint), T.PK(eb["creator"]))
        pos = Position(mint, "t", k, buy.slot - 1, buy.signature, eng.store.attempt(mint=mint, side="buy"), 0)
        eng.positions[mint] = pos
        eng.sig_to_mint[buy.signature] = (mint, "buy")
        eng._on_our_tx(buy)
        assert pos.state == "open" and pos.tokens == eb["token_amount"]
        assert pos.cost == -buy.sol_delta(eb["user"]) and pos.cost > eb["sol_amount"]
        es = [e for k2, e, _ in pump_events(sell) if k2 == "trade"][0]
        if es["user"] == eb["user"]:
            eng.sig_to_mint[sell.signature] = (mint, "sell")
            pos.state = "selling"
            eng._on_our_tx(sell)
            assert pos.state == "closed"


def test_burst_of_creates_counts_launch_numbers_synchronously():
    """Two creates by one creator inside the same second: only the first may be a 'first launch'."""
    import copy
    ntx = from_rpc_json(load("create_v2_devbuy.json"))
    ix = [ix for n, ix in pump_instructions(ntx, include_inner=False) if n in ("create", "create_v2")][0]
    creator = decode_create_ix(ntx, ix)["user"]
    ntx2 = copy.deepcopy(ntx)
    mint_idx = ix.accounts[0]
    ntx2.keys[mint_idx] = "So11111111111111111111111111111111111111112"  # a different mint
    seen = []
    with tempfile.TemporaryDirectory() as tmp:
        eng = _engine(tmp, "me")
        eng.book.add_creator(creator, "t", "f", 1)

        async def fake_decide(ntx_, c, info, launch_no, t, source):
            seen.append(launch_no)

        eng._decide = fake_decide

        async def go():
            eng._maybe_trigger(ntx, 0.0, "deshred")
            eng._maybe_trigger(ntx2, 0.0, "deshred")
            await asyncio.sleep(0)
        asyncio.run(go())
    assert seen == [0, 1]


def test_funding_source_sees_through_temp_account_routing():
    from rugsnipe.research.launches import funding_source
    tx = load("funding_djtd.json")
    ntx = from_rpc_json(tx)
    creator, lam = tx["_expected_fresh_recipients"][0]
    src = funding_source(ntx, creator)
    assert src[0] == "DJTDXMc7BuN4t2PPTEnfmAPSJrwMgsEXrvdbWbxkvga8" and src[1] == lam


def test_refunded_creator_is_rearmed_and_survives_restart():
    """Operators recycle wallets: each new funding starts a new 'first launch', older fundings never do."""
    import copy
    tx = load("funding_djtd.json")
    ntx = from_rpc_json(tx)
    relay = "DJTDXMc7BuN4t2PPTEnfmAPSJrwMgsEXrvdbWbxkvga8"
    creator, _ = tx["_expected_fresh_recipients"][0]
    later, earlier = copy.deepcopy(ntx), copy.deepcopy(ntx)
    later.slot, earlier.slot = ntx.slot + 1000, ntx.slot - 1000
    with tempfile.TemporaryDirectory() as tmp:
        eng = _engine(tmp, "me", funders=[relay], fund_min_sol=0.5, fund_max_sol=100)
        assert eng._scan_funding(ntx, relay, "t") == 1
        ci = eng.book.creators[creator]
        assert ci.fund_slot == ntx.slot and ci.launches == 0
        ci.launches = ci.seen = 2  # it launched twice
        assert eng._scan_funding(ntx, relay, "t") == 0 and ci.launches == 2  # same funding seen again
        assert eng._scan_funding(earlier, relay, "t") == 0 and ci.launches == 2  # older funding
        assert eng._scan_funding(later, relay, "t") == 1  # funded again: next launch is a first launch
        assert ci.fund_slot == later.slot and ci.launches == ci.seen == 0
        eng.store.bump_launches(creator)
        eng.store.commit()
        eng2 = _engine(tmp, "me", funders=[relay], fund_min_sol=0.5, fund_max_sol=100)
        eng2._restore()
        ci2 = eng2.book.creators[creator]
        assert ci2.fund_slot == later.slot and ci2.launches == 1 and ci2.expires > ci2.fund_time


def test_first_after_funding_matches_engine_rule():
    from rugsnipe.research.operator_history import first_after_funding
    L = [{"mint": m, "user": u, "slot": s, "tx_index": i} for m, u, s, i in [
        ("a1", "A", 110, 0), ("a2", "A", 120, 0), ("a3", "A", 210, 1), ("a4", "A", 210, 5),
        ("b1", "B", 50, 0), ("b2", "B", 70, 0)]]
    F = {"A": [[100, 0, 1, 0], [115, 0, 1, 5_000_000], [200, 0, 1, 0]], "B": [[60, 0, 1, 0]]}  # 115: top-up
    assert first_after_funding(L, F) == {"a1", "a3", "b2"}


def test_learner_scores_repeat_launches_as_control_group():
    import shutil
    import time as _t
    from rugsnipe.live.learner import run_learner
    launch = load("launch_djtd.json")
    with tempfile.TemporaryDirectory() as tmp:
        eng = _engine(tmp, "me")
        tdir = Path(tmp, "tapes")
        tdir.mkdir()
        for mint, decision, reason in [("first111", "paper", None), ("repeat11", "skip", "not_first_launch(1)")]:
            shutil.copy(FX / "tape_djtd.jsonl", tdir / f"{mint}.jsonl")
            eng.store.upsert_launch(mint, slot=launch["slot"], tx_index=launch["tx_index"], creator=launch["user"],
                                    operator="t", block_time=launch["block_time"], t_deshred=_t.time() - 3600,
                                    decision=decision, reason=reason)
        eng.store.commit()
        rep = asyncio.run(run_learner(eng.store, eng.book, None, tape_dir=str(tdir), apply=False,
                                      out_path=os.path.join(tmp, "l.json")))
    st = rep["operators"]["t"]
    assert st["eligible"] == 1 and st["sim_slot3"]["n"] == 1
    assert st["sim_repeat_slot3"]["n"] == 1 and st["sim_repeat_slot3"]["mean"] == st["sim_slot3"]["mean"]

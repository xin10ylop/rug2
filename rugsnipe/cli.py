"""rugsnipe command line.

  python -m rugsnipe.cli live    --config config/config.toml     # run the engine (paper or live per config)
  python -m rugsnipe.cli report  --config config/config.toml     # live stats, fills, reconciliation
  python -m rugsnipe.cli learn   --config config/config.toml     # one-off re-validation (no policy changes)
  python -m rugsnipe.cli keygen  --out keys/trader.json          # new trading keypair
  python -m rugsnipe.cli check   --config config/config.toml     # connectivity + simulate a buy (nothing sent)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
import time


def _setup_logging(level: str, path: str | None = None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if path:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(path))
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
                        datefmt="%H:%M:%S", handlers=handlers)


def cmd_live(a):
    from .config import load
    from .live.engine import Engine
    cfg = load(a.config)
    _setup_logging(cfg.log_level, "data/engine.log")
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(Engine(cfg).run())


def cmd_keygen(a):
    from solders.keypair import Keypair
    if os.path.exists(a.out):
        raise SystemExit(f"{a.out} exists - refusing to overwrite")
    kp = Keypair()
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(list(bytes(kp)), f)
    os.chmod(a.out, 0o600)
    print(f"wrote {a.out}\npubkey: {kp.pubkey()}")


def cmd_report(a):
    from .config import load
    from .live.store import Store
    cfg = load(a.config)
    st = Store(cfg.db_path)
    since = time.time() - a.hours * 3600
    print(f"=== rugsnipe report (last {a.hours}h) mode={cfg.mode}")
    if os.path.exists(cfg.status_path):
        s = json.load(open(cfg.status_path))
        print(f"status age {time.time()-s['t']:.0f}s  slot={s['slot']} balance={s['balance_sol']:.4f} "
              f"day_pnl={s['day_pnl_sol']:+.4f} streams={s['streams']}")
        print(f"operators={s['operators']} trigger->send p50={s['trigger_to_send_ms_p50']}ms routes={s['routes']}")
    rows = st.q("SELECT operator, decision, reason, COUNT(*) FROM launches WHERE operator IS NOT NULL AND "
                "COALESCE(t_deshred,t_processed) > ? GROUP BY 1,2,3 ORDER BY 1,4 DESC", (since,))
    print("\n-- watched-operator launches by decision")
    for r in rows:
        print("  ", r)
    att = st.q("SELECT status, slot_offset, sol_delta, t_sent - t_trigger FROM attempts WHERE side='buy' AND t_trigger > ?",
               (since,))
    print(f"\n-- buys: {len(att)} sent")
    by = {}
    for status, off, delta, dt in att:
        by.setdefault(status, []).append((off, delta, dt))
    for k, v in by.items():
        offs = [o for o, _, _ in v if o is not None]
        dts = [d * 1000 for _, _, d in v if d is not None]
        print(f"   {k:8s} n={len(v)} slot_offset med={statistics.median(offs) if offs else None} "
              f"dist={sorted(offs)[:30]} trigger->sent med={statistics.median(dts) if dts else 0:.0f}ms")
    pos = st.q("SELECT operator, pnl, cost, exit_reason, closed - opened FROM positions WHERE status='closed' AND opened > ?",
               (since,))
    print(f"\n-- closed positions: {len(pos)}")
    if pos:
        rets = [p / c for _, p, c, _, _ in pos if c]
        tot = sum(p for _, p, _, _, _ in pos)
        print(f"   total pnl {tot/1e9:+.4f} SOL  mean ret {statistics.fmean(rets):+.3f}  "
              f"win {sum(1 for r in rets if r > 0)/len(rets):.2f}")
        reasons = {}
        for _, p, c, r, h in pos:
            reasons.setdefault(r, []).append(p / c if c else 0)
        for r, v in reasons.items():
            print(f"   exit {r:10s} n={len(v)} mean={statistics.fmean(v):+.3f}")
    if os.path.exists("data/learner.json"):
        L = json.load(open("data/learner.json"))
        print(f"\n-- learner ({time.time()-L['t']:.0f}s ago)")
        for op, s in L["operators"].items():
            print(f"   {op}: {json.dumps(s)}")


def cmd_learn(a):
    from .config import load
    from .live.learner import run_learner
    from .live.operators import OperatorBook
    from .live.store import Store
    from .rpc import RPC
    cfg = load(a.config)
    _setup_logging("INFO")

    async def go():
        async with RPC([cfg.rpc_url], 20) as rpc:
            rep = await run_learner(Store(cfg.db_path), OperatorBook(cfg.operators_path), rpc, hours=a.hours,
                                    tape_minutes=cfg.tape_minutes, apply=False, out_path="data/learner_manual.json")
            print(json.dumps(rep, indent=1))
    asyncio.run(go())


def cmd_check(a):
    """Connectivity check + simulate a real buy transaction on the most recent SOL-quoted launch (never sent)."""
    import base64
    from solders.message import MessageV0
    from solders.transaction import VersionedTransaction
    from .config import load
    from .constants import PUMP_MINT_AUTHORITY
    from .curve import Curve
    from .decode import from_rpc_json, pump_instructions, decode_create_ix
    from .live.engine import load_keypair
    from .rpc import RPC
    from .grpcstream import latest_blockhash
    from . import txbuild as T
    cfg = load(a.config)

    async def go():
        bh, h = await latest_blockhash(cfg.grpc_url, cfg.grpc_token)
        print(f"gRPC ok: blockhash {bh[:12]} height {h}")
        async with RPC([cfg.rpc_url], 10) as rpc:
            print("RPC slot:", await rpc.call("getSlot", [{"commitment": "processed"}]))
            kp = load_keypair(cfg.keypair_path) if os.path.exists(cfg.keypair_path) else None
            if kp:
                bal = (await rpc.call("getBalance", [str(kp.pubkey())]))["value"]
                print(f"wallet {kp.pubkey()} balance {bal/1e9:.4f} SOL")
            payer = kp.pubkey() if kp else T.PK("5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9")
            sigs = await rpc.sigs(PUMP_MINT_AUTHORITY, limit=30)
            for s in sigs:
                if s.get("err"):
                    continue
                n = from_rpc_json(await rpc.get_tx(s["signature"]))
                cr = [decode_create_ix(n, ix) for name, ix in pump_instructions(n, False) if name in ("create", "create_v2")]
                if not cr or not cr[0]["sol_quoted"] or cr[0]["mayhem"]:
                    continue
                c = cr[0]
                creator = T.holder_rewards_pda(T.PK(c["mint"])) if c["holder_reward"] else T.PK(c["creator"])
                k = T.CurveKeys(T.PK(c["mint"]), creator, T.PK(c["token_program"]))
                acc = await rpc.account(str(k.bonding_curve))
                data = base64.b64decode(acc["data"][0])
                vtr, vqr, rtr = [int.from_bytes(data[8 + 8 * i:16 + 8 * i], "little") for i in range(3)]
                if data[48]:
                    continue
                cv = Curve(vqr, vtr, rtr)
                ixs = [T.set_compute_unit_limit(cfg.buy_cu_limit), T.create_ata_idempotent(payer, payer, k.mint, k.token_program),
                       T.buy_exact_sol_in_ix(payer, k, 10_000_000, int(cv.tokens_for_sol(10_000_000) * 0.9))]
                from solders.hash import Hash
                from solders.signature import Signature
                msg = MessageV0.try_compile(payer, ixs, [], Hash.from_string(bh))
                tx = VersionedTransaction.populate(msg, [Signature.default()])
                r = await rpc.call("simulateTransaction", [base64.b64encode(bytes(tx)).decode(), {
                    "encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": True, "commitment": "processed"}])
                v = r["value"]
                print(f"simulated buy of 0.01 SOL on {c['mint']} (mcap {cv.mcap_sol:.1f} SOL): err={v['err']} "
                      f"cu={v.get('unitsConsumed')}  [nothing was sent]")
                if v["err"]:
                    print("\n".join((v.get("logs") or [])[-10:]))
                break
    asyncio.run(go())


def main(argv=None):
    ap = argparse.ArgumentParser(prog="rugsnipe")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("live"); p.add_argument("--config", default="config/config.toml"); p.set_defaults(fn=cmd_live)
    p = sub.add_parser("report"); p.add_argument("--config", default="config/config.toml")
    p.add_argument("--hours", type=float, default=24); p.set_defaults(fn=cmd_report)
    p = sub.add_parser("learn"); p.add_argument("--config", default="config/config.toml")
    p.add_argument("--hours", type=float, default=24); p.set_defaults(fn=cmd_learn)
    p = sub.add_parser("keygen"); p.add_argument("--out", default="keys/trader.json"); p.set_defaults(fn=cmd_keygen)
    p = sub.add_parser("check"); p.add_argument("--config", default="config/config.toml"); p.set_defaults(fn=cmd_check)
    a = ap.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()

"""Live engine.

Streams (Yellowstone gRPC):
  deshred   : every pump.fun create, *before execution* (mint-authority filter)
  processed : watched funders/creators (funding + chain detection), executed creates,
              trade tapes of tracked curves, our own wallet, slots, block meta (blockhashes)

Flow:
  funder (watched) --credits fresh wallet--> creator registered for that operator
  creator launches (seen in deshred) --> launch matches operator signature & policy
      --> buy_exact_sol_in with a price guard, short-lived blockhash, multi-route send
  position: TP / time stop / creator-dump exit, sells retried with escalating slippage
Every decision, send, landing and exit is written to SQLite for reconciliation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from .. import constants as C
from ..config import Config
from ..curve import Curve
from ..decode import (from_grpc_tx, pump_events, pump_instructions, decode_create_ix, dev_buys_preexec,
                      NormTx)
from ..grpcstream import G, Subscription
from ..research.launches import balance_recipients
from ..rpc import RPC
from ..txbuild import (CurveKeys, Fees, PK, TOKEN_2022, bonding_curve_pda, build_buy_tx, build_sell_tx,
                       holder_rewards_pda, min_tokens_for_max_mcap)
from .operators import OperatorBook
from .senders import MultiSender, Route
from .store import Store

log = logging.getLogger("engine")
LAMPORTS = 1_000_000_000
ATA_RENT = {C.TOKEN_2022_PROGRAM: 2_074_080, C.TOKEN_PROGRAM: 2_039_280}  # refunded when we close it on the sell


@dataclass
class Track:
    mint: str
    bonding_curve: str
    operator: str
    creator: str
    create_slot: int
    until: float
    keys: Optional[CurveKeys] = None
    curve: Optional[Curve] = None
    dev_wallets: set = field(default_factory=set)
    n_trades: int = 0


@dataclass
class Position:
    mint: str
    operator: str
    keys: CurveKeys
    create_slot: int
    buy_sig: str
    attempt_id: int
    size: int
    state: str = "pending"  # pending -> open -> selling -> closed | failed
    opened: float = 0.0
    entry_slot: int = 0
    tokens: int = 0
    cost: int = 0  # lamports actually spent (balance delta, incl. fees, tip, ATA rent)
    proceeds: int = 0
    exit_reason: str = ""
    sell_attempts: int = 0
    last_sell_t: float = 0.0
    sell_sigs: list = field(default_factory=list)
    t_trigger: float = 0.0
    tp_pct: float = 30.0
    max_hold_s: float = 60.0
    sl_pct: Optional[float] = None
    reconciling: bool = False


def load_keypair(path: str) -> Keypair:
    raw = json.loads(open(path).read())
    return Keypair.from_bytes(bytes(raw))


class Engine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.store = Store(cfg.db_path)
        self.book = OperatorBook(cfg.operators_path)
        self.paper = cfg.mode != "live"
        if os.path.exists(cfg.keypair_path):
            self.kp = load_keypair(cfg.keypair_path)
        elif self.paper:
            self.kp = Keypair()  # throwaway key: paper mode never sends
            log.warning("no keypair at %s - paper mode with an ephemeral key", cfg.keypair_path)
        else:
            raise SystemExit(f"keypair not found: {cfg.keypair_path}")
        self.me = str(self.kp.pubkey())
        self.sender = MultiSender([Route(r.name, r.url, r.headers, r.kind, r.enabled, tuple(r.sides), r.warm_url)
                                   for r in cfg.routes])
        self.rpc = RPC([cfg.rpc_url], 20, concurrency=8)
        self.slot = 0
        self.blockhashes: deque = deque(maxlen=300)  # (slot, block_height, blockhash)
        self.tracks: dict[str, Track] = {}  # bonding_curve -> Track
        self.tracks_by_mint: dict[str, Track] = {}
        self.positions: dict[str, Position] = {}
        self.sig_to_mint: dict[str, tuple[str, str]] = {}  # our sig -> (mint, side)
        self.decided: dict[str, str] = {}  # mint -> decision
        self.balance = 0
        self.day_pnl = 0
        self.day = time.strftime("%Y-%m-%d", time.gmtime())
        self.consec_losses: dict[str, int] = {}
        self._filter_version = -1
        self.proc_sub: Optional[Subscription] = None
        self.deshred_sub: Optional[Subscription] = None
        self.lat = deque(maxlen=200)  # (deshred->sent ms)
        self._bh_time = 0.0
        self._paper_sims: dict = {}
        self._executed: set = set()
        self.started = time.time()

    # ================================================================ lifecycle
    async def run(self):
        await self.rpc.__aenter__()
        await self.sender.start()
        self._restore()
        await self._bootstrap_creators()
        await self._refresh_balance()
        self.proc_sub = Subscription("processed", self.cfg.grpc_url, self.cfg.grpc_token, self._proc_request,
                                     self._on_processed)
        self.deshred_sub = Subscription("deshred", self.cfg.grpc_url, self.cfg.grpc_token, self._deshred_request,
                                        self._on_deshred, deshred=True)
        tasks = [self.proc_sub.run(), self.deshred_sub.run(), self._timer(), self._housekeeping(), self._status_loop(),
                 self._learner_loop(), self.sender.keepalive()]
        log.info("engine up mode=%s wallet=%s operators=%s creators=%d", self.cfg.mode, self.me,
                 list(self.book.ops), len(self.book.creators))
        try:
            await asyncio.gather(*tasks)
        finally:
            self.store.commit()
            await self.sender.close()
            await self.rpc.__aexit__(None, None, None)

    def _restore(self):
        for creator, op, launches, funder, lam, slot, t, expires in self.store.active_creators():
            if op in self.book.ops and self.book.add_creator(creator, op, funder, lam, slot or 0, t or 0.0):
                ci = self.book.creators[creator]
                ci.launches = ci.seen = launches
                ci.expires = expires
        for addr, op, role in self.store.q("SELECT addr, operator, role FROM funders"):
            if op in self.book.ops:
                self.book.add_funder(addr, op, role)

    async def _bootstrap_creators(self):
        """Creators funded shortly before start-up are found from the funders' recent history."""
        for addr, op_id in list(self.book.funder_to_op.items()):
            op = self.book.ops[op_id]
            since = int(time.time() - op.creator_ttl_h * 3600)
            data, token = [], None
            try:
                for _ in range(6):
                    opts = {"transactionDetails": "full", "sortOrder": "desc", "limit": 100, "encoding": "json",
                            "maxSupportedTransactionVersion": 1, "commitment": "confirmed",
                            "filters": {"blockTime": {"gte": since}, "status": "succeeded"}}
                    if token:
                        opts["paginationToken"] = token
                    res = await self.rpc.call("getTransactionsForAddress", [addr, opts])
                    data += res.get("data") or []
                    token = res.get("paginationToken")
                    if not token or len(res.get("data") or []) < 100:
                        break
            except Exception as e:
                log.warning("bootstrap %s failed (%s) - only new fundings will be tracked", addr[:8], str(e)[:120])
                if not data:
                    continue
            n = 0
            from ..decode import from_rpc_json
            for tx in data:  # newest first: a creator's latest funding is the one that registers it
                ntx = from_rpc_json(tx)
                n += self._scan_funding(ntx, addr, op_id, bootstrap=True)
            log.info("bootstrap %s: %d creators funded in the last %.0fh", addr[:8], n, op.creator_ttl_h)
        # launches made since each creator's latest funding (before start-up, or while the engine was down)
        # must count, or a repeat launch would look like a first launch
        live = [ci for ci in self.book.creators.values() if ci.funder != "static"]
        counts = await asyncio.gather(*[self._count_launches(ci) for ci in live])
        for ci, k in zip(live, counts):
            if k != ci.launches:
                ci.launches = ci.seen = k
                self.store.x("UPDATE fundings SET launches=? WHERE creator=?", (k, ci.creator))
        log.info("creators: %d watched, %d already launched since their latest funding", len(live),
                 sum(1 for k in counts if k))
        self.store.commit()

    async def _count_launches(self, ci) -> int:
        from ..decode import from_rpc_json
        since = int(ci.fund_time) - 120 if ci.fund_time else int(time.time() - 48 * 3600)
        try:
            res = await self.rpc.call("getTransactionsForAddress", [ci.creator, {
                "transactionDetails": "full", "sortOrder": "asc", "limit": 100, "encoding": "json",
                "maxSupportedTransactionVersion": 1, "commitment": "confirmed",
                "filters": {"blockTime": {"gte": since}, "status": "succeeded"}}])
        except Exception:
            return max(ci.launches, 1)  # unknown history: be conservative, never treat as first launch
        k = 0
        for tx in res.get("data") or []:
            ntx = from_rpc_json(tx)
            if ntx.slot < ci.fund_slot:
                continue
            for kind, e, _ in pump_events(ntx):
                if kind == "create" and e.get("user") == ci.creator:
                    k += 1
        return k

    # ================================================================ subscriptions
    def _proc_request(self):
        req = G.SubscribeRequest(commitment=G.CommitmentLevel.PROCESSED)
        req.slots["slots"].CopyFrom(G.SubscribeRequestFilterSlots(filter_by_commitment=True))
        req.blocks_meta["bm"].CopyFrom(G.SubscribeRequestFilterBlocksMeta())
        req.transactions["creates"].CopyFrom(G.SubscribeRequestFilterTransactions(
            vote=False, failed=False, account_include=[C.PUMP_MINT_AUTHORITY]))
        watched = self.book.watched_addresses()
        if watched:
            req.transactions["watch"].CopyFrom(G.SubscribeRequestFilterTransactions(
                vote=False, failed=False, account_include=watched))
        curves = list(self.tracks)
        if curves:
            req.transactions["curves"].CopyFrom(G.SubscribeRequestFilterTransactions(
                vote=False, failed=True, account_include=curves))
        req.transactions["me"].CopyFrom(G.SubscribeRequestFilterTransactions(
            vote=False, failed=True, account_include=[self.me]))
        self._filter_version = self.book.version
        return req

    def _deshred_request(self):
        req = G.SubscribeDeshredRequest()
        req.deshred_transactions["creates"].CopyFrom(G.SubscribeRequestFilterDeshredTransactions(
            vote=False, account_include=[C.PUMP_MINT_AUTHORITY]))
        return req

    def _refilter(self):
        if self.proc_sub and self.proc_sub.connected:
            self.proc_sub.update()

    # ================================================================ deshred: fastest trigger
    def _on_deshred(self, upd):
        if upd.WhichOneof("update_oneof") != "deshred_transaction":
            return
        t = time.time()
        d = upd.deshred_transaction
        info = d.transaction
        try:
            ntx = from_grpc_tx(info.signature, d.slot, info.transaction, None, None,
                               info.loaded_writable_addresses, info.loaded_readonly_addresses)
        except Exception:
            return
        self._maybe_trigger(ntx, t, "deshred")

    def _maybe_trigger(self, ntx: NormTx, t_seen: float, source: str):
        for name, ix in pump_instructions(ntx, include_inner=False):
            if name not in C.CREATE_IXS:
                continue
            c = decode_create_ix(ntx, ix)
            if not c or not c.get("mint"):
                continue
            mint, signer = c["mint"], c["user"]
            info = self.book.creators.get(signer)
            if mint in self.decided:
                continue
            if not info:
                return
            self.decided[mint] = "pending"
            # creators fire several tokens within a second: count sightings synchronously so the
            # 2nd create of a burst is never mistaken for a first launch
            launch_no = max(info.seen, info.launches)
            info.seen = launch_no + 1
            asyncio.get_running_loop().create_task(self._decide(ntx, c, info, launch_no, t_seen, source))
            return

    async def _decide(self, ntx: NormTx, c: dict, info, launch_no: int, t_seen: float, source: str):
        op = self.book.ops.get(info.operator)
        mint = c["mint"]
        pol = op.policy
        devs = dev_buys_preexec(ntx, mint)
        cfee = c.get("creator_fee_bps") or 0
        curve = Curve.fresh(cfee if cfee else C.CREATOR_FEE_BPS_DEFAULT)
        dev_sol = 0
        for d in devs:
            if d.get("token_amount") and d["token_amount"] < curve.vtr:
                cost = curve.sol_for_tokens(d["token_amount"])
                curve.apply_buy_sol(cost, d["token_amount"])
                dev_sol += cost
            elif d.get("sol_in"):
                tok = curve.tokens_for_sol(d["sol_in"])
                net = d["sol_in"] * 10_000 // (10_000 + curve.fee_bps)
                curve.apply_buy_sol(net, tok)
                dev_sol += net
        reason = None
        if not c["sol_quoted"]:
            reason = "non_sol_quote"
        elif c["mayhem"] and not op.allow_mayhem:
            reason = "mayhem"
        elif c.get("cashback"):
            reason = "cashback"
        elif cfee and cfee > op.max_creator_fee_bps:
            reason = f"creator_fee_{cfee}"
        elif pol.first_launch_only and launch_no > 0:
            reason = f"not_first_launch({launch_no})"
        elif not (op.dev_buy_min_sol * LAMPORTS <= dev_sol <= op.dev_buy_max_sol * LAMPORTS):
            reason = f"dev_buy_{dev_sol / LAMPORTS:.2f}"
        elif curve.rtr <= 0:
            reason = "instant_migration"
        mcap_est = curve.mcap_sol
        curve_creator = str(holder_rewards_pda(PK(mint))) if c.get("holder_reward") else c["creator"]
        keys = CurveKeys(PK(mint), PK(curve_creator), PK(c.get("token_program") or C.TOKEN_2022_PROGRAM),
                         mayhem=bool(c["mayhem"]))
        decision = "skip"
        if reason is None:
            if self.paper:
                decision, reason = "paper", "paper_mode"
            elif not pol.enabled:
                decision, reason = "shadow", "policy_disabled"
            else:
                r = self._risk_block(op.id, pol)
                if r:
                    decision, reason = "blocked", r
                else:
                    decision = "buy"
        self.decided[mint] = decision
        self.store.upsert_launch(mint, sig=ntx.signature, slot=ntx.slot, creator=c["user"], operator=op.id,
                                 t_deshred=t_seen if source == "deshred" else None, dev_buy_sol=dev_sol / LAMPORTS,
                                 mayhem=int(c["mayhem"]), creator_fee_bps=cfee, sol_quoted=int(c["sol_quoted"]),
                                 first_launch=int(launch_no == 0), name=c.get("name"), symbol=c.get("symbol"),
                                 uri=c.get("uri"), decision=decision, reason=reason or "", mcap_after_bundle=mcap_est)
        # every eligible launch (and repeat / off-signature launches, for learning) gets its tape recorded
        if decision != "skip" or (reason or "").startswith(("not_first_launch", "dev_buy_")):
            self._track(mint, op.id, c["user"], ntx.slot, keys, curve)
        log.info("launch %s op=%s creator=%s #%d dev=%.2f mcap=%.1f -> %s %s (%s +%.0fms)", mint[:8], op.id,
                 c["user"][:6], launch_no, dev_sol / LAMPORTS, mcap_est, decision, reason or "", source,
                 (time.time() - t_seen) * 1000)
        if decision == "buy":
            await self._buy(mint, op, keys, curve, ntx.slot, t_seen)
        elif decision in ("paper", "shadow") and self.cfg.paper_simulate:
            if source == "processed" or mint in self._executed:  # create already executed
                asyncio.get_running_loop().create_task(self._paper_simulate(mint, keys, curve, op))
            else:
                self._paper_sims[mint] = (keys, curve, op)

    # ================================================================ buying
    def _risk_block(self, op_id: str, pol) -> Optional[str]:
        cfg = self.cfg
        if os.path.exists(cfg.kill_file):
            return "kill_file"
        open_n = sum(1 for p in self.positions.values() if p.state in ("pending", "open", "selling"))
        if open_n >= cfg.max_open_positions:
            return "max_open"
        if sum(1 for p in self.positions.values()
               if p.operator == op_id and p.state in ("pending", "open", "selling")) >= pol.max_open:
            return "max_open_operator"
        if self.day_pnl <= -cfg.daily_loss_limit_sol * LAMPORTS:
            return "daily_loss_limit"
        if self.balance and self.balance < (cfg.min_balance_sol + pol.size_sol) * LAMPORTS:
            return "low_balance"
        if self.consec_losses.get(op_id, 0) >= cfg.max_consecutive_losses:
            return "consecutive_losses"
        if not self._blockhash(12):
            return "no_blockhash"
        return None

    def _blockhash(self, valid_blocks: int) -> Optional[Hash]:
        """A blockhash that expires ~valid_blocks from now (old hash => short validity).

        A buy that cannot land within the window simply expires instead of filling late."""
        if not self.blockhashes:
            return None
        cur_slot, cur_h, cur_bh = self.blockhashes[-1]
        if time.time() - self._bh_time > 10:
            return None  # stream stalled: do not sign with a stale view
        use_h = cur_h > 0
        cur = cur_h if use_h else cur_slot
        # slots run ahead of block heights (skipped slots), so the slot fallback is conservative
        target = cur - (150 - valid_blocks)
        for slot, h, bh in self.blockhashes:
            if (h if use_h else slot) >= target:
                return Hash.from_string(bh)
        return Hash.from_string(self.blockhashes[0][2])

    def _fees(self, side: str, attempt: int = 0) -> Fees:
        cfg = self.cfg
        if side == "buy":
            prio, tip, cu = cfg.buy_priority_lamports, cfg.buy_tip_lamports, cfg.buy_cu_limit
        else:
            mult = 1 + attempt
            prio, tip, cu = cfg.sell_priority_lamports * mult, cfg.sell_tip_lamports * mult, cfg.sell_cu_limit
        tip_acc = random.choice(cfg.tip_accounts) if cfg.tip_accounts else None
        return Fees(cu_limit=cu, cu_price_micro=prio * 1_000_000 // cu, tip_lamports=tip, tip_account=tip_acc)

    async def _buy(self, mint: str, op, keys: CurveKeys, curve: Curve, create_slot: int, t_seen: float):
        pol = op.policy
        size = int(min(pol.size_sol, self.cfg.max_size_sol) * LAMPORTS)
        max_mcap = curve.mcap_sol * pol.max_entry_mcap_mult
        min_tokens = min_tokens_for_max_mcap(size, max_mcap, curve.fee_bps)
        bh = self._blockhash(pol.max_slot_offset + 4)
        tx = build_buy_tx(self.kp, keys, size, min_tokens, bh, self._fees("buy"))
        sig = str(tx.signatures[0])
        t_build = time.time()
        res = await self.sender.send(bytes(tx), "buy")
        t_sent = time.time()
        self.lat.append((t_build - t_seen) * 1000)
        aid = self.store.attempt(mint=mint, operator=op.id, side="buy", t_trigger=t_seen, t_sent=t_sent, sig=sig,
                                 size_lamports=size, min_out=min_tokens, max_entry_mcap=max_mcap,
                                 routes=[(n, ok, round(l, 1)) for n, ok, l, _ in res], status="sent")
        pos = Position(mint, op.id, keys, create_slot, sig, aid, size, t_trigger=t_seen, tp_pct=pol.tp_pct,
                       max_hold_s=pol.max_hold_s, sl_pct=pol.sl_pct)
        self.positions[mint] = pos
        self.sig_to_mint[sig] = (mint, "buy")
        ok_routes = [n for n, ok, _, _ in res if ok]
        log.info("BUY sent %s %s size=%.3f guard_mcap=%.1f build=%.1fms routes_ok=%s errs=%s", mint[:8], sig[:10],
                 size / LAMPORTS, max_mcap, (t_build - t_seen) * 1000, ok_routes,
                 [t[:80] for n, ok, _, t in res if not ok])

    # ================================================================ processed stream
    def _on_processed(self, upd):
        kind = upd.WhichOneof("update_oneof")
        if kind == "slot":
            if upd.slot.slot > self.slot:
                self.slot = upd.slot.slot
            return
        if kind == "block_meta":
            bm = upd.block_meta
            self.blockhashes.append((bm.slot, bm.block_height.block_height, bm.blockhash))
            self._bh_time = time.time()
            return
        if kind != "transaction":
            return
        tu = upd.transaction
        info = tu.transaction
        try:
            ntx = from_grpc_tx(info.signature, tu.slot, info.transaction, info.meta, info.index)
        except Exception:
            log.exception("decode failed")
            return
        filters = set(upd.filters)
        if ntx.signature in self.sig_to_mint:
            self._on_our_tx(ntx)
        has_pump = any(ix.program == C.PUMP_PROGRAM for ix in ntx.outer) or any(
            ix.program == C.PUMP_PROGRAM for ix in ntx.inner)
        if "watch" in filters and not has_pump and ntx.err is None:
            n = 0
            for k in set(ntx.keys):
                op_id = self.book.funder_to_op.get(k)
                if op_id:
                    n += self._scan_funding(ntx, k, op_id)
            for k in ntx.signers:
                ci = self.book.creators.get(k)
                if ci and self.book.ops[ci.operator].follow_chain:
                    n += self._scan_funding(ntx, k, ci.operator, chain=True)
            if n:
                self._refilter()
        if not has_pump:
            return
        for ev_kind, e, _ in pump_events(ntx):
            if ev_kind == "create":
                self._on_create_executed(ntx, e)
            elif ev_kind == "trade":
                self._on_trade(ntx, e)
        if ntx.err is not None:
            # failed tx on a tracked curve (competition / slippage failures)
            for k in ntx.keys:
                tr = self.tracks.get(k)
                if tr:
                    self.store.add_tape(tr.mint, [(ntx.slot, ntx.tx_index or 0, 0, ntx.signature, ntx.fee_payer,
                                                   ntx.fee_payer, None, 0, 0, 0, 0, time.time(), "failed", 1)])

    def _scan_funding(self, ntx: NormTx, source: str, op_id: str, chain: bool = False, bootstrap: bool = False) -> int:
        op = self.book.ops[op_id]
        lo, hi = op.funder_ranges.get(source, (op.fund_min_sol, op.fund_max_sol)) if not chain else \
            (op.fund_min_sol, op.fund_max_sol)
        n = 0
        for to, lam in balance_recipients(ntx, source, int(lo * LAMPORTS)):
            if to in self.book.funder_to_op or to == self.me:
                continue
            i = ntx.keys.index(to)
            fresh = i < len(ntx.pre_balances) and ntx.pre_balances[i] == 0
            if lam > hi * LAMPORTS:
                if (not chain and op.follow_child_funders_min_sol and lam >= op.follow_child_funders_min_sol * LAMPORTS
                        and fresh):
                    if self.book.add_funder(to, op_id, "child"):
                        self.store.x("INSERT OR IGNORE INTO funders VALUES(?,?,?,?,?,?)",
                                     (to, op_id, "child", time.time(), source, f"{lam / LAMPORTS:.3f}"))
                        log.info("child funder %s <- %s (%.2f SOL) op=%s", to[:8], source[:8], lam / LAMPORTS, op_id)
                        n += 1
                continue
            if op.fresh_only and not fresh:
                continue
            known = to in self.book.creators
            if known and not fresh:
                continue  # a top-up, not a recycled (drained to 0) wallet being funded again
            t_fund = ntx.block_time or time.time()
            if self.book.add_creator(to, op_id, source, lam, ntx.slot, t_fund):
                self.store.add_funding(to, source, op_id, lam, ntx.slot, fresh, ntx.signature, t_fund,
                                       op.creator_ttl_h * 3600)
                if not bootstrap:
                    log.info("creator %s %s by %s %.3f SOL (%s) op=%s", to[:8], "re-funded" if known else "funded",
                             source[:8], lam / LAMPORTS, "chain" if chain else "funder", op_id)
                n += 1
        return n

    def _on_create_executed(self, ntx: NormTx, e: dict):
        mint = e.get("mint")
        creator = e.get("user")
        info = self.book.creators.get(creator)
        self.store.upsert_launch(mint, sig=ntx.signature, slot=ntx.slot, tx_index=ntx.tx_index, creator=creator,
                                 t_processed=time.time(), block_time=e.get("timestamp"),
                                 sol_quoted=int(e.get("quote_mint") in (C.DEFAULT_PUBKEY, C.WSOL_MINT)),
                                 operator=info.operator if info else None)
        if not info:
            return
        if mint not in self.decided:
            # deshred missed it (e.g. CPI create): decide from the executed tx (later entry)
            self._maybe_trigger(ntx, time.time(), "processed")
            if mint not in self.decided:  # create via CPI: track tape for learning only
                self.decided[mint] = "untracked_cpi"
        info.launches += 1
        info.seen = max(info.seen, info.launches)
        self.store.bump_launches(creator)
        self._executed.add(mint)
        ps = self._paper_sims.pop(mint, None)
        if ps:
            asyncio.get_running_loop().create_task(self._paper_simulate(mint, *ps))

    def _track(self, mint: str, op_id: str, creator: str, create_slot: int, keys: CurveKeys, curve: Curve):
        bc = str(bonding_curve_pda(PK(mint)))
        if bc in self.tracks:
            return
        tr = Track(mint, bc, op_id, creator, create_slot, time.time() + self.cfg.tape_minutes * 60, keys,
                   curve.copy(), {creator})
        self.tracks[bc] = tr
        self.tracks_by_mint[mint] = tr
        self._refilter()

    def _on_trade(self, ntx: NormTx, e: dict):
        mint = e.get("mint")
        tr = self.tracks_by_mint.get(mint)
        if tr is None:
            return
        tr.n_trades += 1
        tr.curve.vsr, tr.curve.vtr = e["vsr"], e["vtr"]
        tr.curve.rtr = e.get("rtr", tr.curve.rtr)
        self.store.add_tape(mint, [(ntx.slot, ntx.tx_index or 0, tr.n_trades, ntx.signature, e["user"], ntx.fee_payer,
                                    int(e["is_buy"]), e["sol_amount"], e["token_amount"], e["vsr"], e["vtr"],
                                    time.time(), e.get("ix_name", ""), 0)])
        pos = self.positions.get(mint)
        if not pos:
            return
        if e["user"] == self.me:
            return  # handled in _on_our_tx
        if pos.state == "open":
            if not e["is_buy"] and e["user"] in tr.dev_wallets and e["sol_amount"] > 0.2 * LAMPORTS:
                self._sell(pos, "dev_dump")
            else:
                self._check_exit(pos, tr)

    # ================================================================ our transactions
    def _on_our_tx(self, ntx: NormTx):
        mint, side = self.sig_to_mint[ntx.signature]
        pos = self.positions.get(mint)
        if not pos:
            return
        delta = ntx.sol_delta(self.me) or 0
        if side == "buy":
            if ntx.err is not None:
                pos.state = "failed"
                self.store.update_attempt(pos.attempt_id, status="failed", landed_slot=ntx.slot,
                                          landed_idx=ntx.tx_index, slot_offset=ntx.slot - pos.create_slot,
                                          sol_delta=delta, err=str(ntx.err)[:200])
                log.info("BUY failed %s slot+%d err=%s fee=%d", mint[:8], ntx.slot - pos.create_slot,
                         str(ntx.err)[:60], -delta)
                self.day_pnl += delta
                return
            tok = 0
            for k, e, _ in pump_events(ntx):
                if k == "trade" and e.get("user") == self.me and e.get("mint") == mint:
                    tok += e["token_amount"]
            pos.tokens, pos.cost = tok, -delta
            pos.state, pos.opened, pos.entry_slot = "open", time.time(), ntx.slot
            fill_mcap = (pos.size / tok * 1e6) if tok else 0
            self.store.update_attempt(pos.attempt_id, status="filled", landed_slot=ntx.slot, landed_idx=ntx.tx_index,
                                      slot_offset=ntx.slot - pos.create_slot, sol_delta=delta, tokens=tok,
                                      fill_mcap=fill_mcap)
            self.store.x("INSERT OR REPLACE INTO positions(mint,operator,opened,entry_slot,tokens,cost,status) "
                         "VALUES(?,?,?,?,?,?,?)", (mint, pos.operator, pos.opened, ntx.slot, tok, pos.cost, "open"))
            log.info("BUY filled %s slot+%d idx=%s tokens=%d cost=%.4f SOL fill_mcap=%.1f", mint[:8],
                     ntx.slot - pos.create_slot, ntx.tx_index, tok, pos.cost / LAMPORTS, fill_mcap)
        else:
            if ntx.err is not None:
                self.day_pnl += delta  # fee burned
                log.info("SELL failed %s err=%s", mint[:8], str(ntx.err)[:80])
                pos.cost += -delta
                return
            pos.proceeds += delta
            pos.state = "closed"
            pnl = pos.proceeds - pos.cost
            self.day_pnl += pnl
            self.consec_losses[pos.operator] = 0 if pnl > 0 else self.consec_losses.get(pos.operator, 0) + 1
            self.store.x("UPDATE positions SET status='closed', closed=?, proceeds=?, pnl=?, exit_reason=? WHERE mint=?",
                         (time.time(), pos.proceeds, pnl, pos.exit_reason, mint))
            log.info("SELL filled %s reason=%s pnl=%+.4f SOL (%.1f%%) hold=%.1fs", mint[:8], pos.exit_reason,
                     pnl / LAMPORTS, 100 * pnl / max(pos.cost, 1), time.time() - pos.opened)
            if self.consec_losses.get(pos.operator, 0) >= self.cfg.max_consecutive_losses:
                op = self.book.ops.get(pos.operator)
                if op:
                    op.policy.enabled = False
                    self.book.save()
                    log.warning("operator %s disabled after %d consecutive losses", pos.operator,
                                self.consec_losses[pos.operator])

    # ================================================================ exits
    def _check_exit(self, pos: Position, tr: Track):
        if pos.state != "open" or not pos.tokens:
            return
        value = (tr.curve.sell_quote(pos.tokens) + ATA_RENT.get(str(pos.keys.token_program), 2_039_280)
                 - self.cfg.sell_priority_lamports
                 - self.cfg.sell_tip_lamports)
        ret = value / max(pos.cost, 1) - 1
        if ret >= pos.tp_pct / 100:
            self._sell(pos, "tp")
        elif pos.sl_pct is not None and ret <= -pos.sl_pct / 100:
            self._sell(pos, "sl")
        elif time.time() - pos.opened >= pos.max_hold_s:
            self._sell(pos, "time")

    def _sell(self, pos: Position, reason: str):
        now = time.time()
        if pos.state == "selling" and now - pos.last_sell_t < self.cfg.sell_retry_s:
            return
        if pos.state not in ("open", "selling"):
            return
        tr = self.tracks_by_mint.get(pos.mint)
        expected = tr.curve.sell_quote(pos.tokens) if tr else 0
        slip = [0.2, 0.4, 0.7, 0.95][min(pos.sell_attempts, 3)]
        min_out = int(expected * (1 - slip))
        bh = self._blockhash(60)
        if bh is None:
            return
        tx = build_sell_tx(self.kp, pos.keys, pos.tokens, min_out, bh, self._fees("sell", min(pos.sell_attempts, 3)))
        sig = str(tx.signatures[0])
        pos.state, pos.last_sell_t = "selling", now
        if not pos.exit_reason:
            pos.exit_reason = reason
        pos.sell_attempts += 1
        pos.sell_sigs.append(sig)
        self.sig_to_mint[sig] = (pos.mint, "sell")
        aid = self.store.attempt(mint=pos.mint, operator=pos.operator, side="sell", t_trigger=now, sig=sig,
                                 min_out=min_out, status="sent", extra={"reason": reason, "try": pos.sell_attempts})
        asyncio.get_running_loop().create_task(self._send_and_log(tx, aid, "SELL", pos.mint, reason))

    async def _paper_simulate(self, mint: str, keys: CurveKeys, curve: Curve, op):
        """Build the exact buy we would send (+ a sell) and simulate it on live state. Nothing is sent."""
        import base64
        from ..txbuild import build_roundtrip_sim_tx
        pol = op.policy
        size = int(min(pol.size_sol, self.cfg.max_size_sol) * LAMPORTS)
        min_tokens = min_tokens_for_max_mcap(size, curve.mcap_sol * pol.max_entry_mcap_mult, curve.fee_bps)
        bh = self._blockhash(140)
        if bh is None:
            return
        tx = build_roundtrip_sim_tx(PK(self.cfg.sim_payer), keys, size, min_tokens, bh)
        t0 = time.time()
        status, err, logs, v = "sim_exc", None, [], {}
        # the RPC node's bank may not have executed the create yet: wait, retry once
        for delay in (1.0, 2.0):
            await asyncio.sleep(delay)
            try:
                r = await self.rpc.call("simulateTransaction", [base64.b64encode(bytes(tx)).decode(), {
                    "encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": True,
                    "commitment": "processed"}])
                v = r["value"]
                err = v.get("err")
                status = "sim_ok" if err is None else "sim_err"
                logs = (v.get("logs") or [])[-3:]
            except Exception as e:
                status, err, logs, v = "sim_exc", repr(e)[:200], [], {}
            if status == "sim_ok" or "IncorrectProgramId" not in str(err):
                break
        self.store.attempt(mint=mint, operator=op.id, side="paper_sim", t_trigger=t0, status=status,
                           size_lamports=size, min_out=min_tokens, err=str(err)[:300] if err else None,
                           extra={"cu": v.get("unitsConsumed"), "logs": logs, "ms": round((time.time() - t0) * 1000)})
        log.info("paper sim %s %s cu=%s err=%s", mint[:8], status, v.get("unitsConsumed"), str(err)[:120] if err else "")

    async def _send_and_log(self, tx, aid: int, side: str, mint: str, reason: str):
        res = await self.sender.send(bytes(tx), "sell")
        self.store.update_attempt(aid, t_sent=time.time(), routes=[(n, ok, round(l, 1)) for n, ok, l, _ in res])
        log.info("%s sent %s %s routes_ok=%s", side, mint[:8], reason, [n for n, ok, _, _ in res if ok])

    # ================================================================ timers
    async def _timer(self):
        while True:
            await asyncio.sleep(0.4)
            now = time.time()
            for pos in list(self.positions.values()):
                if pos.state == "open":
                    tr = self.tracks_by_mint.get(pos.mint)
                    if tr:
                        self._check_exit(pos, tr)
                elif pos.state == "selling" and now - pos.last_sell_t >= self.cfg.sell_retry_s:
                    if pos.sell_attempts >= 4 and not pos.reconciling:
                        pos.reconciling = True
                        asyncio.get_running_loop().create_task(self._reconcile(pos, "sell"))
                    self._sell(pos, pos.exit_reason)
                elif (pos.state == "pending" and self.slot - pos.create_slot > self.cfg.buy_expire_slots + 20
                      and not pos.reconciling):
                    pos.reconciling = True
                    asyncio.get_running_loop().create_task(self._reconcile(pos, "buy"))

    async def _reconcile(self, pos: Position, side: str):
        """The stream may miss our own transaction: ask the chain what happened."""
        from ..decode import from_rpc_json
        try:
            sigs = [pos.buy_sig] if side == "buy" else pos.sell_sigs[-20:]
            st = await self.rpc.call("getSignatureStatuses", [sigs, {"searchTransactionHistory": True}])
            for sig, s in zip(sigs, st["value"]):
                if s and s.get("confirmationStatus"):
                    tx = await self.rpc.get_tx(sig, commitment="confirmed")
                    if tx:
                        ntx = from_rpc_json(tx)
                        if ntx.err is None or side == "buy":
                            log.warning("reconcile %s %s: found landed tx %s", side, pos.mint[:8], sig[:10])
                            self._on_our_tx(ntx)
                            if ntx.err is None:
                                return
            if side == "buy":
                pos.state = "expired"
                self.store.update_attempt(pos.attempt_id, status="expired")
                log.info("BUY expired %s (not landed)", pos.mint[:8])
            else:
                from ..txbuild import ata
                acc = ata(self.kp.pubkey(), pos.keys.mint, pos.keys.token_program)
                r = await self.rpc.call("getTokenAccountBalance", [str(acc), {"commitment": "confirmed"}])
                amt = int(r["value"]["amount"])
                if amt == 0:
                    pos.state = "closed"
                    self.store.x("UPDATE positions SET status='closed_unreconciled', closed=? WHERE mint=?",
                                 (time.time(), pos.mint))
                    log.warning("reconcile %s: tokens gone but sell tx not found - marked closed", pos.mint[:8])
                elif amt != pos.tokens:
                    log.warning("reconcile %s: token balance %d != tracked %d - resyncing", pos.mint[:8], amt,
                                pos.tokens)
                    pos.tokens = amt
        except Exception as e:
            if "could not find account" in str(e).lower() and side == "sell":
                pos.state = "closed"
                self.store.x("UPDATE positions SET status='closed_unreconciled', closed=? WHERE mint=?",
                             (time.time(), pos.mint))
            else:
                log.warning("reconcile %s %s failed: %s", side, pos.mint[:8], str(e)[:120])
        finally:
            pos.reconciling = False

    async def _housekeeping(self):
        while True:
            await asyncio.sleep(5)
            self.store.commit()
            now = time.time()
            # stop tracking old curves unless we still hold a position
            dead = [bc for bc, tr in self.tracks.items() if tr.until < now and
                    not (tr.mint in self.positions and self.positions[tr.mint].state in ("open", "selling", "pending"))]
            for bc in dead:
                tr = self.tracks.pop(bc)
                self.tracks_by_mint.pop(tr.mint, None)
            self.book.expire()
            if dead or self.book.version != self._filter_version:
                self._refilter()
            day = time.strftime("%Y-%m-%d", time.gmtime())
            if day != self.day:
                self.day, self.day_pnl = day, 0
            if int(now) % 60 < 5:
                await self._refresh_balance()

    async def _learner_loop(self):
        from .learner import run_learner
        while True:
            await asyncio.sleep(self.cfg.learner_every_s)
            try:
                self.store.commit()
                rep = await run_learner(self.store, self.book, self.rpc, tape_minutes=self.cfg.tape_minutes)
                for op_id, st in rep["operators"].items():
                    log.info("learner %s: eligible=%d sim(slot+3)=%s realized=%s action=%s", op_id, st["eligible"],
                             st["sim_slot3"], st["realized"], st["action"])
            except Exception:
                log.exception("learner failed")

    async def _refresh_balance(self):
        try:
            r = await self.rpc.call("getBalance", [self.me, {"commitment": "processed"}])
            self.balance = r["value"]
        except Exception:
            pass

    async def _status_loop(self):
        while True:
            await asyncio.sleep(10)
            lat = sorted(self.lat)
            st = {
                "t": time.time(), "uptime_s": round(time.time() - self.started), "mode": self.cfg.mode,
                "slot": self.slot, "balance_sol": self.balance / LAMPORTS, "day_pnl_sol": self.day_pnl / LAMPORTS,
                "streams": {s.name: {"connected": s.connected, "msgs": s.n_msgs, "reconnects": s.reconnects,
                                     "last_msg_age_s": round(time.time() - s.last_msg, 1) if s.last_msg else None}
                            for s in (self.proc_sub, self.deshred_sub) if s},
                "operators": {o.id: {"enabled": o.policy.enabled, "funders": sum(1 for f, i in self.book.funder_to_op.items() if i == o.id),
                                     "creators": sum(1 for c in self.book.creators.values() if c.operator == o.id)}
                              for o in self.book.ops.values()},
                "tracked_curves": len(self.tracks),
                "positions": {m[:8]: {"state": p.state, "cost": p.cost / LAMPORTS, "tokens": p.tokens}
                              for m, p in self.positions.items() if p.state in ("pending", "open", "selling")},
                "trigger_to_send_ms_p50": round(lat[len(lat) // 2], 1) if lat else None,
                "routes": self.sender.summary(),
            }
            os.makedirs(os.path.dirname(self.cfg.status_path) or ".", exist_ok=True)
            with open(self.cfg.status_path, "w") as f:
                json.dump(st, f, indent=1)

"""Normalized transaction model + pump.fun event / instruction decoding.

One `NormTx` is built either from RPC `getTransaction` JSON (backfill) or from a
Yellowstone gRPC update (live). All analytics and the live trigger work on
`NormTx`, so the backfill and the live path decode launches identically.
"""
from __future__ import annotations

import base64
import struct
from dataclasses import dataclass, field
from typing import Any, Optional

import base58

from .constants import (
    ATA_PROGRAM,
    BASE_FEE_PER_SIGNATURE,
    BUY_IXS,
    COMPUTE_BUDGET_PROGRAM,
    CREATE_IXS,
    EVENT_DISC,
    EVENT_IX_TAG,
    IX_DISC,
    PUMP_PROGRAM,
    SELL_IXS,
    SYSTEM_PROGRAM,
)


def b58(b: bytes) -> str:
    return base58.b58encode(b).decode()


@dataclass(slots=True)
class Ix:
    program: str
    accounts: list[int]
    data: bytes
    outer_index: int  # index of the top-level instruction this belongs to
    inner: bool = False
    stack_height: Optional[int] = None


@dataclass(slots=True)
class NormTx:
    signature: str
    slot: int
    keys: list[str]
    outer: list[Ix]
    inner: list[Ix]
    num_signers: int
    tx_index: Optional[int] = None  # position inside the block
    block_time: Optional[int] = None
    err: Any = None
    fee: Optional[int] = None
    pre_balances: list[int] = field(default_factory=list)
    post_balances: list[int] = field(default_factory=list)
    pre_token: list[dict] = field(default_factory=list)
    post_token: list[dict] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)
    cu_consumed: Optional[int] = None
    # v1 transaction config (SIMD-0385) or ComputeBudget-derived values
    cu_limit: Optional[int] = None
    cu_price_micro: Optional[int] = None  # legacy/v0 microlamports per CU
    v1_priority_fee: Optional[int] = None  # v1 total priority lamports
    executed: bool = True  # False for deshred (pre-execution) updates

    @property
    def fee_payer(self) -> str:
        return self.keys[0]

    @property
    def signers(self) -> list[str]:
        return self.keys[: self.num_signers]

    def priority_fee_lamports(self) -> Optional[int]:
        if self.fee is not None:
            return max(0, self.fee - BASE_FEE_PER_SIGNATURE * self.num_signers)
        if self.v1_priority_fee is not None:
            return self.v1_priority_fee
        if self.cu_price_micro is not None:
            limit = self.cu_limit or 200_000
            return self.cu_price_micro * limit // 1_000_000
        return None

    def sol_delta(self, account: str) -> Optional[int]:
        try:
            i = self.keys.index(account)
        except ValueError:
            return None
        if i >= len(self.pre_balances) or i >= len(self.post_balances):
            return None
        return self.post_balances[i] - self.pre_balances[i]


# --------------------------------------------------------------------------- builders


def _compute_budget(ntx: NormTx) -> None:
    for ix in ntx.outer:
        if ix.program != COMPUTE_BUDGET_PROGRAM or not ix.data:
            continue
        tag = ix.data[0]
        if tag == 3 and len(ix.data) >= 9:
            ntx.cu_price_micro = struct.unpack_from("<Q", ix.data, 1)[0]
        elif tag == 2 and len(ix.data) >= 5:
            ntx.cu_limit = struct.unpack_from("<I", ix.data, 1)[0]


def from_rpc_json(tx: dict) -> NormTx:
    """Build from getTransaction(encoding=json, maxSupportedTransactionVersion=1)."""
    msg = tx["transaction"]["message"]
    meta = tx.get("meta") or {}
    la = meta.get("loadedAddresses") or {}
    keys = list(msg["accountKeys"]) + list(la.get("writable", [])) + list(la.get("readonly", []))
    outer = []
    for i, ix in enumerate(msg["instructions"]):
        outer.append(Ix(keys[ix["programIdIndex"]], list(ix["accounts"]), base58.b58decode(ix["data"]), i))
    inner = []
    for grp in meta.get("innerInstructions") or []:
        for ix in grp["instructions"]:
            inner.append(
                Ix(keys[ix["programIdIndex"]], list(ix["accounts"]), base58.b58decode(ix["data"]), grp["index"], True,
                   ix.get("stackHeight"))
            )
    ntx = NormTx(
        signature=tx["transaction"]["signatures"][0],
        slot=tx["slot"],
        keys=keys,
        outer=outer,
        inner=inner,
        num_signers=msg["header"]["numRequiredSignatures"],
        tx_index=tx.get("transactionIndex"),
        block_time=tx.get("blockTime"),
        err=meta.get("err"),
        fee=meta.get("fee"),
        pre_balances=list(meta.get("preBalances") or []),
        post_balances=list(meta.get("postBalances") or []),
        pre_token=list(meta.get("preTokenBalances") or []),
        post_token=list(meta.get("postTokenBalances") or []),
        logs=list(meta.get("logMessages") or []),
        cu_consumed=meta.get("computeUnitsConsumed"),
    )
    cfg = msg.get("transactionConfig")
    if cfg:
        ntx.cu_limit = cfg.get("computeUnitLimit")
        ntx.v1_priority_fee = cfg.get("priorityFee")
    _compute_budget(ntx)
    return ntx


def _token_balances_pb(tbs) -> list[dict]:
    out = []
    for t in tbs:
        out.append({
            "accountIndex": t.account_index,
            "mint": t.mint,
            "owner": t.owner,
            "programId": t.program_id,
            "uiTokenAmount": {"amount": t.ui_token_amount.amount, "decimals": t.ui_token_amount.decimals},
        })
    return out


def from_grpc_tx(sig: bytes, slot: int, tx_pb, meta_pb=None, index: Optional[int] = None,
                 loaded_writable=None, loaded_readonly=None) -> NormTx:
    """Build from a yellowstone `Transaction` (+ optional `TransactionStatusMeta`).

    For deshred (pre-execution) updates pass `meta_pb=None` and the loaded
    addresses from the deshred info.
    """
    m = tx_pb.message
    keys = [b58(k) for k in m.account_keys]
    if meta_pb is not None:
        keys += [b58(k) for k in meta_pb.loaded_writable_addresses]
        keys += [b58(k) for k in meta_pb.loaded_readonly_addresses]
    else:
        keys += [b58(k) for k in (loaded_writable or [])]
        keys += [b58(k) for k in (loaded_readonly or [])]
    nkeys = len(keys)
    outer = []
    for i, ix in enumerate(m.instructions):
        prog = keys[ix.program_id_index] if ix.program_id_index < nkeys else "?"
        outer.append(Ix(prog, list(ix.accounts), bytes(ix.data), i))
    ntx = NormTx(
        signature=b58(sig),
        slot=slot,
        keys=keys,
        outer=outer,
        inner=[],
        num_signers=m.header.num_required_signatures,
        tx_index=index,
        executed=meta_pb is not None,
    )
    if m.HasField("config"):
        c = m.config
        if c.HasField("compute_unit_limit"):
            ntx.cu_limit = c.compute_unit_limit
        if c.HasField("priority_fee"):
            ntx.v1_priority_fee = c.priority_fee
    _compute_budget(ntx)
    if meta_pb is not None:
        ntx.err = bytes(meta_pb.err.err) if meta_pb.HasField("err") and meta_pb.err.err else None
        ntx.fee = meta_pb.fee
        ntx.pre_balances = list(meta_pb.pre_balances)
        ntx.post_balances = list(meta_pb.post_balances)
        for grp in meta_pb.inner_instructions:
            for ix in grp.instructions:
                prog = keys[ix.program_id_index] if ix.program_id_index < nkeys else "?"
                ntx.inner.append(Ix(prog, list(ix.accounts), bytes(ix.data), grp.index, True,
                                    ix.stack_height if ix.HasField("stack_height") else None))
        ntx.pre_token = _token_balances_pb(meta_pb.pre_token_balances)
        ntx.post_token = _token_balances_pb(meta_pb.post_token_balances)
        if not meta_pb.log_messages_none:
            ntx.logs = list(meta_pb.log_messages)
        if meta_pb.HasField("compute_units_consumed"):
            ntx.cu_consumed = meta_pb.compute_units_consumed
    return ntx


# --------------------------------------------------------------------------- borsh reader


class Rd:
    __slots__ = ("b", "o")

    def __init__(self, b: bytes, o: int = 0):
        self.b = b
        self.o = o

    def u8(self):
        v = self.b[self.o]
        self.o += 1
        return v

    def u16(self):
        v = struct.unpack_from("<H", self.b, self.o)[0]
        self.o += 2
        return v

    def u32(self):
        v = struct.unpack_from("<I", self.b, self.o)[0]
        self.o += 4
        return v

    def u64(self):
        v = struct.unpack_from("<Q", self.b, self.o)[0]
        self.o += 8
        return v

    def i64(self):
        v = struct.unpack_from("<q", self.b, self.o)[0]
        self.o += 8
        return v

    def pk(self):
        if self.o + 32 > len(self.b):
            raise IndexError
        v = b58(self.b[self.o:self.o + 32])
        self.o += 32
        return v

    def bl(self):
        return self.u8() != 0

    def string(self):
        n = self.u32()
        if self.o + n > len(self.b):
            raise IndexError
        v = self.b[self.o:self.o + n].decode("utf-8", "replace")
        self.o += n
        return v


# --------------------------------------------------------------------------- events

_TRADE_FIELDS = [
    ("mint", "pk"), ("sol_amount", "u64"), ("token_amount", "u64"), ("is_buy", "bl"), ("user", "pk"),
    ("timestamp", "i64"), ("vsr", "u64"), ("vtr", "u64"), ("rsr", "u64"), ("rtr", "u64"),
    ("fee_recipient", "pk"), ("fee_bps", "u64"), ("fee", "u64"), ("creator", "pk"),
    ("creator_fee_bps", "u64"), ("creator_fee", "u64"), ("track_volume", "bl"),
    ("total_unclaimed_tokens", "u64"), ("total_claimed_tokens", "u64"), ("current_sol_volume", "u64"),
    ("last_update_ts", "i64"), ("ix_name", "string"), ("mayhem", "bl"), ("cashback_fee_bps", "u64"),
    ("cashback", "u64"), ("buyback_fee_bps", "u64"), ("buyback_fee", "u64"), ("shareholders", "shareholders"),
    ("quote_mint", "pk"), ("quote_amount", "u64"), ("vqr", "u64"), ("rqr", "u64"),
    ("holder_rewards_bps", "u64"), ("holder_rewards", "u64"), ("creator_fee_unclaimed", "u64"),
    ("quote_transfer_fee", "u64"),
]
_CREATE_FIELDS = [
    ("name", "string"), ("symbol", "string"), ("uri", "string"), ("mint", "pk"), ("bonding_curve", "pk"),
    ("user", "pk"), ("creator", "pk"), ("timestamp", "i64"), ("vtr", "u64"), ("vsr", "u64"), ("rtr", "u64"),
    ("supply", "u64"), ("token_program", "pk"), ("mayhem", "bl"), ("cashback", "bl"), ("quote_mint", "pk"),
    ("vqr", "u64"), ("creator_fee_bps", "u64"), ("holder_reward", "bl"), ("depth", "u8"),
]


def _decode_fields(data: bytes, fields) -> dict:
    r = Rd(data, 8)
    e: dict = {}
    try:
        for name, kind in fields:
            if kind == "shareholders":
                n = r.u32()
                e[name] = [(r.pk(), r.u16()) for _ in range(n)]
            else:
                e[name] = getattr(r, kind)()
    except (struct.error, IndexError):
        e["_truncated"] = True
    return e


def decode_trade_event(data: bytes) -> dict:
    return _decode_fields(data, _TRADE_FIELDS)


def decode_create_event(data: bytes) -> dict:
    return _decode_fields(data, _CREATE_FIELDS)


def pump_events(ntx: NormTx) -> list[tuple[str, dict, int]]:
    """(kind, event, outer_ix_index) in execution order. CPI events first, logs as fallback."""
    out = []
    for ix in ntx.inner:
        if ix.program != PUMP_PROGRAM or not ix.data.startswith(EVENT_IX_TAG):
            continue
        d = ix.data[8:]
        if d.startswith(EVENT_DISC["trade"]):
            out.append(("trade", decode_trade_event(d), ix.outer_index))
        elif d.startswith(EVENT_DISC["create"]):
            out.append(("create", decode_create_event(d), ix.outer_index))
        elif d.startswith(EVENT_DISC["complete"]):
            out.append(("complete", {}, ix.outer_index))
    if out:
        return out
    for line in ntx.logs:
        if not line.startswith("Program data: "):
            continue
        try:
            d = base64.b64decode(line[14:])
        except Exception:
            continue
        if d.startswith(EVENT_DISC["trade"]):
            out.append(("trade", decode_trade_event(d), -1))
        elif d.startswith(EVENT_DISC["create"]):
            out.append(("create", decode_create_event(d), -1))
    return out


# --------------------------------------------------------------------------- instructions


def pump_ix_name(data: bytes) -> str:
    return IX_DISC.get(data[:8], "unknown")


def pump_instructions(ntx: NormTx, include_inner: bool = True) -> list[tuple[str, Ix]]:
    """All pump.fun program instructions (outer, and inner = invoked through launchers/bundlers)."""
    res = []
    for ix in ntx.outer:
        if ix.program == PUMP_PROGRAM:
            res.append((pump_ix_name(ix.data), ix))
    if include_inner:
        for ix in ntx.inner:
            if ix.program == PUMP_PROGRAM and not ix.data.startswith(EVENT_IX_TAG):
                res.append((pump_ix_name(ix.data), ix))
    return res


def acct(ntx: NormTx, ix: Ix, i: int) -> Optional[str]:
    if i >= len(ix.accounts):
        return None
    k = ix.accounts[i]
    return ntx.keys[k] if k < len(ntx.keys) else None


def decode_create_ix(ntx: NormTx, ix: Ix) -> Optional[dict]:
    """Decode create/create_v2 instruction (works pre-execution, from the deshred stream)."""
    name = pump_ix_name(ix.data)
    if name not in CREATE_IXS:
        return None
    r = Rd(ix.data, 8)
    try:
        d = {"ix": name, "name": r.string(), "symbol": r.string(), "uri": r.string(), "creator": r.pk(),
             "mayhem": False, "cashback": False, "creator_fee_bps": 0, "holder_reward": False}
        if name == "create_v2":
            d["mayhem"] = r.bl()
            # OptionBool / OptionU64 are single-field structs (fixed size) in this IDL
            if r.o < len(r.b):
                d["cashback"] = r.bl()
            if r.o + 8 <= len(r.b):
                d["creator_fee_bps"] = r.u64()
            if r.o < len(r.b):
                d["holder_reward"] = r.bl()
    except (struct.error, IndexError):
        return None
    d["n_accounts"] = len(ix.accounts)
    # create_v2 remaining accounts start with the quote mint (WSOL counts as SOL)
    d["quote_mint"] = acct(ntx, ix, 16) if name == "create_v2" and len(ix.accounts) > 16 else None
    d["sol_quoted"] = d["quote_mint"] in (None, "So11111111111111111111111111111111111111112",
                                          "11111111111111111111111111111111")
    if name == "create_v2":
        # 0 mint, 1 mint_authority, 2 bonding_curve, 3 associated_bonding_curve, 4 global, 5 user
        d.update(mint=acct(ntx, ix, 0), bonding_curve=acct(ntx, ix, 2), assoc_bonding_curve=acct(ntx, ix, 3),
                 user=acct(ntx, ix, 5), token_program=acct(ntx, ix, 7))
    else:
        # legacy create (SPL Token mint): 0 mint, 1 mint_authority, 2 bonding_curve, 3 associated_bonding_curve,
        # 4 global, 5 mpl_token_metadata, 6 metadata, 7 user, 8 system, 9 token_program
        d.update(mint=acct(ntx, ix, 0), bonding_curve=acct(ntx, ix, 2), assoc_bonding_curve=acct(ntx, ix, 3),
                 user=acct(ntx, ix, 7), token_program=acct(ntx, ix, 9) or "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
    return d


def decode_trade_ix(ntx: NormTx, ix: Ix) -> Optional[dict]:
    """Decode buy/sell instruction args + key accounts (pre-execution safe)."""
    name = pump_ix_name(ix.data)
    if name not in BUY_IXS and name not in SELL_IXS:
        return None
    r = Rd(ix.data, 8)
    try:
        a1, a2 = r.u64(), r.u64()
    except struct.error:
        return None
    d = {"ix": name, "is_buy": name in BUY_IXS}
    if name in ("buy", "buy_v2", "buy_v3"):
        d.update(token_amount=a1, max_sol=a2)
    elif name in ("buy_exact_sol_in", "buy_exact_quote_in_v2", "buy_exact_quote_in_v3"):
        d.update(sol_in=a1, min_tokens=a2)
    else:
        d.update(token_amount=a1, min_sol=a2)
    if name in ("buy", "buy_exact_sol_in", "sell"):
        d.update(mint=acct(ntx, ix, 2), bonding_curve=acct(ntx, ix, 3), user=acct(ntx, ix, 6))
    elif name.endswith("_v3"):
        d.update(mint=acct(ntx, ix, 1), quote_mint=acct(ntx, ix, 2), bonding_curve=acct(ntx, ix, 5),
                 user=acct(ntx, ix, 8))
    else:  # *_v2: 1 base_mint, 2 quote_mint, 10 bonding_curve, 13 user
        d.update(mint=acct(ntx, ix, 1), quote_mint=acct(ntx, ix, 2), bonding_curve=acct(ntx, ix, 10),
                 user=acct(ntx, ix, 13))
    return d


def dev_buys_preexec(ntx: NormTx, mint: str) -> list[dict]:
    """Buy instructions on `mint` inside a (not yet executed) transaction, in order."""
    out = []
    for name, ix in pump_instructions(ntx, include_inner=False):
        if name not in BUY_IXS:
            continue
        d = decode_trade_ix(ntx, ix)
        if d and d.get("mint") == mint:
            out.append(d)
    return out


def system_transfers(ntx: NormTx, include_inner: bool = True) -> list[tuple[str, str, int, bool]]:
    """(from, to, lamports, is_inner) for System Program transfers."""
    res = []
    ixs = ntx.outer + (ntx.inner if include_inner else [])
    for ix in ixs:
        if ix.program != SYSTEM_PROGRAM or len(ix.data) < 12:
            continue
        tag = struct.unpack_from("<I", ix.data, 0)[0]
        if tag == 2 and len(ix.accounts) >= 2:  # Transfer
            lam = struct.unpack_from("<Q", ix.data, 4)[0]
            res.append((acct(ntx, ix, 0), acct(ntx, ix, 1), lam, ix.inner))
        elif tag == 11 and len(ix.accounts) >= 3:  # TransferWithSeed: from(base) is acct 0, to acct 2
            lam = struct.unpack_from("<Q", ix.data, 4)[0]
            res.append((acct(ntx, ix, 0), acct(ntx, ix, 2), lam, ix.inner))
        elif tag == 0 and len(ix.accounts) >= 2 and len(ix.data) >= 12:  # CreateAccount funds new account
            lam = struct.unpack_from("<Q", ix.data, 4)[0]
            res.append((acct(ntx, ix, 0), acct(ntx, ix, 1), lam, ix.inner))
    return res


def token_balance_map(entries: list[dict], keys: list[str]) -> dict[tuple[str, str], int]:
    """(owner, mint) -> raw amount."""
    out = {}
    for t in entries:
        owner = t.get("owner")
        mint = t.get("mint")
        amt = int((t.get("uiTokenAmount") or {}).get("amount") or 0)
        out[(owner, mint)] = out.get((owner, mint), 0) + amt
    return out

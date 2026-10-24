"""Operator profiles + runtime watch sets.

An operator is identified by the wallets that fund its creator wallets. The
live engine watches those funders; every fresh wallet they fund (amount within
the profile's range) becomes a watched creator, and that creator's launches
are traded if they match the profile's launch signature and the operator's
policy is enabled.

operators.json is produced by research/validation and updated by the learner.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Policy:
    enabled: bool = False  # trade at all (paper mode records regardless)
    size_sol: float = 0.1
    tp_pct: float = 30.0  # take profit on net position value vs cost
    max_hold_s: float = 60.0
    sl_pct: Optional[float] = None
    max_entry_mcap_mult: float = 1.15  # price guard vs expected post-dev-buy mcap
    max_slot_offset: int = 8  # don't buy if we'd land later than this (enforced by guard + expiry)
    first_launch_only: bool = True
    max_open: int = 2
    auto: bool = True  # learner may enable/disable
    min_live_ev: float = 0.0  # learner threshold on rolling simulated EV


@dataclass
class Operator:
    id: str
    label: str = ""
    funders: list[str] = field(default_factory=list)
    creators: list[str] = field(default_factory=list)  # static creator wallets (reused-address operators)
    fund_min_sol: float = 1.0
    fund_max_sol: float = 50.0
    fresh_only: bool = True
    follow_chain: bool = True  # creator -> fresh wallet transfers keep the identity
    follow_child_funders_min_sol: Optional[float] = 20.0  # big transfers to fresh wallets => candidate funder
    dev_buy_min_sol: float = 0.0
    dev_buy_max_sol: float = 80.0
    allow_mayhem: bool = False
    max_creator_fee_bps: int = 30
    creator_ttl_h: float = 24.0
    policy: Policy = field(default_factory=Policy)
    stats: dict = field(default_factory=dict)
    note: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "Operator":
        d = dict(d)
        pol = Policy(**d.pop("policy", {}))
        known = {f for f in cls.__dataclass_fields__}
        return cls(policy=pol, **{k: v for k, v in d.items() if k in known})


@dataclass
class CreatorInfo:
    creator: str
    operator: str
    funder: str
    lamports: int
    launches: int = 0
    t: float = field(default_factory=time.time)
    expires: float = 0.0


class OperatorBook:
    def __init__(self, path: str):
        self.path = Path(path)
        self.ops: dict[str, Operator] = {}
        self.funder_to_op: dict[str, str] = {}
        self.funder_role: dict[str, str] = {}
        self.creators: dict[str, CreatorInfo] = {}
        self.version = 0
        self.load()

    def load(self):
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text())
        self.ops = {o["id"]: Operator.from_dict(o) for o in raw.get("operators", [])}
        self.funder_to_op = {}
        for o in self.ops.values():
            for f in o.funders:
                self.funder_to_op[f] = o.id
                self.funder_role.setdefault(f, "root")
            for c in o.creators:
                info = CreatorInfo(c, o.id, "static", 0)
                info.expires = float("inf")
                self.creators[c] = info
        self.version += 1

    def save(self):
        raw = {"updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "operators": [asdict(o) for o in self.ops.values()]}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(raw, indent=1))
        tmp.replace(self.path)

    # ---------------------------------------------------------------- watch sets
    def add_funder(self, addr: str, op_id: str, role: str = "child") -> bool:
        if addr in self.funder_to_op:
            return False
        self.funder_to_op[addr] = op_id
        self.funder_role[addr] = role
        self.version += 1
        return True

    def add_creator(self, creator: str, op_id: str, funder: str, lamports: int) -> bool:
        op = self.ops.get(op_id)
        if not op:
            return False
        new = creator not in self.creators
        info = self.creators.get(creator) or CreatorInfo(creator, op_id, funder, lamports)
        info.expires = time.time() + op.creator_ttl_h * 3600
        self.creators[creator] = info
        if new:
            self.version += 1
        return new

    def expire(self):
        now = time.time()
        dead = [c for c, i in self.creators.items() if i.expires < now]
        for c in dead:
            del self.creators[c]
        if dead:
            self.version += 1

    def watched_addresses(self) -> list[str]:
        return sorted(set(self.funder_to_op) | set(self.creators))

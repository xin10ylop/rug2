"""Config loading (TOML) with environment overrides for secrets."""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib


@dataclass
class RouteCfg:
    name: str
    url: str
    kind: str = "jsonrpc"
    headers: dict = field(default_factory=dict)
    enabled: bool = True
    sides: list = field(default_factory=lambda: ["buy", "sell"])
    warm_url: str | None = None


@dataclass
class Config:
    rpc_url: str
    grpc_url: str
    grpc_token: str | None
    keypair_path: str
    db_path: str = "data/live.db"
    operators_path: str = "config/operators.json"
    mode: str = "paper"  # paper | live
    routes: list[RouteCfg] = field(default_factory=list)
    # fees
    buy_priority_lamports: int = 500_000
    buy_tip_lamports: int = 500_000
    sell_priority_lamports: int = 500_000
    sell_tip_lamports: int = 500_000
    buy_cu_limit: int = 140_000
    sell_cu_limit: int = 100_000
    tip_accounts: list[str] = field(default_factory=list)  # tip destinations (must satisfy every route used)
    # risk
    max_open_positions: int = 3
    max_size_sol: float = 0.5
    daily_loss_limit_sol: float = 1.0
    min_balance_sol: float = 0.3
    max_consecutive_losses: int = 5
    kill_file: str = "STOP"
    # behaviour
    tape_minutes: float = 10.0
    sell_retry_s: float = 1.2
    buy_expire_slots: int = 20
    status_path: str = "data/status.json"
    learner_every_s: float = 600.0
    # paper mode: simulate a buy+sell round trip for every paper decision (nothing is sent)
    paper_simulate: bool = True
    sim_payer: str = "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9"  # any well-funded account (sigVerify off)
    log_level: str = "INFO"


def load(path: str) -> Config:
    raw = tomllib.loads(Path(path).read_text())
    env = os.environ
    conn = raw.get("connection", {})
    rpc_url = env.get("RUGSNIPE_RPC_URL") or conn.get("rpc_url")
    grpc_url = env.get("RUGSNIPE_GRPC_URL") or conn.get("grpc_url")
    grpc_token = env.get("RUGSNIPE_GRPC_TOKEN") or conn.get("grpc_token")
    if not rpc_url or not grpc_url:
        raise SystemExit("config: connection.rpc_url and connection.grpc_url are required (or env RUGSNIPE_*)")
    routes = [RouteCfg(**r) for r in raw.get("routes", [])]
    for r in routes:
        r.url = os.path.expandvars(r.url)
        r.headers = {k: os.path.expandvars(v) for k, v in r.headers.items()}
    cfg = Config(rpc_url=os.path.expandvars(rpc_url), grpc_url=os.path.expandvars(grpc_url),
                 grpc_token=os.path.expandvars(grpc_token) if grpc_token else None,
                 keypair_path=os.path.expandvars(raw.get("wallet", {}).get("keypair_path", "keys/trader.json")),
                 routes=routes)
    for section in ("general", "fees", "risk", "behaviour"):
        for k, v in raw.get(section, {}).items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
    if env.get("RUGSNIPE_MODE"):
        cfg.mode = env["RUGSNIPE_MODE"]
    if cfg.mode not in ("paper", "live"):
        raise SystemExit("mode must be paper or live")
    return cfg

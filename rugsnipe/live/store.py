"""SQLite recorder for the live system (launches, fundings, tapes, attempts, positions).

Everything needed to reconcile live results against the simulator is stored:
when we saw each launch (deshred / processed), what we decided and why, when
our transactions were sent and where they landed, and the full early trade
tape of every launch from a watched operator (traded or not).
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS launches(
  mint TEXT PRIMARY KEY, sig TEXT, slot INTEGER, tx_index INTEGER, creator TEXT, operator TEXT,
  t_deshred REAL, t_processed REAL, block_time INTEGER, dev_buy_sol REAL, mayhem INTEGER, creator_fee_bps INTEGER,
  sol_quoted INTEGER, first_launch INTEGER, name TEXT, symbol TEXT, uri TEXT, decision TEXT, reason TEXT,
  mcap_after_bundle REAL, extra TEXT);
CREATE INDEX IF NOT EXISTS launches_creator ON launches(creator);
CREATE INDEX IF NOT EXISTS launches_operator ON launches(operator, slot);
CREATE TABLE IF NOT EXISTS fundings(
  creator TEXT PRIMARY KEY, funder TEXT, operator TEXT, lamports INTEGER, slot INTEGER, t REAL, fresh INTEGER,
  sig TEXT, launches INTEGER DEFAULT 0, expires REAL);
CREATE TABLE IF NOT EXISTS funders(
  addr TEXT PRIMARY KEY, operator TEXT, role TEXT, added REAL, parent TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS tape(
  mint TEXT, slot INTEGER, idx INTEGER, seq INTEGER, sig TEXT, user TEXT, payer TEXT, is_buy INTEGER,
  sol INTEGER, tok INTEGER, vsr INTEGER, vtr INTEGER, t REAL, ix TEXT, err INTEGER,
  PRIMARY KEY(mint, sig, seq));
CREATE TABLE IF NOT EXISTS attempts(
  id INTEGER PRIMARY KEY AUTOINCREMENT, mint TEXT, operator TEXT, side TEXT, t_trigger REAL, t_sent REAL,
  sig TEXT, size_lamports INTEGER, min_out INTEGER, max_entry_mcap REAL, routes TEXT, status TEXT,
  landed_slot INTEGER, landed_idx INTEGER, slot_offset INTEGER, sol_delta INTEGER, tokens INTEGER,
  fill_mcap REAL, err TEXT, extra TEXT);
CREATE INDEX IF NOT EXISTS attempts_sig ON attempts(sig);
CREATE TABLE IF NOT EXISTS positions(
  mint TEXT PRIMARY KEY, operator TEXT, opened REAL, entry_slot INTEGER, tokens INTEGER, cost INTEGER,
  status TEXT, closed REAL, proceeds INTEGER, pnl INTEGER, exit_reason TEXT, extra TEXT);
CREATE TABLE IF NOT EXISTS events(t REAL, kind TEXT, data TEXT);
"""


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()
        self._pending = 0
        self.db.execute("BEGIN")

    def commit(self):
        with self.lock:
            try:
                self.db.execute("COMMIT")
            except sqlite3.OperationalError:
                pass
            self.db.execute("BEGIN")
            self._pending = 0

    def x(self, sql: str, args: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self.lock:
            self._pending += 1
            return self.db.execute(sql, tuple(args))

    def q(self, sql: str, args: Iterable[Any] = ()) -> list[tuple]:
        with self.lock:
            return self.db.execute(sql, tuple(args)).fetchall()

    # ------------------------------------------------------------------ helpers
    def event(self, kind: str, **data):
        self.x("INSERT INTO events VALUES(?,?,?)", (time.time(), kind, json.dumps(data, default=str)))

    def upsert_launch(self, mint: str, **f):
        cols = ["mint"] + list(f)
        vals = [mint] + [json.dumps(v) if isinstance(v, (dict, list)) else v for v in f.values()]
        upd = ",".join(f"{c}=COALESCE(excluded.{c},{c})" for c in f)
        self.x(f"INSERT INTO launches({','.join(cols)}) VALUES({','.join('?' * len(cols))}) "
               f"ON CONFLICT(mint) DO UPDATE SET {upd}", vals)

    def add_funding(self, creator: str, funder: str, operator: str, lamports: int, slot: int, fresh: bool, sig: str,
                    ttl_s: float):
        now = time.time()
        self.x("INSERT INTO fundings(creator,funder,operator,lamports,slot,t,fresh,sig,launches,expires) "
               "VALUES(?,?,?,?,?,?,?,?,0,?) ON CONFLICT(creator) DO UPDATE SET expires=excluded.expires",
               (creator, funder, operator, lamports, slot, now, int(fresh), sig, now + ttl_s))

    def active_creators(self) -> list[tuple]:
        return self.q("SELECT creator, operator, launches, funder, lamports FROM fundings WHERE expires > ?",
                      (time.time(),))

    def bump_launches(self, creator: str):
        self.x("UPDATE fundings SET launches = launches + 1 WHERE creator=?", (creator,))

    def add_tape(self, mint: str, rows: list[tuple]):
        with self.lock:
            self.db.executemany("INSERT OR IGNORE INTO tape VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                [(mint,) + r for r in rows])
            self._pending += len(rows)

    def attempt(self, **f) -> int:
        cols = list(f)
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in f.values()]
        cur = self.x(f"INSERT INTO attempts({','.join(cols)}) VALUES({','.join('?' * len(cols))})", vals)
        return cur.lastrowid

    def update_attempt(self, aid: int, **f):
        sets = ",".join(f"{c}=?" for c in f)
        vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in f.values()]
        self.x(f"UPDATE attempts SET {sets} WHERE id=?", vals + [aid])

    def tape_for(self, mint: str) -> list[dict]:
        rows = self.q("SELECT slot, idx, seq, sig, user, payer, is_buy, sol, tok, vsr, vtr, t, ix, err FROM tape "
                      "WHERE mint=? ORDER BY slot, idx, seq", (mint,))
        return [dict(zip(["slot", "idx", "seq", "sig", "user", "payer", "is_buy", "sol", "tok", "vsr", "vtr", "t",
                          "ix", "err"], r)) for r in rows]

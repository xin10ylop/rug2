"""pump.fun bonding-curve math (SOL-quoted curves) with the current fee schedule.

Integer math mirrors the program closely enough for sizing/slippage (within a few
lamports); simulation uses the same functions so live and backtest agree.
"""
from __future__ import annotations

from dataclasses import dataclass

from .constants import (
    CREATOR_FEE_BPS_DEFAULT,
    INITIAL_REAL_TOKEN_RESERVES,
    INITIAL_VIRTUAL_SOL_RESERVES,
    INITIAL_VIRTUAL_TOKEN_RESERVES,
    PROTOCOL_FEE_BPS,
    TOKEN_TOTAL_SUPPLY,
)

BPS = 10_000


@dataclass(slots=True)
class Curve:
    vsr: int  # virtual SOL reserves (lamports)
    vtr: int  # virtual token reserves (raw, 6 decimals)
    rtr: int = INITIAL_REAL_TOKEN_RESERVES
    protocol_fee_bps: int = PROTOCOL_FEE_BPS
    creator_fee_bps: int = CREATOR_FEE_BPS_DEFAULT

    @classmethod
    def fresh(cls, creator_fee_bps: int = CREATOR_FEE_BPS_DEFAULT) -> "Curve":
        return cls(INITIAL_VIRTUAL_SOL_RESERVES, INITIAL_VIRTUAL_TOKEN_RESERVES, INITIAL_REAL_TOKEN_RESERVES,
                   PROTOCOL_FEE_BPS, creator_fee_bps)

    def copy(self) -> "Curve":
        return Curve(self.vsr, self.vtr, self.rtr, self.protocol_fee_bps, self.creator_fee_bps)

    @property
    def fee_bps(self) -> int:
        return self.protocol_fee_bps + self.creator_fee_bps

    # price in lamports per raw token unit == SOL per whole token * 1e3
    @property
    def price(self) -> float:
        return self.vsr / self.vtr

    @property
    def mcap_sol(self) -> float:
        """Market cap in SOL (1B supply)."""
        return self.vsr / self.vtr * TOKEN_TOTAL_SUPPLY / 1e9

    # ---- quotes (no state change)
    def tokens_for_sol(self, sol_in_gross: int) -> int:
        """buy_exact_sol_in: gross lamports in (fees included) -> tokens out."""
        net = sol_in_gross * BPS // (BPS + self.fee_bps)
        tok = self.vtr * net // (self.vsr + net)
        return min(tok, self.rtr)

    def sol_for_tokens(self, tokens: int) -> int:
        """buy: cost before fees for exactly `tokens` out."""
        if tokens >= self.vtr:
            raise ValueError("tokens exceed reserves")
        return self.vsr * tokens // (self.vtr - tokens) + 1

    def sell_quote(self, tokens: int) -> int:
        """sell: lamports received after fees."""
        gross = self.vsr * tokens // (self.vtr + tokens)
        fee = (gross * self.protocol_fee_bps + BPS - 1) // BPS + (gross * self.creator_fee_bps + BPS - 1) // BPS
        return max(0, gross - fee)

    # ---- state transitions
    def apply_buy_sol(self, net_sol: int, tokens: int) -> None:
        self.vsr += net_sol
        self.vtr -= tokens
        self.rtr -= tokens

    def apply_sell(self, tokens: int, gross_sol: int) -> None:
        self.vsr -= gross_sol
        self.vtr += tokens
        self.rtr += tokens

    def buy_sol(self, sol_in_gross: int) -> int:
        """Execute a gross-SOL buy against the curve, return tokens received."""
        net = sol_in_gross * BPS // (BPS + self.fee_bps)
        tok = min(self.vtr * net // (self.vsr + net), self.rtr)
        self.apply_buy_sol(net, tok)
        return tok

    def sell_tokens(self, tokens: int) -> int:
        gross = self.vsr * tokens // (self.vtr + tokens)
        out = self.sell_quote(tokens)
        self.apply_sell(tokens, gross)
        return out


def mcap_sol(vsr: int, vtr: int) -> float:
    return vsr / vtr * TOKEN_TOTAL_SUPPLY / 1e9


def roundtrip_value(curve: Curve, tokens: int) -> int:
    """What `tokens` would sell for right now (after fees), lamports."""
    return curve.sell_quote(tokens)

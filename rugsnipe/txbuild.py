"""Transaction builders for pump.fun buy / sell (SOL-quoted curves, current program).

Account order follows the IDL in @pump-fun/pump-sdk 4.3.1 and the SDK's
`getBuyInstructionInternal` / `getSellInstructionInternal` (remaining accounts:
bonding_curve_v2 PDA (ro), buyback fee recipient (w)).

The buy uses `buy_exact_sol_in` with `min_tokens_out` as a hard price guard:
if we land late (after a pump) the transaction fails instead of buying high.
"""
from __future__ import annotations

import random
import struct
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

from . import constants as C

PK = Pubkey.from_string
PUMP = PK(C.PUMP_PROGRAM)
FEE_PROGRAM = PK(C.PUMP_FEE_PROGRAM)
SYSTEM = PK(C.SYSTEM_PROGRAM)
TOKEN_2022 = PK(C.TOKEN_2022_PROGRAM)
TOKEN = PK(C.TOKEN_PROGRAM)
ATA = PK(C.ATA_PROGRAM)
GLOBAL = PK(C.PUMP_GLOBAL)
EVENT_AUTH = PK(C.PUMP_EVENT_AUTHORITY)
FEE_CONFIG = PK(C.PUMP_FEE_CONFIG)


def pda(seeds: list[bytes], program: Pubkey = PUMP) -> Pubkey:
    return Pubkey.find_program_address(seeds, program)[0]


GLOBAL_VOLUME_ACC = pda([b"global_volume_accumulator"])


@lru_cache(maxsize=4096)
def bonding_curve_pda(mint: Pubkey) -> Pubkey:
    return pda([b"bonding-curve", bytes(mint)])


@lru_cache(maxsize=4096)
def bonding_curve_v2_pda(mint: Pubkey) -> Pubkey:
    return pda([b"bonding-curve-v2", bytes(mint)])


@lru_cache(maxsize=4096)
def creator_vault_pda(creator: Pubkey) -> Pubkey:
    return pda([b"creator-vault", bytes(creator)])


@lru_cache(maxsize=4096)
def holder_rewards_pda(mint: Pubkey) -> Pubkey:
    return pda([b"holder-rewards", bytes(mint)])


@lru_cache(maxsize=256)
def user_volume_acc_pda(user: Pubkey) -> Pubkey:
    return pda([b"user_volume_accumulator", bytes(user)])


@lru_cache(maxsize=8192)
def ata(owner: Pubkey, mint: Pubkey, token_program: Pubkey = TOKEN_2022) -> Pubkey:
    return Pubkey.find_program_address([bytes(owner), bytes(token_program), bytes(mint)], ATA)[0]


def create_ata_idempotent(payer: Pubkey, owner: Pubkey, mint: Pubkey, token_program: Pubkey = TOKEN_2022) -> Instruction:
    return Instruction(ATA, bytes([1]), [
        AccountMeta(payer, True, True),
        AccountMeta(ata(owner, mint, token_program), False, True),
        AccountMeta(owner, False, False),
        AccountMeta(mint, False, False),
        AccountMeta(SYSTEM, False, False),
        AccountMeta(token_program, False, False),
    ])


def close_account(account: Pubkey, dest: Pubkey, owner: Pubkey, token_program: Pubkey = TOKEN_2022) -> Instruction:
    return Instruction(token_program, bytes([9]), [
        AccountMeta(account, False, True),
        AccountMeta(dest, False, True),
        AccountMeta(owner, True, False),
    ])


@dataclass(frozen=True)
class CurveKeys:
    mint: Pubkey
    creator: Pubkey  # bonding_curve.creator (holder-rewards PDA for holder-reward coins)
    token_program: Pubkey = TOKEN_2022
    mayhem: bool = False
    cashback: bool = False

    @property
    def bonding_curve(self) -> Pubkey:
        return bonding_curve_pda(self.mint)

    @property
    def assoc_bonding_curve(self) -> Pubkey:
        return ata(self.bonding_curve, self.mint, self.token_program)


def fee_recipient(mayhem: bool) -> Pubkey:
    return PK(random.choice(C.RESERVED_FEE_RECIPIENTS if mayhem else C.FEE_RECIPIENTS))


def buyback_recipient() -> Pubkey:
    return PK(random.choice(C.BUYBACK_FEE_RECIPIENTS))


def buy_exact_sol_in_ix(user: Pubkey, k: CurveKeys, spendable_sol: int, min_tokens_out: int,
                        track_volume: bool = False) -> Instruction:
    data = C.IX_DISC_BY_NAME["buy_exact_sol_in"] + struct.pack("<QQ", spendable_sol, min_tokens_out) \
        + bytes([1 if track_volume else 0]) + bytes([0])  # track_volume OptionBool, partial_fill OptionBool(false)
    accounts = [
        AccountMeta(GLOBAL, False, False),
        AccountMeta(fee_recipient(k.mayhem), False, True),
        AccountMeta(k.mint, False, False),
        AccountMeta(k.bonding_curve, False, True),
        AccountMeta(k.assoc_bonding_curve, False, True),
        AccountMeta(ata(user, k.mint, k.token_program), False, True),
        AccountMeta(user, True, True),
        AccountMeta(SYSTEM, False, False),
        AccountMeta(k.token_program, False, False),
        AccountMeta(creator_vault_pda(k.creator), False, True),
        AccountMeta(EVENT_AUTH, False, False),
        AccountMeta(PUMP, False, False),
        AccountMeta(GLOBAL_VOLUME_ACC, False, False),
        AccountMeta(user_volume_acc_pda(user), False, True),
        AccountMeta(FEE_CONFIG, False, False),
        AccountMeta(FEE_PROGRAM, False, False),
        # remaining accounts
        AccountMeta(bonding_curve_v2_pda(k.mint), False, False),
        AccountMeta(buyback_recipient(), False, True),
    ]
    return Instruction(PUMP, data, accounts)


def sell_ix(user: Pubkey, k: CurveKeys, token_amount: int, min_sol_out: int) -> Instruction:
    data = C.IX_DISC_BY_NAME["sell"] + struct.pack("<QQ", token_amount, min_sol_out)
    accounts = [
        AccountMeta(GLOBAL, False, False),
        AccountMeta(fee_recipient(k.mayhem), False, True),
        AccountMeta(k.mint, False, False),
        AccountMeta(k.bonding_curve, False, True),
        AccountMeta(k.assoc_bonding_curve, False, True),
        AccountMeta(ata(user, k.mint, k.token_program), False, True),
        AccountMeta(user, True, True),
        AccountMeta(SYSTEM, False, False),
        AccountMeta(creator_vault_pda(k.creator), False, True),
        AccountMeta(k.token_program, False, False),
        AccountMeta(EVENT_AUTH, False, False),
        AccountMeta(PUMP, False, False),
        AccountMeta(FEE_CONFIG, False, False),
        AccountMeta(FEE_PROGRAM, False, False),
    ]
    if k.cashback:
        accounts.append(AccountMeta(user_volume_acc_pda(user), False, True))
    accounts += [
        AccountMeta(bonding_curve_v2_pda(k.mint), False, False),
        AccountMeta(buyback_recipient(), False, True),
    ]
    return Instruction(PUMP, data, accounts)


@dataclass
class Fees:
    cu_limit: int = 120_000
    cu_price_micro: int = 0  # microlamports per CU (priority fee)
    tip_lamports: int = 0
    tip_account: Optional[str] = None  # if None and tip>0 -> random Jito tip account


def _budget_and_tip(payer: Pubkey, fees: Fees) -> tuple[list[Instruction], list[Instruction]]:
    pre = [set_compute_unit_limit(fees.cu_limit)]
    if fees.cu_price_micro:
        pre.append(set_compute_unit_price(fees.cu_price_micro))
    post = []
    if fees.tip_lamports > 0:
        to = PK(fees.tip_account or random.choice(C.JITO_TIP_ACCOUNTS))
        post.append(transfer(TransferParams(from_pubkey=payer, to_pubkey=to, lamports=fees.tip_lamports)))
    return pre, post


def build_buy_tx(kp: Keypair, k: CurveKeys, spendable_sol: int, min_tokens_out: int, blockhash: Hash,
                 fees: Fees) -> VersionedTransaction:
    user = kp.pubkey()
    pre, post = _budget_and_tip(user, fees)
    ixs = pre + [create_ata_idempotent(user, user, k.mint, k.token_program),
                 buy_exact_sol_in_ix(user, k, spendable_sol, min_tokens_out)] + post
    msg = MessageV0.try_compile(user, ixs, [], blockhash)
    return VersionedTransaction(msg, [kp])


def build_sell_tx(kp: Keypair, k: CurveKeys, token_amount: int, min_sol_out: int, blockhash: Hash, fees: Fees,
                  close: bool = True) -> VersionedTransaction:
    user = kp.pubkey()
    pre, post = _budget_and_tip(user, fees)
    ixs = pre + [sell_ix(user, k, token_amount, min_sol_out)]
    if close:
        ixs.append(close_account(ata(user, k.mint, k.token_program), user, user, k.token_program))
    ixs += post
    msg = MessageV0.try_compile(user, ixs, [], blockhash)
    return VersionedTransaction(msg, [kp])


def min_tokens_for_max_mcap(spendable_sol: int, max_mcap_sol: float, fee_bps: int) -> int:
    """Smallest acceptable token amount so that our *average* fill is no worse than `max_mcap_sol`.

    avg price (lamports/raw token) = net_sol / tokens; mcap = price * 1e15 / 1e9.
    """
    net = spendable_sol * 10_000 // (10_000 + fee_bps)
    max_price = max_mcap_sol * 1e9 / 1e15  # lamports per raw unit
    return int(net / max_price)


def build_roundtrip_sim_tx(payer: Pubkey, k: CurveKeys, spendable_sol: int, min_tokens_out: int, blockhash: Hash,
                           cu_limit: int = 250_000) -> VersionedTransaction:
    """Unsigned buy-then-sell transaction for simulateTransaction(sigVerify=false) only.

    Proves the exact buy and sell instructions we would send execute against live state
    (accounts, PDAs, fee recipients, remaining accounts) without holding funds or sending."""
    from solders.signature import Signature
    ixs = [set_compute_unit_limit(cu_limit), create_ata_idempotent(payer, payer, k.mint, k.token_program),
           buy_exact_sol_in_ix(payer, k, spendable_sol, min_tokens_out),
           sell_ix(payer, k, min_tokens_out, 0)]
    msg = MessageV0.try_compile(payer, ixs, [], blockhash)
    return VersionedTransaction.populate(msg, [Signature.default()])

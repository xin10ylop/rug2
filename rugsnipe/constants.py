"""Program ids, discriminators and fixed accounts for pump.fun as deployed on mainnet.

Verified against the live program on 2026-10-24 (Global/FeeConfig accounts read
on-chain, layouts from @pump-fun/pump-sdk 4.3.1 IDL).
"""
from solders.pubkey import Pubkey

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
MAYHEM_PROGRAM = "MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"
WSOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_PUBKEY = "11111111111111111111111111111111"

# Every create/create_v2 references this PDA, buys and sells never do: a
# getSignaturesForAddress / gRPC account filter on it yields exactly the launches.
PUMP_MINT_AUTHORITY = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"
PUMP_GLOBAL = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"
PUMP_EVENT_AUTHORITY = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"
PUMP_FEE_CONFIG = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"

PUMP_PROGRAM_PK = Pubkey.from_string(PUMP_PROGRAM)
PUMP_FEE_PROGRAM_PK = Pubkey.from_string(PUMP_FEE_PROGRAM)

# Anchor emit_cpi! prefix on the self-invoked event instruction.
EVENT_IX_TAG = bytes.fromhex("e445a52e51cb9a1d")

EVENT_DISC = {
    "trade": bytes([189, 219, 127, 211, 78, 230, 97, 238]),
    "create": bytes([27, 114, 169, 77, 222, 235, 99, 118]),
    "complete": bytes([95, 114, 97, 156, 212, 46, 152, 8]),
}

IX_DISC = {
    bytes([214, 144, 76, 236, 95, 139, 49, 180]): "create_v2",
    bytes([24, 30, 200, 40, 5, 28, 7, 119]): "create",
    bytes([102, 6, 61, 18, 1, 218, 235, 234]): "buy",
    bytes([56, 252, 116, 8, 158, 223, 205, 95]): "buy_exact_sol_in",
    bytes([51, 230, 133, 164, 1, 127, 131, 173]): "sell",
    bytes([184, 23, 238, 97, 103, 197, 211, 61]): "buy_v2",
    bytes([7, 5, 29, 196, 245, 23, 101, 80]): "buy_v3",
    bytes([194, 171, 28, 70, 104, 77, 91, 47]): "buy_exact_quote_in_v2",
    bytes([225, 247, 80, 30, 213, 179, 132, 136]): "buy_exact_quote_in_v3",
    bytes([93, 246, 130, 60, 231, 233, 64, 178]): "sell_v2",
    bytes([28, 146, 222, 119, 38, 196, 105, 213]): "sell_v3",
}
IX_DISC_BY_NAME = {v: k for k, v in IX_DISC.items()}
BUY_IXS = {"buy", "buy_exact_sol_in", "buy_v2", "buy_v3", "buy_exact_quote_in_v2", "buy_exact_quote_in_v3"}
SELL_IXS = {"sell", "sell_v2", "sell_v3"}
CREATE_IXS = {"create", "create_v2"}

# Curve constants (Global account, SOL quote). Read live by `rugsnipe.curve.load_global`;
# these are the values observed on 2026-10-24 and are used only as fallbacks.
INITIAL_VIRTUAL_TOKEN_RESERVES = 1_073_000_000_000_000
INITIAL_VIRTUAL_SOL_RESERVES = 30_000_000_000
INITIAL_REAL_TOKEN_RESERVES = 793_100_000_000_000
TOKEN_TOTAL_SUPPLY = 1_000_000_000_000_000
TOKEN_DECIMALS = 6
PROTOCOL_FEE_BPS = 95
CREATOR_FEE_BPS_DEFAULT = 30
MAX_CONFIGURABLE_CREATOR_FEE_BPS = 300

# Fee recipients (Global.fee_recipient + fee_recipients); any one is accepted.
FEE_RECIPIENTS = [
    "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV",
    "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
    "7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX",
    "9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz",
    "AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY",
    "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
    "FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz",
    "G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP",
]
# Mayhem-mode curves pay the reserved recipients instead.
RESERVED_FEE_RECIPIENTS = [
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS",
    "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
    "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR",
    "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
    "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6",
    "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
    "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq",
    "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA",
]
BUYBACK_FEE_RECIPIENTS = [
    "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD",
    "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
    "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL",
    "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
    "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6",
    "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
    "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD",
    "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW",
]

# Jito tip accounts (mainnet). Any one may be used.
JITO_TIP_ACCOUNTS = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
]

LAMPORTS_PER_SOL = 1_000_000_000
BASE_FEE_PER_SIGNATURE = 5_000

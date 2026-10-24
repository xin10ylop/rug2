# rugsnipe — operator sniping on pump.fun, validated against what actually fills

This repository holds an on-chain research pipeline, a fill-realistic simulator and a live
trading engine (Yellowstone gRPC + multi-route sending). They were built to answer one
question honestly: **does the "snipe the ruggers" method from the F-Project videos have a net
edge you can capture live, after competition, ordering, latency, failed entries and all costs?**

The research below was run on 2026-10-24 against mainnet, using public RPC plus the Triton
endpoint. Every number can be re-run with the tools in `rugsnipe/research/`.

---

## 1. Verdict on the video's method

The video makes four claims.

**1. Systematic operators exist and are trackable via funding.** **True.**
- We found many operators that fund fresh creator wallets the way the video describes.
- Exchange examples: Binance withdrawals of exactly 0.999, 1.049, 1.144404 SOL to fresh wallets.
- "Mother wallet" examples: wallets that always send 2.0 SOL, or 60/80 SOL, to fresh creators.

**2. Snipe them in block 0 and take +100%.** **Not supported.**
- At realistic positions (end of the creation block, or 1–3 blocks later), +100% is the worst take-profit we tested.
- Price doubling from a realistic entry is rare: 3.3% of random launches.
- Most trackable operators lose 4–15% per trade after fees: their tokens never pump, or the dev sells into followers.

**3. "Rich ruggers" pump reliably.** **True for very few operators, and regime-dependent.**
- Of 34 launches that doubled, most came from funders with no other launch in the window, so they were not trackable in advance.
- Pumps are concentrated in a few operators, and their edge varies by the hour.

**4. Speed (block 0) is what makes it work.** **False for the edge we found.**
- The validated edge is flat from end-of-block+1 to block+5.
- Speed matters for the block-0 sniper race, which is exactly the race where the evidence says the money isn't (see section 3).

Broad rules lose money at realistic fills:
- 708 launches with a 3–80 SOL dev buy: **−8.8% per trade** (t = −8.5).
- 4,319 non-mayhem SOL launches, baseline at end of block+1: −3% to −6%.

Following early snipers (entering after their buys, no lookahead) is also negative or flat.

## 2. The edge that survived validation

The one operator that holds up we call `djtd`.
- **Funding:** a relay wallet, `DJTDXMc7BuN4t2PPTEnfmAPSJrwMgsEXrvdbWbxkvga8`, funds creator wallets with 4–24 SOL. Funding is routed through a temporary account, so it is attributed by balance changes, not transfer instructions.
- **Recycled wallets:** the creators are aged wallets (first used in 2024), drained to 0 SOL between uses and funded again later. "Fresh" therefore means *0 SOL at the funding*, not a new address. A filter on wallet age or history misses this operator completely; the first discovery runs did exactly that.
- **Behind the relay:** a bank wallet, `Df8CJQR7fUTYAQSQwtsgUDs5b6JWNULzwhJJXDCJkdya`.
  - It funds the relay, receives each creator's proceeds back (+13 to +58 SOL per creator), and seeds about 100 pump wallets with ~2 SOL each.
  - The engine watches both the bank and the relay, so a relay rotation is followed automatically.
- **Launch pattern:** each funding is followed by 1–7 tokens on trending narratives, with about 10 SOL dev buys. The first launch comes 14 slots or more after the funding (median ~90), so the engine always knows the creator in time.
- **Mechanics:** legacy `create` (SPL Token) + `extend_account` + `buy`, via several launch tools.
- **Second pattern, not traded:** on 10-23 it also ran a no-dev-buy variant (1–5 SOL fundings). It was positive too (+13.9 %, n = 24, t = 1.8), but it lies outside the configured profile (dev buy 8–14 SOL) and has not reappeared since.

"First launch" means **the first launch after the creator's latest funding**. That is exactly what the engine trades: a recycled wallet funded again from 0 starts a new count; a top-up does not.

| set (72 h to 2026-10-24 13:27 UTC; TP +30 %, hold 60 s, all costs) | n | mean / trade | win | t |
|---|---|---|---|---|
| all launches, entry end of slot+1 | 241 | +3.8 % | 65 % | 1.5 |
| **first launch after each funding**, entry end of slot+1 | 79 | **+12.9 %** | 80 % | **3.3** |
| same, entry end of slot+3 / slot+5 | 79 | +13.1 % / +13.1 % | 80 % | 3.3 |
| same, entry end of slot+10 | 79 | +10.2 % | 76 % | 2.6 |
| same, inside the profile's ranges (funding 2–30 SOL, dev buy 8–14 SOL) | 53 | +12.2 % | 75 % | 2.6 |
| repeat launches (2nd and later after a funding), slot+1 / slot+10 | 162 | −0.6 % / −4.4 % | 58 % | −0.2 |
| **discovery pipeline, automatic, strictly out-of-sample** (the 72 h before 10-24 02:17), slot+2 | 68 | **+13.7 %** | 81 % | **3.25** |

First launches by half-day session:

| 10-22 AM | 10-22 PM | 10-23 AM | 10-23 PM | 10-24 AM |
|---|---|---|---|---|
| −18 % (n = 9) | +26 % (16) | +29 % (14) | +9.7 % (29) | +5.2 % (10) |

The last three sessions are lower each time. That may be noise or the edge decaying, and 24 hours of live data will not settle which. The live system re-checks it continuously (section 3, point 4) and has hard kill criteria (RUNBOOK).

Robustness checks:
- **Parameters:** the TP 15–75 % × hold 20–180 s surface is positive everywhere (+2.5 % … +11.8 %); +30 % / 60–90 s is on a plateau, not a spike.
- **Size:** best at 0.25–0.5 SOL (≈ +11 %). At 0.05 SOL fixed fees eat it (+6.7 %); at 1 SOL our own impact costs ~3 points (+8.3 %).
- **Price guard:** median price drift in the first 3 blocks is 3.9 % (p95 8.9 %), so a +15 % guard rarely binds. Winners do not drift more than losers (no adverse selection through the guard). The guard blocks more losers (8 %) than winners (1 %).
- **Regime:** sessions range from −18 % to +29 % (table above). The engine re-validates continuously and auto-disables (section 5).

**Caveat.** The "first launch" split was found after looking at the data. It has a mechanism: follow-up launches are duplicates competing for the same narrative. It has since passed a stricter test: the discovery pipeline found the operator and the rule on its own, out of sample (t = 3.25). But on 10-24 morning, repeats (+18.7 %, n = 16) beat first launches (+5.2 %, n = 10). So the learner now scores repeat launches live as a control group (`sim_repeat_slot3`). Treat the rule as well supported, not proven.

Expected economics if it holds:
- Volume: ~18 first launches/day inside the profile (53 in 72 h).
- At 0.25 SOL and +12 %: about **+0.5 SOL/day**, with a daily standard deviation around ±0.4 SOL.
- At the last session's level (+5 %): about +0.2 SOL/day.
- This is a real but modest edge. The way to grow it is the discovery pipeline (section 6) finding more operators like this one, not raising size.

## 3. Why this does not repeat the "dry run scores races we never win" failure

Your previous strategy's profit came from launches where a rival was first. Those were the
launches that pumped, and live you only ever got the other half. This system is built to make
that impossible to miss:

1. **No assumed race wins.** The simulator (`rugsnipe/sim.py`) never places us first.
   - Entries are scored at the end of the creation block or 1, 2, 3, 5 or 10 blocks later, behind everyone who actually traded.
   - Exits trigger on what we would have seen, then land one block later, so a rug in that block is taken in full.
   - Every trade after our entry is re-applied on a curve that includes our own buy.
2. **The edge does not depend on speed.** It is the same from block+1 to block+5, so rivals beating us to block 0 does not remove it.
3. **Price guard + short-lived blockhash.**
   - `buy_exact_sol_in` carries a hard minimum-tokens bound (max +15 % vs the expected post-dev-buy price).
   - It is signed with a blockhash that expires about 12 blocks later.
   - A late or contested entry fails cheaply (base fee only) instead of buying the top or a dead token.
4. **Missed vs filled comparison, live.**
   - For every eligible launch, the learner re-simulates the full tape after the fact.
   - It compares launches we filled with launches we missed (expired or guard-failed).
   - If the missed ones are systematically the good ones (`adverse_selection_gap` > 15 %), the operator is disabled.
   - This is the exact check that would have caught the previous strategy within a day.
5. **Adverse selection by rivals.** Within `djtd`, winners and losers have the same number of early outside snipers (median 5 vs 5). The sniper race does not carry the outcome here.
6. **The live rule is the measured rule.** The engine counts launches from each creator's latest funding, both live and when it rebuilds state at start-up, and re-arms recycled wallets. That is the same definition as the tables above, checked by tests on a real djtd funding.
   - An earlier version counted launches over the last 24–48 hours instead. It would have skipped first launches by recycled wallets, which are the best group (+21.7 %, n = 15), and still traded everything else.
   - That is a smaller version of the "dry run scores trades we never take" problem, now removed.

## 4. Architecture

```
Yellowstone gRPC (Triton)
  SubscribeDeshred  ── creates, pre-execution ─────────────┐
  Subscribe(processed)                                     │
    watched funders/creators ─► funding & chain detection ─┤─► OperatorBook (operators.json)
    executed creates, tracked curves, our wallet, blocks ──┤
                                                           ▼
                        trigger: watched creator launches + signature match + policy + risk
                        ─► buy_exact_sol_in (price guard, ~12-block blockhash)
                        ─► MultiSender: Triton RPC + Helius Sender FRA (same signed bytes)
                        position: TP / time stop / dev-dump exit; sells retried with escalating slippage;
                        on-chain reconciliation if the stream misses our tx
SQLite (data/live.db): launches, fundings, tapes, attempts (send→land timing, slot offset), positions
Learner (every 10 min): full tapes via getTransactionsForAddress → re-simulate → reconcile → auto-disable
```

| path | what |
|---|---|
| `rugsnipe/constants.py`, `decode.py` | pump.fun program as deployed today: `create`/`create_v2`, buy/sell v1–v3, quote mints, holder-reward/mayhem/cashback flags, CPI events, **v1 transactions** (SIMD-0385) |
| `rugsnipe/curve.py` | bonding-curve math with the live fee schedule (0.95 % protocol + 0.30 % creator; buyback is a slice of protocol) |
| `rugsnipe/txbuild.py` | buy/sell instructions (account order from the official SDK 4.3.1), Token-2022 and SPL Token, holder-reward creator vault |
| `rugsnipe/sim.py` | fill-realistic replay (section 3) |
| `rugsnipe/research/` | `backfill` (launches/funding/sigscan/tapes), `discover`, `evaluate`, `operator_history` (72 h out-of-sample), `pipeline` (all of it, automated) |
| `rugsnipe/live/` | `engine`, `senders`, `store`, `operators`, `learner` |
| `rugsnipe/cli.py` | `live`, `report`, `learn`, `check`, `keygen` |

Everything that touches the program was verified on mainnet:
- **Decoder:** matches the executed CreateEvent on 147/147 recent launches.
- **Dev-buy decoding:** matches the TradeEvent token amounts.
- **Buy+sell round trips:** `simulateTransaction` passes on Token-2022, SPL-Token (legacy create) and holder-reward curves (133–161K CU).
- **Engine paper run:** ran end to end on the live Triton streams (deshred + processed). Paper decisions were followed by successful round-trip simulations.

## 5. Running it

```bash
# Frankfurt box, Python 3.11+
git clone <repo> && cd rug2
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./scripts/gen_protos.sh
export TRITON_RPC_URL="https://<endpoint>.mainnet.rpcpool.com/<token>"
export TRITON_GRPC_URL="https://<endpoint>.mainnet.rpcpool.com"
export TRITON_X_TOKEN="<token>"
cp config/config.example.toml config/config.toml
cp config/operators.example.json config/operators.json
python -m rugsnipe.cli keygen --out keys/trader.json      # fund it with a small amount
python -m rugsnipe.cli check  --config config/config.toml # gRPC + RPC + simulated buy (nothing sent)
python -m rugsnipe.cli live   --config config/config.toml # mode=paper first
python -m rugsnipe.cli report --config config/config.toml
```

Follow **RUNBOOK.md** for the 24-hour go-live plan and its kill criteria.

Safety:
- `touch STOP` halts new buys immediately; open positions keep being managed.
- Hard limits: daily loss, max open positions, max size, min balance, and consecutive losses per operator (which auto-disables it).
- The learner only ever disables; enabling is your decision.

## 6. Finding the next operator

```bash
python -m rugsnipe.research.pipeline --hours 8 --out data/discovery/$(date +%H%M)
```

The pipeline:
- pulls every launch in the window (100 creates per call)
- finds the funding right before each creator's first launch, by balance changes, so routed funding is still attributed
- counts a creator as fresh when it held 0 SOL at that funding, as the engine does; recycled aged wallets count
- simulates the candidate class (fresh at funding, ≥3 SOL dev buy, SOL quote) at realistic entries
- re-validates in-sample-positive funders on the **prior** 72 hours, strictly out-of-sample, on first launches after each funding

On the 12 hours to 10-24 14:17 UTC it ranks `djtd` first in-sample (n = 28, +11.9 %) and reproduces its edge out-of-sample (table in section 2). Before the freshness fix the same run did not see `djtd` at all.

It writes `candidates.json` with ready-to-paste operator profiles. Promote a candidate to paper
first, and to live only when its out-of-sample first-launch mean is above +5 % with n ≥ 20 and t ≥ 2.

## 7. Costs per trade (as configured)

| item | buy | sell |
|---|---|---|
| pump fee (protocol + creator) | 1.25 % | 1.25 % |
| priority fee | 0.0002 SOL | 0.0005 SOL (×2, ×3, ×4 on retries) |
| tip (Helius Sender) | 0.00001 SOL (SWQOS) | 0.001 SOL (Max) |
| ATA rent | ~0.002 SOL | refunded on close |

The simulator charges 0.002 SOL per buy and 0.001 SOL per sell on top of the curve fees, which is
more than the configured costs.

Triton bills per million requests and per GB:
- **Engine:** deshred on creates only, plus processed on watched accounts. Small: tens of MB/hour.
- **Learner:** about 5–20 calls per eligible launch.
- **Discovery pipeline:** about 1–2 GB per 8-hour run; run it every few hours, not continuously.

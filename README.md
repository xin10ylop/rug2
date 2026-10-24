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
- **Funding:** a mother wallet, `DJTDXMc7BuN4t2PPTEnfmAPSJrwMgsEXrvdbWbxkvga8`, funds fresh creator wallets with 4–19 SOL. Funding is routed through a temporary account, so it is attributed by balance changes, not transfer instructions.
- **Launch pattern:** each creator fires 1–18 tokens on trending narratives, with about 10 SOL dev buys.
- **Mechanics:** legacy `create` (SPL Token) + `extend_account` + `buy`, via several launch tools.

| set (72 h, 2026-10-21..24) | n | mean / trade (all costs) | median | win | t |
|---|---|---|---|---|---|
| all launches, entry end of slot+1, TP +30 %, hold 60 s | 241 | **+3.8 %** | +28.6 % | 65 % | 1.5 |
| **first launch of each creator**, entry end of slot+1 | 64 | **+10.9 %** | — | 78 % | **2.4** |
| first launches, entry end of slot+3 / slot+5 | 64 | +11.0 % / +11.0 % | | | 2.4 |
| first launches, entry end of slot+10 | 64 | +7.6 % | | | 1.7 |

Robustness checks:
- **Parameters:** the TP 15–75 % × hold 20–180 s surface is positive everywhere (+2.5 % … +11.8 %); +30 % / 60–90 s is on a plateau, not a spike.
- **Size:** best at 0.25–0.5 SOL (≈ +11 %). At 0.05 SOL fixed fees eat it (+6.7 %); at 1 SOL our own impact costs ~3 points (+8.3 %).
- **Price guard:** median price drift in the first 3 blocks is 3.9 % (p95 8.9 %), so a +15 % guard rarely binds. Winners do not drift more than losers (no adverse selection through the guard). The guard blocks more losers (8 %) than winners (1 %).
- **Regime:** 12-hour buckets ranged **−14 % … +29 %**. The engine re-validates continuously and auto-disables (section 5).

**Caveat.** The "first launch" split was found after looking at the data. It has a mechanism: follow-up launches are duplicates competing for the same narrative. But treat it as a hypothesis to confirm live at small size, not a proven law.

Expected economics if it holds:
- Volume: ~20 first launches/day.
- At 0.25 SOL and +10 %: about **+0.5 SOL/day**, with a daily standard deviation around ±0.4 SOL.
- If it decays to the all-launch level (+4 %): about +0.2 SOL/day.
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
- traces fresh creators' funding by balance changes, so routed funding is still attributed
- simulates the candidate class (fresh creator, ≥3 SOL dev buy, SOL quote) at realistic entries
- re-validates in-sample-positive funders on the **prior** 72 hours, strictly out-of-sample

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

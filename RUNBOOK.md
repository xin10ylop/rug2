# 24-hour go-live runbook

Goal: show a **live, realized** net edge, or kill the strategy, inside 24 hours, with the
downside capped. There are no days of dry run. A short paper phase only proves the plumbing on
your box. After that, every judgement uses real fills.

Defaults:
- Size: 0.1 SOL per trade.
- Daily loss limit: 0.5 SOL.
- Max open positions: 2.

Fund the trading wallet with **1.0–1.5 SOL** for the whole day. That covers the loss limit,
fees and the minimum balance.

---

## Phase 0 — setup (T+0:00 → T+0:20)

```bash
cd rug2 && python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
./scripts/gen_protos.sh
export TRITON_RPC_URL=... TRITON_GRPC_URL=... TRITON_X_TOKEN=...
cp config/config.example.toml config/config.toml          # mode = "paper"
cp config/operators.example.json config/operators.json    # djtd: enabled=false
python -m rugsnipe.cli keygen --out keys/trader.json      # send 1.0-1.5 SOL to the printed address
python -m rugsnipe.cli check --config config/config.toml
```

**Gate 0:** `check` prints:
- the gRPC blockhash
- the RPC slot
- your wallet balance
- `simulated buy ... err=None`

If the simulated buy fails, stop and send me the logs.

## Phase 1 — paper on the live streams (T+0:20 → ~T+1:20)

```bash
nohup python -m rugsnipe.cli live --config config/config.toml > data/live.out 2>&1 &
watch -n 10 cat data/status.json
grep -E "launch |paper sim|BUY|SELL|creator .* funded" data/engine.log | tail
```

Expected:
- `bootstrap DJTDXMc7: N creators funded in the last 24h` with N > 0. If 0, the operator went quiet; run discovery (Phase 4) before going live.
- `creators: N watched, M already launched since their latest funding`. Launch counts start at each creator's latest funding, so a creator with 0 here has its first launch still ahead.
- Streams `connected`, `reconnects` low, `last_msg_age_s` < 5.
- Within an hour, `launch ... op=djtd ... -> paper paper_mode`, each followed by `paper sim ... sim_ok`. The operator launches about 3/hour; first launches about 1/hour.

**Gate 1, to go live:**
- At least 1 djtd decision has been logged.
- Every `paper sim` is `sim_ok`.
- No tracebacks in `data/engine.log`.
- `status.json` shows both streams alive for 30+ minutes.

## Phase 2 — live micro-size (T+1:20 → T+13)

In `config/config.toml` set `mode = "live"`. In `config/operators.json` set `djtd.policy.enabled = true` and keep `size_sol = 0.1`. Then restart:

```bash
pkill -f "rugsnipe.cli live"; sleep 2
nohup python -m rugsnipe.cli live --config config/config.toml > data/live.out 2>&1 &
```

Every 2 hours run `python -m rugsnipe.cli report --config config/config.toml --hours 24` and check:

| check | where | healthy | action if not |
|---|---|---|---|
| fill rate | `buys: filled/sent` | ≥ 70 % | if guard failures dominate, widen `max_entry_mcap_mult` to 1.25; if "expired", check routes/fees |
| landing position | `slot_offset` | median ≤ 3, ≥ 80 % ≤ 5 | raise `buy_priority_lamports` |
| trigger→send | `trigger->sent med` | < 50 ms | box/CPU problem |
| realized return | `closed positions mean ret` | consistent with the learner's `sim_on_filled` | gap > 10 pts ⇒ the model is wrong for us: stop |
| adverse selection | learner `adverse_selection_gap` | < 0.10 | > 0.15 ⇒ auto-disabled; do not re-enable |
| first vs repeat | learner `sim_slot3` vs `sim_repeat_slot3` | first launches ahead | if repeats beat first launches with n ≥ 10 each, the first-launch rule is not what carries the edge: do not scale, re-assess |
| exits | `exit tp / time / dev_dump` | tp ~60–75 %; no stuck `selling` | stuck ⇒ check routes and `data/engine.log` |

Kill criteria (stop and re-assess; most are automatic):
- Realized mean < −5 % after ≥ 10 closed trades.
- 6 consecutive losses (auto-disables the operator).
- Daily loss limit hit (auto: no new buys).
- Any position stuck > 2 minutes, or repeated reconcile warnings.
- `touch STOP` at any time halts new buys.

## Phase 3 — decide at about T+13 to T+24

With about 12 first-launch trades plus repeat-launch shadows (the learner simulates every eligible launch):
- **Scale to 0.25 SOL** only if all of these hold:
  - realized mean ≥ +5 % over ≥ 12 closed trades
  - landing median ≤ 3 slots
  - no adverse-selection flag
  - the learner's rolling `sim_slot3` mean > 0
- **Keep at 0.1** if realized is between −5 % and +5 %. The sample is too small to tell; let it run.
- **Stop** if realized < −5 %, or realized is more than 10 points below the simulation on the same launches.

**Never go above 0.5 SOL on this operator.** Our own impact eats the edge (the size sweep in the README).

## Phase 4 — find more operators (run in parallel, every ~6 h)

```bash
python -m rugsnipe.research.pipeline --hours 6 --out data/discovery/$(date +%m%d%H%M)
cat data/discovery/*/candidates.json | head -100
```

For a candidate with out-of-sample `oos_first.mean ≥ 0.05`, `n ≥ 20` and `t ≥ 2`:
1. Paste its `suggested_operator` into `config/operators.json` with `enabled: false`. That is shadow mode: it is recorded, simulated and reconciled by the learner without trading.
2. Enable it only after the learner's live `sim_slot3` on its new launches agrees (n ≥ 10, mean > 0).

## Notes

- **Operator schedule.** `djtd` works in sessions, roughly **05:00–11:00 and 13:00–21:00 UTC**,
  with 1.5–3 hour gaps between them. Over 72 hours its sessions produced 8–39 launches each.
  On 2026-10-24 its morning session ran 07:51–11:19 UTC: 12 creators, about 28 launches.
  - At 14:19–14:30 the bank moved 300 SOL out through its cash-out route (bank → `7Pq8Lhv3…` → `55jakZLs…`). It did the same between sessions on 10-22, so this is profit-taking, not an exit.
  - The operator's money path: bank `Df8CJQR7…` → relay `DJTDXMc7…` → fresh creator. The creator launches and sends its proceeds back through the relay to the bank.
  - Profits per creator ranged from +13 to +58 SOL, so this operator is still active and motivated.
  - Both the bank and the relay are watched. A new relay is picked up automatically, because a bank → fresh wallet transfer of 10–40 SOL registers the wallet and `follow_chain` follows its fundings.
  - If neither the bank nor the relay moves for more than 24 hours, treat the operator as gone and promote a discovery candidate instead.
- **Recycled wallets.** `djtd` creators are aged wallets drained to 0 and funded again later. The log shows `creator X re-funded by ...`, and that creator's next launch counts as a first launch again, which is the rule the edge was measured on. Top-ups (wallet not at 0) never re-arm a creator.
- **Restarts are safe.** Watched creators, child funders and launch counts are restored from SQLite. At startup every creator's launches are recounted on-chain from its latest funding. Creators funded or re-funded while the engine was down are recovered from the funder's history.
- **Operator rotation.** A watched funder that moves ≥ 40 SOL (`follow_child_funders_min_sol`) to a fresh wallet makes that wallet a child funder automatically. Creators that forward funds to a fresh wallet (`follow_chain`) pass the identity on.
- **Costs.**
  - Buys: 0.0002 SOL priority + 0.00001 SOL tip.
  - Sells: 0.0005 SOL priority + 0.001 SOL tip, escalating on retries.
  - Pump fee: 1.25 % per side.
  - ATA rent: refunded when the account is closed on the sell.

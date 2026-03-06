# Wallet Fleet Analysis: Limitless Prediction Market Arbitrage Operation

**Report Date:** March 7, 2026
**Data Source:** base.blockscout.com (Base L2 chain)
**Observation Window:** May 12, 2025 -- March 6, 2026
**Transactions Analyzed:** 54,001 across 15 wallets
**PnL Wallets Tracked:** 19 wallets (4 additional via PnL-only tracking)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Fleet Identification and Taxonomy](#2-fleet-identification-and-taxonomy)
3. [Wallet Profiles](#3-wallet-profiles)
4. [Strategy Analysis](#4-strategy-analysis)
5. [On-Chain Patterns](#5-on-chain-patterns)
6. [Gas and Cost Analysis](#6-gas-and-cost-analysis)
7. [Risk Analysis](#7-risk-analysis)
8. [Conclusions and Implications](#8-conclusions-and-implications)

---

## 1. Executive Summary

This report details a sophisticated, multi-wallet automated arbitrage operation targeting the **Limitless prediction market** on the Base L2 chain. The operation exploits pricing inefficiencies in binary outcome markets (e.g., "Will BTC be above $X at time T?") by purchasing both YES and NO positions when the combined price falls below $1.00, guaranteeing a riskless profit upon market resolution.

### Key Metrics at a Glance

| Metric | Value |
|--------|-------|
| Total PnL (all 19 wallets) | **$186,550** |
| Top wallet PnL (0x16B1) | **$79,736** |
| Combined fleet win rate | **81--82%** of days profitable |
| Max single-day gain | **$11,810** (0x16B1, Mar 6 2026) |
| Max drawdown / PnL ratio | **1.7%** (0x16B1) |
| Daily avg PnL (0x16B1) | **$857/day** |
| Total gas cost (all wallets) | **~$255 USD** |
| Unique market contracts traded | **109+** |
| Assets traded | BTC, ETH, SOL, DOGE, XRP + 9 others |
| Unique markets in snapshots | **155** (hourly price prediction markets) |
| Market contracts per day | **18--23** (one per market, each lives <1 hour) |
| Fleet operation duration | **93 days** (primary bot), **16 days** (fleet) |

The operation achieves a remarkable return-to-risk profile: $186,550 in cumulative profit against a maximum drawdown of just $1,391 on the primary trading wallet and total gas expenditure under $256 across all wallets combined. Gas costs represent less than 0.14% of total profits.

---

## 2. Fleet Identification and Taxonomy

Analysis of on-chain behavior, nonce patterns, bridge timing, and method distributions reveals three distinct wallet types operating under coordinated control.

### 2.1 Architecture Overview

```
                        Master Operator
                              |
                 +------------+------------+
                 |            |            |
            0x16B1       Bridge         External
         (Primary Bot)  (0x81D4)       Relayer(?)
          |                |               |
          | buy()          |               |
          v                v               v
      109 Market      10+ Fleet        0x8fdf
      Contracts       Wallets        (Independent)
          |               |
          | resolve       | redeemPositions()
          v               v
      ConditionalTokens (0xC9c9...8632B76e18)
                 |
                 v
              USDC out
```

### 2.2 Wallet Classification Summary

| Type | Count | Role | Identifying Pattern |
|------|-------|------|-------------------|
| Primary Trading Bot | 1 | Executes buy orders across markets | 66% buy, 32% redeemPositions, 22% buy error rate |
| Redemption Fleet | 8 | Redeems resolved positions at scale | ~100% redeemPositions, synchronized nonces ~1.7M, coordinated bridge funding |
| Independent Operators | 6 | Various: separate bot, smart contract wallet, order matcher | Heterogeneous method profiles, independent nonce sequences |

### 2.3 Fleet Identification Evidence

The 8 redemption fleet wallets share characteristics that cannot be coincidental:

1. **Synchronized nonces**: All 8 wallets have lifetime nonces clustered in the range 1,737,377 -- 1,737,769 (spread of only 392 across ~1.7M transactions), indicating they were deployed from the same factory or controller.

2. **Identical initialization sequence**: Every fleet wallet follows the exact same boot pattern:
   - Transaction 1: `null` method (self-interaction at max nonce)
   - Transaction 2: `receiveMessage` (bridge funding from `0x81D40F21`)
   - Transaction 3: `approve` USDC or `setApprovalForAll` on ConditionalTokens

3. **Coordinated bridge funding**: Bridge deposits arrive in batches within 30-minute windows, always in the same sequential order (0x9F99 first, 0xBB7e last), across 10+ funding events.

4. **Identical operational profile**: All 8 wallets execute 99.5--100% `redeemPositions` on the ConditionalTokens contract, with zero buy transactions and zero errors.

---

## 3. Wallet Profiles

### 3.1 Primary Trading Bot -- `0x16B1763BD7261190e573a510334Ce9b22673934a`

The flagship wallet of the operation. This is the only wallet that executes `buy()` calls on Limitless market contracts.

| Attribute | Value |
|-----------|-------|
| Total PnL | **$79,736.38** |
| Win rate (profitable days) | **82%** (78/95 days) |
| Daily avg PnL | **$857/day** |
| Active since | Dec 2, 2025 |
| Lifetime txns (nonce max) | 119,193 |
| Txns scraped (window) | 10,000 (Jan 28 -- Mar 6, 2026) |
| Max single-day gain | $11,810 (Mar 6, 2026) |
| Max single-day loss | -$1,002 |
| Max drawdown | $1,391 |
| Drawdown / PnL | **1.7%** |

**Method Distribution:**

| Method | Count | Percentage | Avg Gas | Avg Fee |
|--------|-------|------------|---------|---------|
| `buy` | 6,629 | 66.3% | 165,144 | 6.8 uETH |
| `redeemPositions` | 3,241 | 32.4% | 95,157 | -- |
| `approve` | 117 | 1.2% | 53,812 | -- |
| Other | 13 | 0.1% | -- | -- |

**Buy Performance:**

- **Successful buys:** 5,158 (77.8%)
- **Reverted buys:** 1,471 (22.2%)
- **Successful buy avg gas:** 198,204
- **Reverted buy avg gas:** 49,222 (fails fast, minimal waste)
- **Unique market contracts:** 109
- **Avg buys per day:** ~175

The 22.2% revert rate is characteristic of an aggressive bot strategy: fire buy transactions optimistically, accept that market conditions may change between detection and execution. The low gas on reverted transactions (49,222 vs 198,204 for successes) confirms that reverts fail early in execution -- the bot is not wasting significant gas on failed attempts.

**Top 5 Market Contracts by Buy Volume:**

| Contract | Buy Count |
|----------|-----------|
| `0x8AFF879B8941204d2122A3B0F46630Bd5b48D576` | 167 |
| `0xF8aaA2F907ba0D1BAe5DeEa630C6eE123e06af20` | 161 |
| `0x104753A00218b18824DE49aa2D949391D92E5253` | 156 |
| `0x7D1c422a755E8835B35E3ab38e9779B3a3745696` | 141 |
| `0xcdB4E5baC513aD31E470cE74997ca6a5903E7B94` | 141 |

**Weekly PnL Progression:**

| Week Ending | Cumulative PnL | Weekly Gain |
|-------------|---------------|-------------|
| 2025-12-06 | $38.74 | $38.74 |
| 2025-12-13 | $4,309.84 | $2,602.97 |
| 2025-12-20 | $6,851.82 | $2,293.47 |
| 2025-12-27 | $11,099.93 | $4,115.94 |
| 2025-12-31 | $14,739.25 | $2,160.19 |
| 2026-01-03 | $15,753.97 | $690.22 |
| 2026-01-10 | $16,991.10 | $1,466.01 |
| 2026-01-17 | $18,343.28 | $2,354.53 |
| 2026-01-24 | $28,066.63 | $7,036.12 |
| 2026-01-31 | $33,984.21 | $5,702.87 |
| 2026-02-07 | $43,191.56 | $7,284.86 |
| 2026-02-14 | $50,770.10 | $7,399.93 |
| 2026-02-21 | $57,987.67 | $5,806.42 |
| 2026-02-28 | $61,642.00 | $4,421.77 |
| 2026-03-06 | $79,736.38 | $17,675.06 |

The bot has been consistently profitable every week, with acceleration in late January through March. Week 10 of 2026 (ending Mar 6) produced the largest weekly gain of $17,675.

**Daily Transaction Volume (Last 10 Days):**

| Date | Txns |
|------|------|
| Feb 25 | 96 |
| Feb 26 | 70 |
| Feb 27 | 108 |
| Feb 28 | 90 |
| Mar 1 | 80 |
| Mar 2 | 78 |
| Mar 3 | 71 |
| Mar 4 | 77 |
| Mar 5 | 70 |
| Mar 6 | 62 |

Average: 263 txns/day across full period (including high-volume early days up to 1,784 txns/day).

---

### 3.2 Redemption Fleet (8 Wallets)

These wallets serve a single purpose: redeem resolved prediction market positions from the ConditionalTokens contract to extract USDC. They do not place any buy orders -- positions arrive through an off-chain or cross-contract mechanism.

| Wallet | Max Nonce | Txns Scraped | Bridge Txns | Daily Redeems (avg) | Active Days | PnL |
|--------|-----------|-------------|-------------|-------------------|-------------|-----|
| `0xb5b6...4E0E` | 1,737,769 | 3,935 | 11 | 231 | 17 | $5,316 |
| `0x2409...1Df6` | 1,737,749 | 3,848 | 11 | 240 | 16 | $1,502 |
| `0x4CDD...3D8E` | 1,737,747 | 3,763 | 10 | 234 | 16 | $2,155 |
| `0x9477...35bf` | 1,737,676 | 3,523 | 8 | 207 | 17 | $1,464 |
| `0x2C18...6c5C` | 1,737,672 | 4,098 | 9 | 240 | 17 | $3,647 |
| `0x0C05...F157` | 1,737,660 | 3,568 | 11 | 209 | 17 | $2,416 |
| `0x7836...0983` | 1,737,645 | 3,562 | 10 | 209 | 17 | $882 |
| `0x51c8...d739` | 1,737,377 | 3,539 | 14 | 207 | 17 | $1,932 |

**Fleet Aggregate:**
- **Combined PnL:** $19,312.85
- **Combined redeemPositions:** ~30,000 transactions
- **All started:** February 18, 2026
- **Zero error rate:** 0.00% across all 8 wallets

**Redemption Cadence:**
- **Median inter-transaction time:** 2.0 seconds
- **Mean inter-transaction time:** ~360 seconds (skewed by overnight gaps)
- **Minimum gap:** 0.0 seconds (same-block transactions)
- **Maximum daily redemptions:** 438 (single wallet)

The 2-second median cadence indicates automated execution with rapid-fire sequential transactions, consistent with a bot cycling through resolved market positions as they become redeemable.

---

### 3.3 Independent Operators

#### 3.3.1 `0x8fdfa8b2D83b429CaAF3D9d6f0F2865546eB12f1` -- Secondary Arbitrage Bot

| Attribute | Value |
|-----------|-------|
| Total PnL | **$64,208.90** |
| Win rate | **81%** (69/85 days) |
| Daily avg PnL | **$595/day** |
| Active since | Nov 17, 2025 (oldest profitable operator) |
| Lifetime nonce range | 0 -- 4,672 |
| Txns scraped | 4,604 |
| Method: `redeemPositions` | 4,576 (99.4%) |
| Error rate | 0.65% (30 errors) |
| Max drawdown | $1,141 |
| Drawdown / PnL | **1.8%** |

This wallet is the second-most profitable in the operation. Notably, 99.4% of its on-chain transactions are redemptions -- **buys are executed through a different mechanism** (possibly a separate wallet, a relayer, or an off-chain order system). Active positions show 9 of 11 markets held with both YES+NO sides, confirming the arbitrage strategy.

**Recent PnL flattening:** Between Mar 5 and Mar 6, PnL moved from $64,211.53 to $64,208.90 (a loss of $2.63), suggesting this wallet may be winding down or has reduced position sizing.

#### 3.3.2 `0x7d895A4acdd05E65f6340282f25D04A5E7C4BdBd` -- Smart Contract Wallet

| Attribute | Value |
|-----------|-------|
| Total PnL | $1,953.96 |
| Lifetime nonce range | 0 -- 191,151 |
| Txns scraped | 858 |
| Method: `null` (self-interactions) | 422 (49%) |
| Method: `redeemPositions` | 383 (45%) |
| Active since | Sep 18, 2025 |

The 49% `null` method transactions (419 self-interactions) are the hallmark of a smart contract wallet (e.g., Safe/Gnosis multisig or Account Abstraction wallet). The wallet interacts with itself to execute batched operations. This wallet has the longest history (nonce max 191,151) but relatively modest PnL, suggesting it is a more general-purpose wallet that includes prediction market activity as one of several strategies.

#### 3.3.3 `0x1c19c782722C3fBaE474316cd3f8dDf051F6E965` -- Legacy Wallet

| Attribute | Value |
|-----------|-------|
| Total PnL | $704.93 |
| Lifetime nonce range | 0 -- 3,710,217 |
| Txns scraped | 266 |
| Method: `redeemPositions` | 224 (84%) |
| First txn in data | May 12, 2025 |
| Data span | 297 days |

The oldest wallet in the dataset with 3.7M lifetime transactions. Only 266 were captured in our scrape window, indicating extremely high-volume historical activity that has significantly slowed. The nonce count of 3.7M suggests this wallet has been operational for a long time and may predate the Limitless prediction market operation.

#### 3.3.4 `0xBB7e70cf7874379Fc4f91CeB2BD3160b98c5DA16` -- High-Volume Redeemer

| Attribute | Value |
|-----------|-------|
| Total PnL | $9,697.42 |
| Lifetime nonce range | 1 -- 4,784 |
| Txns scraped | 4,450 |
| Method: `redeemPositions` | 4,440 (100%) |
| Bridge txns | 8 |
| Active since | Feb 18, 2026 |

Behaves identically to fleet wallets but with higher PnL, suggesting it receives larger or more favorable position allocations.

#### 3.3.5 `0x9F997F7C146FBC311aA5C1D0FD01b19b814277A5` -- Fleet-Adjacent Redeemer

| Attribute | Value |
|-----------|-------|
| Total PnL | $1,282.84 |
| Lifetime nonce range | 0 -- 4,253 |
| Txns scraped | 3,937 |
| Method: `redeemPositions` | 3,922 (100%) |
| Bridge txns | 13 (highest of any wallet) |

This wallet participates in coordinated bridge funding events alongside the fleet but has a higher bridge count and was typically the first wallet funded in each batch. May serve as a "lead" or test wallet for the fleet.

#### 3.3.6 `0xF94ef760884b0605E433853Aed17DA574160226E` -- FeeModule / Order Matcher

| Attribute | Value |
|-----------|-------|
| Txns scraped | 50 |
| Method: `matchOrders` | 50 (100%) |
| Nonce range | 124,459 -- 786,505 |
| Active window | 36 seconds (Mar 3, 23:20:03 -- 23:20:39) |

This is not a regular wallet but a **FeeModule smart contract** that performs Limitless order matching. All 50 transactions occurred within a 36-second burst, matching orders on the exchange. The high nonce range (up to 786K) confirms this is an infrastructure contract with long operational history.

---

### 3.4 PnL-Only Tracked Wallets (No Transaction Data Scraped)

These wallets appear in PnL tracking but their on-chain transactions were not included in the 54,001-transaction dataset:

| Wallet | PnL | First Seen | Notes |
|--------|-----|------------|-------|
| `0xAeC2` | $4,885.83 | Feb 18, 2026 | Fleet-era wallet |
| `0x44bF` | $4,344.64 | Feb 18, 2026 | Fleet-era wallet |
| `0xA6Ca` | $2,627.57 | Feb 18, 2026 | Fleet-era wallet |
| `0xC492` | -$276.16 | Feb 18, 2026 | Fleet-era, net negative |
| `0x48b5` | -$1,929.02 | Feb 18, 2026 | Fleet-era, largest loss |

All 5 appeared on the same day as the fleet (Feb 18, 2026), strongly suggesting they are additional fleet members. Combined PnL: $9,653.

---

## 4. Strategy Analysis

### 4.1 Core Strategy: Binary Outcome Arbitrage

The operation exploits a fundamental pricing inefficiency in binary prediction markets. In a binary market, the sum of YES and NO prices should equal exactly $1.00 (less fees). When `price_YES + price_NO < $1.00`, purchasing both sides guarantees a profit regardless of the outcome.

**How it works:**

```
Market: "Will BTC be above $68,416 at 16:00 UTC?"

  YES price: $0.5525    (55.25% implied probability)
  NO  price: $0.4273    (42.73% implied probability)
  --------------------------------
  Combined:  $0.9798    (97.98% -- should be ~100%)

  Arbitrage profit per share: $1.00 - $0.9798 = $0.0202 (2.02%)

  If outcome = YES: collect $1.00 on YES, lose NO stake
  If outcome = NO:  collect $1.00 on NO, lose YES stake

  Either way: gross profit = $1.00 - $0.9798 = $0.0202 per matched share
```

### 4.2 Observed Arbitrage Spreads

From the snapshot data (active positions as of March 6, 2026), observed combined prices ranged widely:

**Tight arbitrage examples (from 0x8fdf active positions):**

| Market | YES Price | NO Price | Combined | Arb Margin |
|--------|-----------|----------|----------|-----------|
| BTC > $70,566 at 11:00 | $0.4321 | $0.3959 | $0.8280 | **17.2%** |
| ETH > $1,971.28 at 18:00 | $0.5455 | $0.4120 | $0.9575 | **4.25%** |

The 17.2% spread on BTC is exceptionally wide, suggesting either low liquidity or temporary mispricing that the bot captured before market makers could correct.

### 4.3 Market Structure: Ephemeral Hourly Price Prediction Markets

**The 109 buy contracts are NOT 109 different assets.** They are ephemeral, single-use market contracts — one per prediction market — that each live for less than 1 hour.

**Contract lifespan analysis (0x16B1 buy contracts):**

| Metric | Value |
|--------|-------|
| Total unique buy contracts | 109 |
| Active < 24 hours | **109 (100%)** |
| Median lifespan | **47 minutes** |
| Max lifespan | 58 minutes |

Every contract is active for a single day (first_buy = last_buy for all 109). Limitless deploys a new contract for each market (e.g., "BTC above $69,224 on Mar 6 at 15:00 UTC"). The bot enters each one within its ~47 minute window.

**Market format:**
```
"Will {ASSET} be above ${STRIKE} at {DATE} {HOUR}:00 UTC?"

Example: dollarbtc-above-dollar6922402-on-mar-6-1500-utc
         → "Will BTC be above $69,224.02 on Mar 6 at 15:00 UTC?"
```

### 4.4 Asset Coverage

Across all 155 unique markets observed in snapshots:

| Asset | Markets | Strike Range | Example Strike |
|-------|---------|-------------|----------------|
| **XRP** | 31 | $0.134 – $0.143 | $0.1372 |
| **SOL** | 29 | $83.5 – $90.8 | $86.78 |
| **DOGE** | 27 | $0.0089 – $0.0095 | $0.00908 |
| **ETH** | 27 | $1,949 – $2,092 | $1,989 |
| **BTC** | 25 | $67,764 – $71,266 | $69,224 |
| ONDO | 2 | ~$0.027 | — |
| XMR | 2 | $332 – $369 | — |
| HBAR, BCH, PAXG, ADA, AVAX, BNB, WLFI | 1 each | — | — |

The **top 5 assets (XRP, SOL, DOGE, ETH, BTC) account for 90%+** of all market activity.

### 4.5 Hourly Market Grid

Limitless creates markets on a systematic grid: **3 strike levels per asset per resolution hour**, structured as below/near/above the current spot price:

```
BTC markets resolving at 15:00 UTC on Mar 6:
  Strike $68,466  (below spot — YES-favored, cheap NO)
  Strike $69,224  (near spot — ~50/50)
  Strike $69,849  (above spot — NO-favored, cheap YES)
```

During active hours (11:00–18:00 UTC), this creates **~16 markets per hour** across 6 assets:

| Resolution Hour (UTC) | Markets | Assets |
|-----------------------|---------|--------|
| 11:00 | 16 | 6 |
| 12:00 | 16 | 6 |
| 13:00 | 11 | 6 |
| 14:00 | 15 | 5 |
| 15:00 | 16 | 6 |
| 16:00 | 16 | 6 |
| 17:00 | 16 | 6 |
| 18:00 | 16 | 6 |
| Off-hours (00–10) | 1–6 | 1–4 |

The bot systematically enters **18–23 new markets every day**, placing 60–167 buy orders per market in its ~47 minute window (a buy every 17–47 seconds).

### 4.6 The Business Model

This is a **volume game exploiting structural inefficiency in thin prediction markets:**

1. Limitless creates hourly price prediction markets across 5+ crypto assets
2. Thin liquidity means YES + NO often prices below $1.00 combined
3. The bot systematically buys both sides whenever combined cost < $1.00
4. At resolution, one side always pays $1.00 → guaranteed profit
5. Fleet wallets handle high-volume redemption (46,513 redeems)
6. Small edge per trade ($0.02–$0.17) × thousands of markets = $80k+ in 3 months

### 4.7 Position Allocation Across Wallets

The latest snapshot reveals how positions are distributed across wallets for the same markets:

| Market | 0x16B1 | 0xb5b6 (fleet) | 0x0C05 (fleet) | 0x7836 (fleet) |
|--------|--------|----------------|----------------|----------------|
| ETH > $1,971.28 | YES+NO | YES+NO | YES+NO | YES+NO |
| BTC > $68,446 | YES+NO | YES | YES+NO | -- |
| SOL > $834.64 | -- | YES+NO | YES+NO | YES |
| DOGE > $0.009081 | -- | YES+NO | YES+NO | YES+NO |
| ETH > $1,989.28 | -- | YES+NO | NO only | YES+NO |

Multiple wallets hold positions in the same markets, confirming these are coordinated from the same operator. The primary bot (0x16B1) and fleet wallets often hold identical market exposure.

### 4.8 Both-Sides Position Frequency

| Wallet | Total Markets | Both Sides | Single Side | Both-Sides % |
|--------|--------------|------------|-------------|-------------|
| `0x16B1` | 6 | 5 | 1 | **83%** |
| `0x0C05` | 10 | 7 | 3 | **70%** |
| `0x7836` | 10 | 6 | 4 | **60%** |
| `0xb5b6` | 11 | 6 | 5 | **55%** |

Single-side positions likely represent markets where only one side was available at a favorable price, or where the other side has already been redeemed after resolution.

---

## 5. On-Chain Patterns

### 5.1 Transaction Volume by Method

| Method | Count | % of Total | Primary Contract |
|--------|-------|-----------|------------------|
| `redeemPositions` | 46,513 | 86.1% | ConditionalTokens (`0xC9c9...76e18`) |
| `buy` | 6,642 | 12.3% | 109 unique market contracts |
| `null` (self-interaction) | 440 | 0.8% | Self (smart contract wallets) |
| `approve` | 158 | 0.3% | USDC, ConditionalTokens |
| `receiveMessage` | 105 | 0.2% | Bridge (`0x81D4...`) |
| `matchOrders` | 50 | 0.1% | FeeModule |
| `transfer` | 39 | 0.1% | Various |
| Other | 54 | 0.1% | DEX routers, claims |

The 86:12 ratio of redeems to buys reflects the fleet architecture: one wallet buys across 109 markets, while 10+ wallets redeem the resulting positions.

### 5.2 Contract Interaction Map

| Contract | Name | Txn Count | Purpose |
|----------|------|-----------|---------|
| `0xC9c98965...76e18` | ConditionalTokens | 46,530 | Core prediction market settlement |
| `0x833589fCD6...` | USDC | 193 | Token approvals for trading |
| `0x81D40F21F1...` | AdminUpgradableProxy | 105 | Cross-chain bridge (funding) |
| `0xF94ef76088...` | FeeModule | 50 | Order matching infrastructure |
| `0x4409921Ae4...` | DexRouter | 7 | DEX swaps (occasional) |
| `0x9EadbE35F3...` | LMTS | 4 | Limitless token interactions |

### 5.3 Hourly Trading Patterns

Transaction volume by hour (UTC), all wallets:

```
Hour  |  Count  |  Distribution
------+---------+---------------------------------------------
00:00 |   2,015 |  ####################
01:00 |   2,430 |  ########################
02:00 |   2,465 |  ########################
03:00 |   2,344 |  #######################
04:00 |   2,271 |  ######################
05:00 |   2,239 |  ######################
06:00 |   2,162 |  #####################
07:00 |   2,116 |  #####################
08:00 |   1,762 |  #################
09:00 |   1,996 |  ###################
10:00 |   2,066 |  ####################
11:00 |   1,699 |  ################        <-- daily low
12:00 |   2,033 |  ####################
13:00 |   2,239 |  ######################
14:00 |   2,012 |  ####################
15:00 |   2,420 |  ########################
16:00 |   2,627 |  ##########################
17:00 |   2,792 |  ###########################
18:00 |   2,188 |  #####################
19:00 |   2,412 |  ########################
20:00 |   2,190 |  #####################
21:00 |   3,195 |  ###############################  <-- daily peak
22:00 |   2,366 |  #######################
23:00 |   1,962 |  ###################
```

**Key observations:**
- **Peak activity:** 21:00 UTC (3,195 txns) -- corresponds to US afternoon trading hours
- **Secondary peak:** 16:00--17:00 UTC (2,627--2,792 txns) -- European close / US market open
- **Lowest activity:** 11:00 UTC (1,699 txns) -- US pre-market hours
- **24/7 operation:** No hour falls below 1,699 txns, confirming fully automated bot operation

**0x16B1 Buy-Specific Hourly Pattern:**

```
Hour  |  Buys  |  Distribution
------+--------+---------------------------------------------
04:00 |   382  |  ###################      <-- peak buying
16:00 |   455  |  ######################   <-- highest buying
17:00 |   409  |  ####################
01:00 |   344  |  #################
05:00 |   340  |  #################
13:00 |   340  |  #################
08:00 |   173  |  ########               <-- lowest buying
14:00 |   149  |  #######                <-- second lowest
23:00 |     1  |                         <-- near zero
```

The bot concentrates buying during 04:00 and 16:00--17:00 UTC, likely corresponding to prediction market resolution cycles and new market creation windows.

### 5.4 Day-of-Week Distribution

| Day | Transactions | Notes |
|-----|-------------|-------|
| Monday | 8,597 | High volume |
| Tuesday | 5,782 | Moderate |
| Wednesday | 5,781 | Moderate |
| **Thursday** | **13,133** | **Highest -- 2.3x average** |
| Friday | 9,338 | High |
| Saturday | 4,711 | Lowest |
| Sunday | 6,659 | Moderate |

Thursday's 13,133 transactions (24.3% of total) is anomalous and may reflect a specific market event or batch processing cycle. Weekend activity (Saturday at 4,711) is notably lower but still substantial, confirming 7-day operation.

### 5.5 Transaction Status

| Status | Count | Percentage |
|--------|-------|------------|
| `ok` | 52,390 | **97.02%** |
| `error` | 1,556 | 2.88% |
| `null` | 55 | 0.10% |

**Error concentration by wallet:**

| Wallet | OK | Error | Total | Error Rate |
|--------|-----|-------|-------|-----------|
| `0x16B1` | 8,475 | 1,525 | 10,000 | **15.25%** |
| `0x8fdf` | 4,574 | 30 | 4,604 | 0.65% |
| `0x7d89` | 856 | 1 | 857 | 0.12% |
| All others | 38,485 | 0 | 38,485 | **0.00%** |

Errors are overwhelmingly concentrated in `0x16B1` (97.7% of all errors), which is expected given its aggressive buy strategy. The redemption fleet operates with a perfect 100% success rate -- redemptions are deterministic operations that either succeed or are not attempted.

### 5.6 Bridge Funding Coordination

Bridge deposits from `0x81D40F21` (AdminUpgradableProxy) arrive in clearly coordinated batches. A representative batch from February 21, 2026:

```
2026-02-21 11:37:29  0x9F997F  nonce=747
2026-02-21 11:39:09  0x51c8ad  nonce=674
2026-02-21 11:39:55  0x7836F6  nonce=585
2026-02-21 11:41:39  0x0C0540  nonce=533
2026-02-21 11:44:05  0x2C18e1  nonce=574
2026-02-21 11:44:43  0x9477eb  nonce=804
2026-02-21 11:51:13  0x4CDDEE  nonce=848
2026-02-21 11:51:37  0x24098e  nonce=730
2026-02-21 11:55:17  0xb5b66a  nonce=861
2026-02-21 11:56:41  0xBB7e70  nonce=758
```

All 10 wallets funded within a 19-minute window, in a consistent order that repeats across all funding events. This pattern repeated at least **10 times** between Feb 18 and Mar 5, with 3--10 wallets per batch.

**Funding Frequency:** Approximately every 2--4 days, suggesting periodic capital injection to support ongoing position settlement.

### 5.7 Redemption Cadence

Fleet wallets execute redemptions at machine speed:

| Metric | Value |
|--------|-------|
| Median inter-tx time | **2.0 seconds** |
| Mean inter-tx time | ~360 seconds (includes overnight gaps) |
| Minimum gap | 0.0 seconds (same block) |
| 95th percentile gap | ~3,460 seconds (~58 min) |
| Max daily redemptions per wallet | 438 |
| Avg daily redemptions per wallet | ~220 |

The 2-second median confirms automated sequential execution. Multiple redemptions landing in the same block (0.0s gap) suggests the bot batches transactions when many positions resolve simultaneously.

**0x16B1 Buy Cadence:**

| Metric | Value |
|--------|-------|
| Median inter-buy time | **18 seconds** |
| Mean inter-buy time | 63.6 seconds |
| Minimum gap | 2 seconds |
| 5th percentile | 2 seconds |
| 75th percentile | 50 seconds |
| 95th percentile | 193 seconds |

The primary bot fires a buy every 18 seconds (median), with burst periods as fast as every 2 seconds when multiple opportunities align.

---

## 6. Gas and Cost Analysis

### 6.1 Total Gas Expenditure

| Wallet Type | Total Gas (ETH) | Approx USD | % of Total |
|-------------|----------------|-----------|-----------|
| 0x16B1 (Primary) | 0.068277 ETH | ~$170.69 | 66.8% |
| Fleet (8 wallets) | 0.018451 ETH | ~$46.13 | 18.1% |
| Other wallets | 0.015482 ETH | ~$38.71 | 15.1% |
| **Total** | **0.102210 ETH** | **~$255.53** | **100%** |

*USD estimates based on ~$2,500/ETH*

### 6.2 Gas Efficiency by Method

| Method | Avg Gas Used | Avg Fee (wei) | Cost per Tx (uETH) |
|--------|-------------|--------------|-------------------|
| `redeemPositions` | 89,551 | 754B | 0.75 |
| `buy` (success) | 198,204 | 9.4T | 9.4 |
| `buy` (revert) | 49,222 | ~2.4T | ~2.4 |
| `approve` | 53,812 | 8.9T | 8.9 |
| `receiveMessage` | 156,763 | 1.3T | 1.3 |
| `matchOrders` | N/A (contract) | 50.4T | 50.4 |

Redemption transactions are the cheapest operation at 0.75 uETH average, explaining why the operation distributes redemptions across many wallets -- the marginal cost of an additional wallet's redemptions is negligible.

### 6.3 Cost-Profit Analysis

| Metric | Value |
|--------|-------|
| Total Profit (all wallets) | $186,550 |
| Total Gas Cost | $255.53 |
| Gas as % of Profit | **0.137%** |
| Profit per ETH of gas | **$1,825,044** |
| Avg cost per profitable trade | ~$0.005 |

The gas-to-profit ratio is extraordinarily favorable. Base L2's low transaction fees make this arbitrage strategy viable at scales that would be uneconomical on Ethereum mainnet.

---

## 7. Risk Analysis

### 7.1 Drawdown Profile

| Wallet | Max Drawdown | Final PnL | DD/PnL Ratio | Worst Day |
|--------|-------------|-----------|-------------|-----------|
| 0x16B1 | $1,391 | $79,736 | **1.7%** | -$1,002 |
| 0x8fdf | $1,141 | $64,209 | **1.8%** | -$1,052 |
| 0xb5b6 (fleet) | $1,107 | $5,316 | 20.8% | -- |
| 0x7836 (fleet) | $3,157 | $882 | **357%** | -- |
| 0xC492 | $3,343 | -$276 | N/A (negative) | -- |
| 0x48b5 | $3,413 | -$1,929 | N/A (negative) | -- |

The top two wallets demonstrate exceptional risk management with drawdown-to-PnL ratios under 2%. Fleet wallets show higher relative volatility due to smaller absolute PnL, with 0x7836 experiencing a drawdown 3.5x its final profit.

Two wallets (0xC492 and 0x48b5) are net negative, with drawdowns exceeding $3,000 -- these may represent experimental wallets or wallets caught in unfavorable market conditions.

### 7.2 PnL Consistency (0x16B1)

| Metric | Value |
|--------|-------|
| Profitable days | 78 of 95 (82%) |
| Losing days | 17 of 95 (18%) |
| Avg profitable day | $1,107 |
| Avg losing day | -$232 |
| Profit factor | 4.77x (avg win / avg loss) |
| Win/loss ratio | 4.59:1 (by count) |
| Longest win streak | -- |
| Max consecutive losses | -- |

The 4.77x profit factor combined with 82% win rate produces an extremely favorable expected value per day.

### 7.3 Identified Risks

1. **Market Efficiency Risk:** As more arbitrageurs enter the Limitless market, spreads will compress. The recent PnL flattening of 0x8fdf (from $64,212 to $64,209 over 2 days) may be an early signal.

2. **Smart Contract Risk:** All profits flow through the ConditionalTokens contract (`0xC9c9...`). A bug or exploit in this contract could freeze or drain funds.

3. **Liquidity Risk:** The bot's 22.2% revert rate on buys indicates frequent competition for available liquidity. As market depth decreases or competition increases, more buys will fail.

4. **Concentration Risk:** 77% of total PnL ($143,945) comes from just 2 wallets. Compromise or disruption of either would significantly impact the operation.

5. **Bridge Risk:** Fleet wallets depend on cross-chain bridge funding via `0x81D40F21`. Bridge exploits or delays could disrupt the redemption pipeline.

6. **Regulatory Risk:** Automated market manipulation or arbitrage at this scale may attract regulatory attention, particularly if Limitless markets are classified as derivatives.

---

## 8. Conclusions and Implications

### 8.1 Professional Firm Assessment

**Verdict: Almost certainly a professional or semi-professional quantitative operation.**

**Evidence for professional operation:**

| Signal | What It Tells Us |
|--------|-----------------|
| 8 wallets with synchronized nonces at ~1,737,000 | Programmatic deployment from a single controller — not manual wallet creation |
| Cross-chain bridge coordination (0x81D40F21) funding 10 wallets in 19-min batches | Infrastructure pipeline with scripted capital deployment |
| 119,193 lifetime txns on primary bot | No human does this manually — this is a dedicated trading bot |
| Fail-fast gas pattern (49K gas on reverts vs 198K on success) | Contracts engineered to revert cheaply — professional smart contract design |
| Separation of buying (1 wallet) vs redemption (8+ wallets) | Architectural separation of concerns to maximize throughput and avoid nonce contention |
| 24/7 operation across all hours with no gaps | Cloud-hosted bot, not a laptop script |
| $680K+ annualized on $255 total gas | Capital-efficient operation optimized for Base L2 economics |
| 18–23 new markets entered daily across 5 assets | Systematic, not opportunistic — scanning every available market |

**What kind of firm:**

Likely a **small quant shop (2–5 people) or a sophisticated solo operator** running prediction market arbitrage as one strategy. The fleet architecture (1.7M nonces per wallet) suggests they've been running automated systems on Base chain well before the Limitless opportunity — these wallets have deep operational history. The operation is too systematic for a retail trader but too niche for a large fund.

The 22% buy error rate is a deliberate engineering choice: submit first, accept reverts. The gas cost of a revert ($0.006) is trivial compared to the edge captured ($0.02–$0.17/share). This is a latency-optimized strategy where speed beats precision — characteristic of professional market-making operations.

### 8.2 Operation Sophistication

This is a **professionally engineered arbitrage operation** with the following hallmarks:

- **Dedicated infrastructure**: Separate wallets for buying, redeeming, and settlement, minimizing nonce contention and maximizing throughput.
- **Automated 24/7 execution**: Sub-20-second buy cadence and 2-second redemption cadence across all hours and days.
- **Aggressive risk management**: 82% win rate with only 1.7% drawdown-to-PnL ratio on the primary wallet.
- **Capital efficiency**: $186,550 profit on $255 in gas costs -- a 730:1 return on infrastructure costs.
- **Scalable architecture**: The fleet pattern (8+ redemption wallets processing 200+ redeems/day each) can be expanded simply by deploying additional wallets.
- **Market coverage**: Systematically trades every hourly crypto price prediction market on Limitless — 18–23 ephemeral markets/day across BTC, ETH, SOL, DOGE, XRP with 3 strike levels per asset per hour.

### 8.3 Revenue Scale

| Period | Revenue Rate |
|--------|-------------|
| Per day (0x16B1 avg) | $857 |
| Per week (0x16B1 avg) | $5,996 |
| Per month (estimated) | $25,700 |
| Annualized run rate | $312,000 |
| All wallets annualized | $680,000+ |

### 8.4 Competitive Landscape Indicators

- The 22.2% buy revert rate suggests **active competition** for arbitrage opportunities -- other bots are likely competing for the same mispriced markets.
- The diversification across 109 market contracts indicates the bot **does not rely on a single market** but systematically scans all available markets.
- The fleet expansion on Feb 18, 2026 (8 new wallets deployed simultaneously) suggests the operator identified scaling opportunities and deployed additional infrastructure.

### 8.5 Replication Considerations

For those looking to replicate this strategy, the key requirements are:

1. **Real-time market monitoring**: Detect when `price_YES + price_NO < 1.00` across all Limitless markets.
2. **Fast execution**: Median 18-second buy cadence; slower bots will be front-run.
3. **Multi-wallet architecture**: Separate buy and redeem operations to avoid nonce bottlenecks.
4. **Error tolerance**: Accept 20%+ revert rates as a cost of aggressive execution.
5. **Base L2 deployment**: Gas costs on Base make this viable; Ethereum mainnet gas would erode margins.
6. **Cross-chain funding pipeline**: Bridge USDC from L1 or other chains to sustain fleet operations.

### 8.6 Market Impact Assessment

The operation processes approximately **6,600 buys** and **46,500 redemptions** over 38 days in the observation window. At an estimated $10--50 per buy (based on position sizes in snapshot data), total buy volume is approximately **$66,000--$330,000**. This volume likely represents a **significant fraction of Limitless market liquidity**, potentially impacting market pricing and available arbitrage spreads for other participants.

---

## Appendix A: Full Wallet Address Reference

| Short ID | Full Address | Type |
|----------|-------------|------|
| `0x16B1` | `0x16B1763BD7261190e573a510334Ce9b22673934a` | Primary Trading Bot |
| `0x8fdf` | `0x8fdfa8b2D83b429CaAF3D9d6f0F2865546eB12f1` | Independent Bot |
| `0xb5b6` | `0xb5b66a7aF26744F0431a37Eb7772a6F50f174E0E` | Redemption Fleet |
| `0x2409` | `0x24098eb6E196a8b94B42BD46FdCBF507af651Df6` | Redemption Fleet |
| `0x4CDD` | `0x4CDDEE9263161003aF7F20F919181FFF14713D8E` | Redemption Fleet |
| `0x9477` | `0x9477eb4970dDE9E51bf2edDfA78fD3791B0135bf` | Redemption Fleet |
| `0x2C18` | `0x2C18e153217b1B1C84A57588073A4Fa915116c5C` | Redemption Fleet |
| `0x0C05` | `0x0C05403DBC8cd7558a0907292c8d5F6c032dF157` | Redemption Fleet |
| `0x7836` | `0x7836F635E86B548bD1b245cd159f9318B79b0983` | Redemption Fleet |
| `0x51c8` | `0x51c8ad3fc12a10EBe225A0E66eA69288503Ad739` | Redemption Fleet |
| `0xBB7e` | `0xBB7e70cf7874379Fc4f91CeB2BD3160b98c5DA16` | Independent Redeemer |
| `0x9F99` | `0x9F997F7C146FBC311aA5C1D0FD01b19b814277A5` | Fleet-Adjacent Redeemer |
| `0x7d89` | `0x7d895A4acdd05E65f6340282f25D04A5E7C4BdBd` | Smart Contract Wallet |
| `0x1c19` | `0x1c19c782722C3fBaE474316cd3f8dDf051F6E965` | Legacy Wallet |
| `0xF94e` | `0xF94ef760884b0605E433853Aed17DA574160226E` | FeeModule Contract |
| `0xAeC2` | -- | PnL-only (fleet-era) |
| `0x44bF` | -- | PnL-only (fleet-era) |
| `0xA6Ca` | -- | PnL-only (fleet-era) |
| `0xC492` | -- | PnL-only (fleet-era, negative) |
| `0x48b5` | -- | PnL-only (fleet-era, negative) |

## Appendix B: Key Contract Addresses

| Address | Name | Role |
|---------|------|------|
| `0xC9c98965297Bc527861c898329Ee280632B76e18` | ConditionalTokens | Prediction market settlement |
| `0x81D40F21F12A8F...` | AdminUpgradableProxy | Cross-chain bridge |
| `0x833589fCD6...` | USDC | Settlement currency |
| `0xF94ef760884b...` | FeeModule | Order matching |
| `0x4409921Ae4...` | DexRouter | DEX routing |
| `0x9EadbE35F3...` | LMTS | Limitless token |

## Appendix C: Data Sources

- **Transaction data:** 15 parquet files from `base.blockscout.com`, totaling 54,001 transactions
- **PnL history:** 19 parquet files with 1,682 data points (4-hour granularity)
- **Position snapshots:** Real-time position data captured March 6, 2026 (61 rows across 4 wallets, 15 markets)
- **Observation window:** May 12, 2025 -- March 6, 2026 (broadest), with bulk data concentrated Jan 28 -- Mar 6, 2026

---

*This analysis was produced from on-chain data only. No off-chain communications, API keys, or private information were used. All findings are based on publicly available blockchain transactions on Base L2.*

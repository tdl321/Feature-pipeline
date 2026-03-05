# Identifying & Arbitraging Retail Flow: A Complete Guide

> Compiled from the QuantArb Substack series. All quotes and techniques sourced directly from the articles.

---

## Table of Contents

1. [Identifying Retail / Uninformed Flow](#1-identifying-retail--uninformed-flow)
   - 1.1 Order Size Signals
   - 1.2 Round-Number Clustering
   - 1.3 Directional Imbalance
   - 1.4 TWAP / Algo Detection ("Spotting Idiots")
2. [Toxic vs Non-Toxic Flow Framework](#2-toxic-vs-non-toxic-flow-framework)
   - 2.1 Defining Flow Toxicity
   - 2.2 Measuring Toxicity: Markouts
   - 2.3 Adverse Selection Sources
   - 2.4 Skewing & Inventory Management Based on Toxicity
   - 2.5 The Hitting Machine
   - 2.6 Retail Quoter Game Theory
3. [Cross-Exchange Arbitrage: Exploiting Retail Flow](#3-cross-exchange-arbitrage-exploiting-retail-flow)
   - 3.1 The Core Setup
   - 3.2 Finding Opportunities
   - 3.3 Execution Mechanics
   - 3.4 Hedging
   - 3.5 Risk Management
4. [Market Making & Prediction Markets Against Retail Flow](#4-market-making--prediction-markets-against-retail-flow)
   - 4.1 Market Making on Non-Toxic Venues
   - 4.2 Prediction Market Making (Binary Options)
   - 4.3 Fan Token / Sentiment Plays
   - 4.4 Event-Based Retail Flow
5. [Execution Concealment & Detection](#5-execution-concealment--detection)
   - 5.1 How Institutions Hide as Retail
   - 5.2 Detection Methods
   - 5.3 Trade Data Infrastructure
   - 5.4 Practical Detection Pipeline
6. [Appendix: Key Parameters & Thresholds](#6-appendix-key-parameters--thresholds)
7. [Source Articles](#7-source-articles)

---

## 1. Identifying Retail / Uninformed Flow

### 1.1 Order Size Signals

**Source:** `2023-02-08_using-order-size-for-alpha.md`

#### Variation 1 — Average Order Size (All Volume)

> "We take the quote asset volume, which is effectively the volume in USDT since all our assets are quoted in USDT, and divide by the number of trades. We use the USDT volume instead of the base asset volume to help normalize this metric."

```
Alpha_1(t) = QuoteAssetVolume(t) / NumberOfTrades(t)
```

Both numerator and denominator are measured over a **24-hour (daily bar)** window.

#### Variation 2 — Taker Buy Average Order Size (Retail Proxy)

> "We use the volume of taker buy orders instead of the overall volume since taker buy orders are the most likely order to be used by retail traders. This is a well-known relationship that is often exploited by execution algorithms to hide their flow."

```
Alpha_2(t) = TakerBuyQuoteVolume(t) / NumberOfTrades(t)
```

#### Why Taker Buy Volume Is the Retail Proxy

> "Retail traders tend to prefer market orders and tend to send buy orders more than sell orders. This is a relationship that can be found across many assets such as equities and not just digital assets. HFT orders are more likely to be short-term bets that provide a lot less information about the time horizons we consider here."

Three retail characteristics in one sentence: (1) preference for market orders (taker aggression), (2) directional bias toward buys, and (3) longer time-horizon motivation vs. HFT.

#### Z-Score Normalization — Full Formula Chain

> "It is usually a good idea to take the z-score of our alpha over a rolling basis (5 weeks is an amount I personally like to use). This means that we are now looking at which assets have had a sudden change in average order size instead of ranking assets based on their current average order size."

Rolling window: **5 weeks = 35 days**.

```
Z_Alpha(t) = (Alpha(t) - mean(Alpha, t-35d to t)) / std(Alpha, t-35d to t)
```

> "There will be differences in average order sizes between assets, and these will be persistent, the more interesting information is which assets have seen a recent increase in their order sizes."

#### Volatility Adjustment

> "We adjust for volatility since we want to penalize high-volatility assets. There is a strong positive relationship between the volatility of portfolio constituents and turnover."

```
Final_Signal(t) = Z_Alpha(t) / Volatility_35d(t)
```

This serves two purposes: (1) reduce turnover by penalizing volatile assets, (2) lower total trading costs.

#### Portfolio Construction

> "Our logic here is that if participants on average want to trade more of an asset it is likely a signal to purchase the asset. Retail buy orders increasing shows that they are very interested in this asset all of a sudden."

Signal used in a Ranked Long/Short (RLS) framework: rank all liquid assets on the final signal descending, go long the top asset, short the bottom. Rebalance daily at close.

**Result:** The taker-buy-only variant nearly doubled the Sharpe ratio vs. the general variant.

---

### 1.2 Round-Number Clustering

**Source:** `2023-11-19_hide-n-seek-pt1-avoiding-detection.md`

#### The Core Observation

> "People trade a hell of a lot more 100-size orders than 99 or 101-size orders. Why? Retail traders love whole numbers."

#### Dual Distribution Problem (USD vs. Raw Token)

> "One interesting issue with digital assets is that there is a USD distribution and a raw amount distribution. People will cluster around $1,000 sizes, but also 0.1 BTC sizes. So, we need to work on matching both of these distributions."

You must build **two separate histograms** for any crypto asset:
1. Trade sizes in USD/USDT — spikes at $100, $500, $1,000, $5,000, $10,000
2. Trade sizes in base token units — spikes at 0.1 BTC, 1 BTC, 10 BTC, etc.

#### Detection via Histogram Anomalies

> "You can detect that someone is executing a lot of size if suddenly the range between $3,000 - $4,000 (as an example) orders is massively higher than it should be compared to historical data, but in the same vein, we can tell something is awry if the retail round numbers don't show up in our histogram."

Two anomaly modes:
- **Elevated bin**: a non-round-number range is far above historical baseline — algo executing there
- **Missing round numbers**: retail peaks absent or suppressed — algo-only flow, no retail participation

#### Retail vs. Algo Histogram

| Feature | Retail Histogram | Algo Histogram |
|---|---|---|
| Round-number peaks | Pronounced spikes at $100, $500, $1k, $5k, $10k | Absent or suppressed |
| Token-denomination peaks | Spikes at 0.01, 0.1, 1, 10 BTC | Absent or suppressed |
| Overall shape | Power-law falloff with round-number spikes | Uniform/declining, or concentrated in a specific non-round bin |
| Detection signal | Normal retail behavior | Specific bin anomalously elevated = algo execution |

---

### 1.3 Directional Imbalance

**Source:** `2023-11-19_hide-n-seek-pt1-avoiding-detection.md`

> "They especially love market orders that are in the direction of the overall regime (retail has a much higher imbalance in their flow compared to institutions) so if we are in a bull market, then we will have a pretty low impact with buy market orders."

Key characteristics:
- Retail has **higher buy/sell imbalance** than institutional flow
- Retail directional bias tracks the macro regime: more buyers in bull markets, more sellers in bear markets
- Institutions maintain closer to flow neutrality

Measurement:
```
Imbalance(t) = TakerBuyVolume(t) / (TakerBuyVolume(t) + TakerSellVolume(t))
```

For retail, expect values significantly above 0.5 in bull regimes. Near 0.5 or below in a bull regime signals institutional/algo participation.

---

### 1.4 TWAP / Algo Detection ("Spotting Idiots")

**Source:** `2023-07-17_small-trader-alpha-2-advanced-arbitrage.md` and `2023-12-09_hide-n-seek-pt2-detection.md`

#### Definition of "Idiot" Flow

> "Idiots - Anyone who executes size like an idiot. This includes: using an illiquid exchange instead of Binance, dumping too much size too fast, and using a hilariously obvious execution algorithm."

#### Method 1 — Recurring Size Detection

> "The most basic model would be to test for any sizes that have been recurring. If the size 2000 gets printed every 20 seconds then we know someone is executing an order."

> "I've seen this happen directly in front of me before... It was 1999.53 then 1998.26 (arbitrarily made-up example... but roughly around 2k and every 20s)"

Implementation:
- Filter trade feed to orders above a minimum size threshold
- Group by size bucket (e.g., $2,000 +/- $50)
- If a specific bucket recurs with high frequency, flag as TWAP

Verify the **time schema**:
> "We want to make sure that we figure out the time schema - this then requires us to see if there is equal spacing (either in the time or volume spectrum depending on whether we are detecting TWAP or VWAPs) between the orders."

#### Method 2 — Binned Repetition

> "This should be enough to detect any large TWAP or VWAP orders, but of course, people tend to be more complicated with their approaches and the binning approach still requires a fair bit of parameter tuning."

Parameters to tune: bin width, lookback window, anomaly threshold.

#### Method 3 — Non-Uniform FFT (NUFFT)

> "We need to use the Non-Uniform Fast Fourier transform instead of normal FFT due to the non-uniformity of our trade data time series. The trades do not arrive at regular intervals and thus we must adapt to this."

**Library:** `pynufft` — https://jyhmiinlin.github.io/pynufft/

A TWAP executing every 30 seconds creates a spike at 1/30 Hz in the frequency domain. Even after randomization, if not sophisticated enough, harmonic structure remains visible.

Spread-based signal amplification:
> "Spreads will widen after a large impact... by looking at where spreads are relative to the global order book, we can detect if it is unusually high and also work with a much more normalized metric for applying Fourier too."

Apply NUFFT to both raw trade timing AND the **spread time series** as a normalized proxy.

#### Method 4 — Global vs. Local Orderbook Comparison

> "This is likely one of the best methods for detecting the execution of large sizes. We simply compare the global order book and the local order book of an exchange."

Multi-dimensional comparison:
1. **Volume comparison**: local exchange volume vs. global aggregated volume
2. **Orderbook depth**: local book depth vs. global at each price level
3. **Net volume imbalance**: directional breakdown
4. **Order size quantile breakdown**: volume by size quantiles as % of total volume

> "Going beyond volume, we can also look at net volume imbalance or break it down by order size quantiles (adjusted to be a % of volume - use with the previously mentioned volume comparison so that we don't adjust away the recently high volume from our results)."

#### BBA Spread Widening as Signal

> "A large BBA spread should suggest that liquidity is being taken aggressively anyways, so this is a good sign to look deeper."

Elevated local BBA spread relative to global/historical normal is an initial trigger. Then apply more detailed methods.

#### Empirical Distribution of Trade Offsets from Midprice

> "We take the bps offset for each trade over some lookback window and plot the empirical distribution. We align quote data with trade data and then calculate the midprice prior to each trade."

```
Offset_bps(trade_i) = (trade_price_i - midprice_prior_to_i) / midprice_prior_to_i * 10000
```

Uses:
- **Limit order placement optimization**: loop over offsets, compute expected APR at each
- **TWAP detection**: look at past few hours for multiple wide fills
- **Size capping** (mandatory): cap order sizes to your capital budget
- **Recency weighting**: preferentially weight more recent data points

---

## 2. Toxic vs Non-Toxic Flow Framework

### 2.1 Defining Flow Toxicity

**Source:** `2024-08-26_market-making-for-dummies-code-inside.md` and `2025-08-02_advanced-market-making.md`

> "Most people think skewing is about 'managing inventory risk' when in reality the risk of holding some inventory isn't actually THAT big a deal. What actually matters is holding on to toxic inventory, and holding on to lots of it. That's what really sucks."

**Venue-level classification:**
- **Toxic**: Binance (informed flow, arb players, HFT)
- **Non-toxic**: Smaller exchanges (retail flow, uninformed participants)
- **Non-toxic instruments**: Stablecoin pairs (USDC/USDT)

**Aggressiveness and toxicity:**

> "The more aggressive someone becomes, the more likely they are to be a toxic player so you widen your spreads gradually in accordance with their impacts, but of course this doesn't apply in non toxic markets where you know they're basically all non-toxic and this super aggressive player is probably just an idiot who wants size."

---

### 2.2 Measuring Toxicity: Markouts

> "You start by recording your post-fill markouts. This is simply the change in price after all fill. You want to also record your inventory, and then figure out how toxic that inventory is by the loss rate on that inventory."

**Before you have your own data:**

> "You can use the average markout (say over 5 seconds) of every single trade that happens on that instrument (or even exchange) and track the EWMA of that over time. You'll notice that the ones that sustain a really great set of markouts are the super non-toxic pairs."

**Conditional accuracy:**

> "Most people think about the forecasting problem as optimizing for R2/MSE/IC of their model fit when in reality most of the moves you are forecasting are ones you will never get filled on so it doesn't really matter. You want to be forecasting conditional on you getting filled."

> "Say you have a 90% accuracy, but on the fills you get, your forecast is wrong 70% of the time, then in reality you only have a 30% accuracy."

**Fair value defined in terms of markouts:**

> "Fair value is simply a price that when you quote around you get good markouts. Let's say that after 750ms most of your losses tend to realize on your orders, then you need to roughly quote around the price where you expect the market to be at (future mid-price) in 750ms."

**Sizing by markouts:**

> "One advanced trick is to scale your sizing based on your markouts for that symbol so that you are quoting more size on the names that you are making money on. Markouts are fairly responsive as well, so you will be sizing down very quickly when you start losing money."

**Weighting multiple forecast horizons:**

> "A very simple way to do this is to just average their markout curves per second. I.e. if you have a 1s forecast with 2 bps of edge and a 30 bps 1 minute forecast then when you average them that's 0.5 bps per second from the 1 minute forecast and 2 bps per second from your 1s forecast so you should weight the forecast at 20% the 1 minute forecast, and then 80% towards the 1s forecast."

**Interpreting negative markouts:**

> "Just because you had a bad return on that fill, doesn't make it a bad fill. Markouts can decrease and your PnL actually decrease. Why? That's because some fills, whilst costing you money, may take off inventory for less than your reward for putting on inventory normally."

---

### 2.3 Adverse Selection Sources

> "In crypto, you have Bitcoin on 400+ exchanges... So because of this you end up with about 50% of your adversity cost coming from arbitrage related pickoffs, or at least the price on your exchange being led by prices on the other."

**OFI (Order Flow Imbalance):**

> "The basics of avoiding adversity start with OFI & keeping prices out of arbitrage bounds."

Feature importance hierarchy:
> "Top of book imbalance, trade volume imbalance, and volatility are all very simple features to get started with. Top of book imbalance is by far the strongest feature."

**Three making scenarios:**
1. Making into informed flow — need to outsmart them
2. Making into uninformed flow — just don't let people outsmart you
3. HFT taking — reacting to events before quotes adjust

---

### 2.4 Skewing & Inventory Management Based on Toxicity

**Non-toxic venues: hold risk, don't skew aggressively**

> "For smaller cap assets you want to hold the risk quite a bit, and not skew too aggressively because that is the only way to be competitive when the flow is non-toxic. Non-toxic flow will decrease the spread to the point where holding risk becomes a requirement."

> "Aggressively working out of those positions will cost you too much and make you uncompetitive. Especially because you'll have to dump on toxic exchanges that won't cut you slack on the spread."

**Toxic venues: skew aggressively**

> "You skew more aggressively when the market is more toxic. If someone wants to put on 50% of my inventory capacity as a position then that's a pretty aggressive trade and they probably know something I don't."

**Simple skewing rules:**

> "You don't need a complex model for skewing. You should roughly follow: 1. Have a global skew across all exchanges. 2. A local skew for each exchange specifically."

> "We typically don't want to skew past our theo (which is the price we would quote assuming we have no inventory)."

**BTC/ETH correlation pickoff lesson:**

> "We were using BTC/ETH correlation to offset each other's positions... This ended up causing big pickoffs because whenever there was information that was BTC specific we would get loads of BTC inventory on and get picked off in terms of markouts against the BTC/ETH pairs spread."

---

### 2.5 The Hitting Machine

> "Often, you'll have what's called a hitting machine that manages your hidden quotes. That is to say the prices you are willing to trade at but are not posting in the book."

**Using non-toxic exchanges to dump toxic inventory:**

> "If we are market-making on a highly non-toxic exchange, likely one of the smaller exchanges, then we can take against the other market makers on that exchange as a way to dump toxic inventory accumulated on bigger exchanges using our hitting machine."

**Locking positions into arb book:**

> "Arbitrages may manifest because another MM has too much risk on one exchange. If we don't have much risk on that exchange, then we happily take the inventory off their hands at a great premium, locking the position into our arbitrage book."

**Full-maker arbitrage — the ideal form:**

> "Say I quote around the Binance mid-price on another exchange which often falls out of line with Binance, then one side of my quotes will be aggressive when the other exchange diverges from Binance and we will pick up inventory that bets on the convergence. This is actually the final form of arbitrage as it is the lowest cost way to pull off an arbitrage."

> "So when you hear people say 'just quote around Binance mid on a crappy exchange' what they are really saying is 'just do full maker arbitrage against Binance'."

---

### 2.6 Retail Quoter Game Theory

> "Often when you are taking, there will be people who quote a fairly relaxed amount of skew because it's a small exchange and they expect that it'll be almost entirely retail flow. I won't name names but there is a certain market maker where if you pick them off too much they switch on the smart quoter which you will never be able to pick off, but if they don't get picked off too much they'll stick with the retail quoter."

> "So keeping your PnL under a threshold is actually to your own benefit in this case."

**Rebate structures — how retail pays for them:**

> "This rebate typically gets charged to retail via an increase in maker fee, although on some exchanges, the exchange simply takes the cost."

> "The requirements to stay on the top of the book to earn rewards discourages skewing too much so you'll often be able to take against them and put on arbitrage positions at very very reasonable costs."

---

## 3. Cross-Exchange Arbitrage: Exploiting Retail Flow

### 3.1 The Core Setup

**Source:** `2023-07-03_small-trader-alpha-another-real-strategy.md` and `2025-06-20_ultimate-crypto-arbitrage-guide.md`

> "We start by defining our liquid and illiquid exchanges. The liquid exchange is where we get our fair value from and where we reconcile our mispricing against. The illiquid exchange is where we get our alpha; this is the mispriced exchange due to imbalanced supply and demand, moving prices into a mispricing."

**Canonical example:**
- Liquid Exchange: Binance (fair value)
- Illiquid Exchange: Poloniex (alpha source)

**Why low-cap tokens are the sweet spot:**

> "These opportunities tend to appear in low market cap tokens as they have worse capacity, borrowing ability, and risks meaning they get ignored - leaving bread crumbs on the table. Very profitable breadcrumbs."

> "The bulk of the money that you can actually make without a large team comes from trading small exchanges vs big exchanges, the stuff that is small enough to get ignored by the best of the best."

**Two arb types:**

**Type 1 — Midprice Arbitrage (taker-taker):** Midprices are severely misaligned. Quick but risky — reverts before transfer arrives.

**Type 2 — Spread Arbitrage (maker illiquid / taker liquid):** The preferred type.

> "Our second type of arbitrage is much better, the favorite. It occurs because whoever has been executing their orders has not properly distributed them in accordance with where the exchange liquidity is. We are effectively transferring liquidity from where it is (Binance) to where it is needed (Poloniex)."

**Convergence asymmetry:**

> "ShitEx will do almost all of the moving, which we can round to 100% of the moving... Especially if you are finding edges on smaller exchanges, the small exchange will do all the moving and Binance won't budge an inch."

**Execution flow (Buy_Illiquid — preferred):**
1. Post limit buy at top of book on illiquid exchange
2. Once filled, transfer asset to liquid exchange
3. Taker sell on liquid exchange
4. Transfer USDT back (or keep 2x capital to avoid waiting)

> "We expect that convergence will only come from the illiquid exchange moving towards the liquid exchange, so buying on the illiquid exchange is less risky than selling."

---

### 3.2 Finding Opportunities

**Source:** `2025-09-22_finding-arbitrage-opportunities.md`

#### Wash Flow Detection

**Method 1 — OI/Volume mismatch:**
> "One easy inspection for wash flow is checking that open interest and volume match. Often they only bother to spoof volume and not open interest."

**Method 2 — Inside-spread fills:**
> "On exchanges with wash flow you'll see the % of orders filling inside the spread above 10-20%, sometimes even at 90%+. It's usually less than a single % on reputable exchanges."

**Method 3 — DNS-based detection:** Referenced @0xLoris tweets.

#### Internalized Flow Exchanges

> "When all the flow is super uninformed it's very profitable to simply take the flow on yourself instead of letting others MM it, and hence some exchanges will do this. MEXC for example simply shut off their futures API for 9 months because they didn't like the flow."

> "There was one exchange I used to trade on where we were >80% of the taker volume for a period and were just aggressively picking off their internal MM."

#### Geographic Constraints

> "Korean exchanges... tend to have really good opportunities and the top names with all the opportunities pretty much never approve institutional accounts (mostly because they're the market maker on the exchange) so if you have a Korean passport and can create an account you can have access to this exchange and pick them off."

#### New Exchange / New Listing Opportunities

> "It's usually on newer exchanges where you find the best arbitrage opportunities."

> "Integrate lots of DeFi exchanges, even ones with AMMs - the shittier the better, and you'll find tons of arbs. Especially, on the Solana exchanges."

#### VC Edge

> "Back at a previous firm of mine we were raking in 6 figures every single month... on an extremely high Sharpe simply from marking up a leading exchange by a fixed spread and quoting it. We were 90%+ of the volume and had been there largely because we started quoting when the exchange was in its infancy due to our VC investment."

Integration stipend range: **$5k-$75k/month**.

---

### 3.3 Execution Mechanics

#### Fixed vs Variable Costs — The Transfer Threshold Problem

Fixed costs (do NOT scale): Withdrawal fees (e.g., Binance 1 USDT flat). On $1,000 = 10 bps. On $10,000 = 1 bp.

**Worked example:**
> "Say we leave our time to get filled fixed at 1h if we can get filled on $10,000 on a 50 bps arb during this time. We discount by 5 bps for market price risk, say it is a sell trade, so we also discount another 10 bps because of convergence risk, and then finally, we take off transfer costs of 1 bp for the USDT and withdrawal costs of 10 bps for the underlying asset. We have a risk-adjusted return of 24 bps per hour."

#### Capacity Estimation

> "Our first step is to split buy & sell trades. If we are selling, we want to see who is buying. It isn't always 50/50, and we can often see 80%+ of the volume be in one direction, hence causing the arbitrage."

> "Start by assuming you can take half the flow on the buy/sell side of volume, then work from there."

#### Going Deeper — Optimal Offset Calculation

Three orderbook zones: tight liquidity (near midprice), monster levels (deep, concentrated), deep liquidity.

**Causes of wide arbs to classify:**
- Risk Premium — volatile conditions
- **Idiots** — large orders TWAPing into illiquid book
- Shit^2Coin — too illiquid for anyone to bother
- Random wide fills — thin book

> "If the reason is idiots, then we will want to go deeper."

#### Monster Levels

> "A market maker may, for example, be required to place 100k worth of liquidity at a 1% spread, as well as another 100k at a 3% spread on both sides of the book. Most market makers earn their profit from the tokens/options they receive for this service."

> "We can place our orders 1 tick tighter than these monster orders and hog all the flow for only a marginally lower spread without much chance that we will end up in a bidding war."

#### Diming / Price Improvement

> "Placing tighter than BBA to receive more flow can drastically improve the performance of the strategy, but it can also send the performance straight to 0. This is because of bidding wars."

De-escalation: place at BBA. If joining from back of queue anyway, "test the market" by price improving 1 tick and immediately de-escalating if followed.

#### Both Sides of the Book

> "If I have part of my order filled and the spreads are so wide on both sides of the book... then it can often make sense to place on both sides of the book using our inventory."

#### Triangular Arbitrage (from Small Trader Alpha #3)

- Make on the most illiquid leg (COIN/USDT)
- Exit via triangular path: COIN -> BTC/ETH/BNB -> USDT

**Gatling Gun trick**: Send 5 identical orders simultaneously to exploit latency jitter.

> "Sending (usually I find 5 is optimal) multiple orders at once... The reason it improves latency is because your latency is not deterministic."

---

### 3.4 Hedging

**Default: no hedge**

> "Hedging costs money and that's something that you'll find is really scarce when running arbitrages. Every basis point of edge counts, you are really skimming very fine lines, and hedging is a whopping DOUBLE the cost!"

> "Running this in production will lead you to the answer that the best profits come with a bit of risk. This risk is quantified, factored in, and then averages out very nicely because our ROI is so high."

**Futures/perp hedging** — cheapest but rarely available for illiquid tokens.

**Spot borrow** — expensive but relative to arb size often acceptable. Hold borrow open between trades to avoid losing it.

**Correlated asset hedging** — explicitly discouraged:
> "I don't really think this one is a good solution because we are hedging out the arbitrage converging... This can only really be done by hedging in the same asset."

**Exception: funding arb always requires hedging** — long holding periods mean market moves drown funding PnL.

---

### 3.5 Risk Management

**Transfer risk**: Check withdrawals are enabled via API (can be incorrect). Wallet whitelisting for hack mitigation.

**Counterparty risk**: Accepted because ROI is high enough to absorb the tail.

**Early convergence risk**: Estimate by looking at arb persistence over recent hours. Buying on illiquid exchange is lower risk (locks in the illiquid price first).

**Market risk**: Discount via dirty 1h/24h price change method. Don't arb LUNA at 10% if it's cratering at -30%/hour.

**Cross-exchange leverage/liquidation risk**:
> "A lot of people end up blowing their books up because the market moves 15% down and suddenly they're down 2 mil on one exchange and up 2 mil on another exchange."

---

## 4. Market Making & Prediction Markets Against Retail Flow

### 4.1 Market Making on Non-Toxic Venues

**Source:** `2023-08-13_levering-to-the-tits-dead-simple.md` and `2025-08-02_advanced-market-making.md`

**Stablecoin pairs as the "Treasury Bill of Market Making":**

> "Stablecoin markets under normal conditions are almost entirely retail flow which means you're looking at the treasury bill of market making. The flow isn't very toxic, and the spreads are almost always the same."

Returns: 8 round trips/day at 1 bps each = **29.2% annualized** without leverage. **Over 200%** with 10x leverage.

**Quote around Binance mid on crappy exchanges = full maker arb:**

> "So when you hear people say 'just quote around Binance mid on a crappy exchange' what they are really saying is 'just do full maker arbitrage against Binance'."

**Implementation**: Hummingbot for simple BBA quoting logic.

**When to shut off**: When spreads widen beyond 1-tick (volatility/depeg events).

---

### 4.2 Prediction Market Making (Binary Options)

**Source:** `2024-04-06_small-trader-alpha-5-market-making.md`

> "The flow on these bets are almost exclusively retail, but more importantly we can actually use option prices for the real tradfi markets on these assets to figure out the implied probability of outcomes."

**Binary Option Pricing (Black-Scholes):**

```python
import numpy as np
from scipy.stats import norm

def binary_option_price(S, X, T, r, sigma, option_type='call'):
    d1 = (np.log(S / X) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'call':
        price = np.exp(-r * T) * norm.cdf(d2)
    else:
        price = np.exp(-r * T) * norm.cdf(-d2)
    return price
```

**Implied Volatility Extraction (Newton-Raphson):**

```python
def bs_call_price(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

def bs_vega(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * np.sqrt(T) * norm.pdf(d1)

def find_implied_volatility(S, K, T, r, market_price, sigma_estimate=0.2, tolerance=1e-5, max_iterations=100):
    sigma = sigma_estimate
    for i in range(max_iterations):
        price = bs_call_price(S, K, T, r, sigma)
        vega = bs_vega(S, K, T, r, sigma)
        if abs(vega) < 1e-10:
            break
        price_difference = market_price - price
        sigma += price_difference / vega
        if abs(price_difference) < tolerance:
            return sigma
    raise ValueError("Implied volatility not found")
```

> "It's best to use py_vollib_vectorized in my experience since that's faster, and more robust."

**Cross-venue arbs**: Same outcome priced differently on two prediction platforms. Purely mechanical, no pricing model needed — just verify identical settlement terms.

---

### 4.3 Fan Token / Sentiment Plays

**Source:** `2023-03-01_a-novel-approach-to-frontrunning.md`

**The phenomenon**: Sports fan tokens (CITY, FCB, JUV) collapse in price before match dates.

**Two hypotheses:**
1. Market makers reduce inventory before binary event
2. Fans sell tokens to bet on sportsbooks instead

**Strategy**: Short fan token day before match, hedge long BTC. Close at end of match day.

**Results**: ~80% return on CITY without leverage. Capacity ~$100k-$200k per trade.

> "It costs about 1% to execute 50k on CITY."

---

### 4.4 Event-Based Retail Flow (The CPI Trade)

**Source:** `2024-10-17_real-strategies-the-cpi-trade.md`

> "A lot of stupid effects occur because of stupid flows. In this case, it was lots of CPI flows."

**The alpha signal:**
```python
alpha = np.sign(actual - forecast) * -1
```

**Applicable events**: CPI, GDP, ISM PMI, NFP, PCE, PPI, Unemployment Rate.

**Four strategies:**

1. **Initial Shock** (3s latency, exit at 15s) — taker, spread is the biggest cost
2. **Reversal via Limit Orders** — retail momentum chasers fill your limits on the reversal
3. **Momentum Extension** — if 10s move confirms direction, hold for 60s+
4. **Lead-Lag Cross-Exchange** — slower exchanges (DYDX) lag Binance by many seconds

**Latency engineering:**
> "We make 1000s of email accounts and sign up to receive the emails from BLS, this dramatically lowers the latency."

**Connection to retail flow:**
> "The reversal is harder to capture... Luckily, you can get filled very easily because of all the retail flow. Post limit orders, get the fill."

---

## 5. Execution Concealment & Detection

### 5.1 How Institutions Hide as Retail

**Source:** `2023-11-19_hide-n-seek-pt1-avoiding-detection.md`

Three dimensions of concealment:

1. **Order Size** — match the USD + raw token dual distribution including round-number peaks
2. **Order Frequency** — randomize but within bounded execution window
3. **Aggression Level** — match the market's distribution across taker, PI, BBA, and passive levels

> "Our goal here is to match the market distribution for aggression so that even if we create an imbalance in the volume overall, it will not be clear that all of the orders are from one participant."

**Volume seasonality**: Execute during peak volume windows. A shift in the hourly volume profile vs. historical baseline is a detection signal.

**Buy/sell imbalance matching**: Institutions must match the regime-appropriate imbalance (heavy buys in bull markets).

**Self-testing framework:**
> "The best way to build the stealthiest execution algorithm is to be your own enemy. Trying to detect your own algorithm and then learning lessons from this should be the research process behind a strong algorithm."

---

### 5.2 Detection Methods (Flipping the Script)

| Characteristic | Real Retail | Disguised Institutional |
|---|---|---|
| Order sizes | Round-number clusters ($100, $1k; 0.1 BTC) | Non-round bins elevated, or round peaks missing |
| Frequency pattern | Irregular, human-driven | May show TWAP/VWAP regularity |
| Aggression mix | Mixed taker/maker | Single order type dominant |
| Buy/sell ratio | Directional with regime | Artificially balanced or counter-regime |
| Intraday timing | Follows volume seasonality | Deviates from baseline hourly profile |
| Spread behavior | No systematic widening | Periodic widening after certain sizes |

**Detection pipeline in order of complexity:**
1. Recurring size + time spacing (simple)
2. Binned repetition analysis (moderate)
3. NUFFT periodicity detection (advanced)
4. Global vs. local orderbook divergence (advanced, "likely one of the best methods")
5. Spread widening analysis (complementary)
6. Inside-spread trade rate (wash flow specific)

---

### 5.3 Trade Data Infrastructure

#### Binance Futures aggTrades

```python
import pandas as pd
import requests
from tqdm import tqdm
from datetime import datetime as dt, timedelta

binance_futures_base_url = "https://fapi.binance.com"
binance_futures_agg_trades = "/fapi/v1/aggTrades"

ts_24h_ago = int((dt.now() - timedelta(days=1)).timestamp() * 1_000)
symbol = "BTCUSDT"

r = requests.get(
    f"{binance_futures_base_url}{binance_futures_agg_trades}"
    f"?symbol={symbol}&startTime={ts_24h_ago}&limit=1000"
).json()

start_id = r[-1]['a'] + 1
df = pd.DataFrame(r)

for i in tqdm(range(100)):
    r = requests.get(
        f"{binance_futures_base_url}{binance_futures_agg_trades}"
        f"?symbol={symbol}&fromId={start_id}&limit=1000"
    ).json()
    start_id = r[-1]['a'] + 1
    df = pd.concat([df, pd.DataFrame(r)])
    if len(r) < 1000:
        break

df.columns = ['tradeId', 'px', 'qty', 'firstId', 'lastId', 'timestamp', 'buyerMaker']
df.to_parquet('Data/Binance/Futures/BTCUSDT_trades.parquet', index=False)
```

#### Key Derived Fields

```python
df['px'] = df['px'].astype(float)
df['qty'] = df['qty'].astype(float)
df['usd_notional'] = df['px'] * df['qty']
df['side'] = df['buyerMaker'].apply(lambda x: 'SELL' if x else 'BUY')
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
```

#### Orderbook Impact Calculation

```python
def calculate_impact(row: pd.Series, order_size: float, side: str = 'buy') -> (str, str):
    bids = [(row[f'bids[{i}].price'], row[f'bids[{i}].amount']) for i in range(25)]
    asks = [(row[f'asks[{i}].price'], row[f'asks[{i}].amount']) for i in range(25)]
    bids = sorted(bids, key=lambda x: x[0], reverse=True)
    asks = sorted(asks, key=lambda x: x[0])
    midprice = (bids[0][0] + asks[0][0]) / 2
    remaining_size = order_size
    weighted_price = 0
    if side == 'buy':
        for price, amount in asks:
            if remaining_size <= 0: break
            filled = min(remaining_size, amount)
            weighted_price += price * filled
            remaining_size -= filled
    else:
        for price, amount in bids:
            if remaining_size <= 0: break
            filled = min(remaining_size, amount)
            weighted_price += price * filled
            remaining_size -= filled
    avg_price = weighted_price / (order_size - remaining_size)
    impact_bps = abs(avg_price - midprice) / midprice * 10000
    return impact_bps, remaining_size
```

#### Power-Law Impact Extrapolation

```python
from scipy.optimize import curve_fit

def power_law(x, a, b):
    return a * np.power(x, b)

def estimate_impact_model(row, side='buy'):
    sizes = np.logspace(0, 4, num=20)
    impacts = calculate_impact_at_sizes(row, sizes, side)
    valid_mask = impacts > 0
    valid_sizes = sizes[valid_mask]
    valid_impacts = impacts[valid_mask]
    if len(valid_sizes) < 2:
        return None, None
    popt, _ = curve_fit(power_law, valid_sizes, valid_impacts, p0=[1, 0.5], bounds=([0, 0], [np.inf, 1]))
    return popt[0], popt[1]
```

#### Diming Logic

```python
def apply_diming_logic(self, bids, asks, px_tick_sz, symbol):
    buy_maker_active, sell_maker_active = self.get_orders_present(symbol)
    fv_maker_bid_price, fv_maker_ask_price = self.create_markup(symbol)
    for price, amount in bids:
        if price < fv_maker_bid_price:
            if buy_maker_active:
                existing_px, existing_amt = self.inv_mgr.ob_mgr.get_maker_quote(symbol, "buy_maker")
                if price == existing_px and amount == existing_amt:
                    maker_bid_price = price
                    continue
            maker_bid_price = round(price + px_tick_sz, 7)
            break
    for price, amount in asks:
        if price > fv_maker_ask_price:
            if sell_maker_active:
                existing_px, existing_amt = self.inv_mgr.ob_mgr.get_maker_quote(symbol, "sell_maker")
                if price == existing_px and amount == existing_amt:
                    maker_ask_price = price
                    continue
            maker_ask_price = round(price - px_tick_sz, 7)
            break
```

---

### 5.4 Practical Detection Pipeline

**Step 1** — Pull aggTrades from Binance. Store as Parquet.

**Step 2** — Compute derived fields (usd_notional, side, timestamps).

**Step 3** — Size distribution histogram analysis (both USD and token distributions). Compare against historical baseline.

**Step 4** — Recurring size + time spacing test. Bin trades, check time deltas within bins.

**Step 5** — NUFFT periodicity detection on trade times and spread series.

**Step 6** — Global vs. local orderbook divergence. Compare local spread/depth/volume against aggregated global.

**Step 7** — Inside-spread trade rate check (>10% = wash flow).

**Step 8** — Hourly volume profile deviation (chi-squared or KS test against baseline).

#### False Positive Considerations

1. Per-asset historical baselines, not cross-asset
2. MM quoting creates recurring passive sizes (not aggressive trade sizes)
3. Exchange minimum order sizes create artificial clustering
4. News/macro events cause legitimate spread widening
5. Low-liquidity periods amplify small-order impact
6. VWAP algos naturally align with volume seasonality (harder to detect than TWAP)

---

## 6. Appendix: Key Parameters & Thresholds

| Parameter | Value | Source |
|---|---|---|
| Z-score rolling window | 35 days (5 weeks) | order-size article |
| Volatility normalization window | 35 days | order-size article |
| Size bucket tolerance | $2,000 +/- $50 (example) | small-trader-alpha-2 |
| Trade offset truncation | +/- 100 bps | small-trader-alpha-2 |
| TWAP lookback | "past few hours" | small-trader-alpha-2 |
| Wash flow inside-spread threshold | >10% of trades | finding-arb-opportunities |
| Typical transfer time | 10-15 minutes | small-trader-alpha |
| Binance USDT withdrawal fee | 1 USDT flat | small-trader-alpha |
| Directional volume threshold | 80%+ in one direction | small-trader-alpha |
| Risk-adjusted return example | 24 bps/hour (on 50 bps arb) | small-trader-alpha |
| Market risk discount | 5 bps/hour (example) | small-trader-alpha |
| Convergence risk discount (sell side) | 10 bps/hour (example) | small-trader-alpha |
| Integration stipend range | $5k-$75k/month | finding-arb-opportunities |
| Monster level offsets | Whole-number %s (1%, 3%) | small-trader-alpha-2 |
| Gatling gun order count | 5 simultaneous | small-trader-alpha-3 |
| TriArb liquidity pulled by MMs | ~50% | small-trader-alpha-3 |
| Stablecoin MM returns (unlevered) | ~29.2% annualized | levering-to-the-tits |
| CPI trade latency assumption | 3 seconds | cpi-trade |
| CPI trade exit window | 15 seconds | cpi-trade |
| Markout horizon for FV | ~750ms (example) | advanced-market-making |
| Markout EWMA window | 5 seconds (example) | advanced-market-making |
| Adversity from arb pickoffs | ~50% | market-making-for-dummies |

---

## 7. Source Articles

All articles from `/Users/tdl321/Substack/QuantArb/`:

| Article | Key Topics |
|---|---|
| `2023-02-08_using-order-size-for-alpha.md` | Order size signals, z-score, taker buy proxy |
| `2023-03-01_a-novel-approach-to-frontrunning.md` | Fan token strategy, retail sentiment |
| `2023-04-24_execution-without-the-fluff.md` | Informed vs uninformed flow, OFI, adversity |
| `2023-07-03_small-trader-alpha-another-real-strategy.md` | Cross-exchange spot arb, liquid/illiquid setup |
| `2023-07-17_small-trader-alpha-2-advanced-arbitrage.md` | "Spotting Idiots", optimal offset, monster levels, diming |
| `2023-08-13_levering-to-the-tits-dead-simple.md` | Stablecoin MM, leverage, queue dynamics |
| `2023-09-17_small-trader-alpha-3-triangles.md` | Triangular arb, gatling gun, fill latency |
| `2023-11-19_hide-n-seek-pt1-avoiding-detection.md` | Round-number clustering, aggression matching, concealment |
| `2023-12-09_hide-n-seek-pt2-detection.md` | TWAP detection, NUFFT, global vs local book |
| `2024-04-06_small-trader-alpha-5-market-making.md` | Binary options MM, prediction markets, Black-Scholes |
| `2024-08-26_market-making-for-dummies-code-inside.md` | Toxic/non-toxic framework, hitting machine, diming code |
| `2024-09-21_hft-research-for-dummies.md` | Markouts, pickoff investigations, impact calculation |
| `2024-10-17_real-strategies-the-cpi-trade.md` | CPI trade, event-based retail flow, lead-lag |
| `2025-06-20_ultimate-crypto-arbitrage-guide.md` | Comprehensive arb guide, retail quoter dynamics |
| `2025-08-02_advanced-market-making.md` | Markout sizing, retail quoter game theory, FV |
| `2025-09-22_finding-arbitrage-opportunities.md` | Wash flow, internalized flow, exchange selection |

Let me propose a **fully corrected, end-to-end plan** for how to collect:

First of all, the official API Documentation link is:
https://fiscaldata.treasury.gov/api-documentation/
All the following ideas must comply with the API documentation.

* 2Y, 5Y, 7Y, 10Y, 20Y, 30Y **auction data**
* **Yield moves** around auctions
* So you can label auctions as **tailed / strong** and link them to **yields, mortgages, tech valuations, and risk sentiment**

I’ll separate **data sources** (official), **what to collect**, and **how to wire it together**.

---

## 1️⃣ Official Sources You Should Use

### A. U.S. Treasury Fiscal Data API (primary machine-readable source)

The Bureau of the Fiscal Service exposes auction data via the **Fiscal Data API**:

* **Base URL**
  `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/` ([PyPI][1])

Key auction-related endpoints you can rely on:

1. **Record-Setting Auction Data**

   * Endpoint:
     `v2/accounting/od/record_setting_auction` ([PyPI][1])
   * Contains record highs/lows for rates/yields, offering amounts, etc.
   * Good for context and sanity checks, not your primary day-to-day data.

2. **Treasury Bulletin: Offerings of Marketable Securities (other than weekly bills)**

   * Endpoint:
     `v1/accounting/tb/pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills` ([PyPI][1])
   * This table includes **results of auctions of marketable securities**, i.e. the notes/bonds you care about (2Y, 5Y, 7Y, 10Y, 20Y, 30Y) over roughly the last 2 years. It gives you **offering size, high yield, total bids, awards**, etc.([PyPI][1])

3. **Treasury Bulletin: Offerings of Regular Weekly Treasury Bills** (for completeness)

   * Endpoint:
     `v1/accounting/tb/pdo1_offerings_regular_weekly_treasury_bills` ([PyPI][1])
   * You don’t need this for 2Y–30Y, but it’s the symmetric table for bills.([PyPI][1])

There is also a dedicated **Treasury Securities Auctions** dataset referenced in the docs, with an endpoint `v1/accounting/od/auctions_query` under the same base URL.([Fiscal Data][2])
When you have direct access to the Fiscal Data docs, that’s the one you’d use for a **longer history and more granular auction results**.

---

### B. TreasuryDirect Auction Pages (for backup / HTML or CSV scraping)

TreasuryDirect exposes auction info mainly as **HTML, PDFs, and CSVs**, not a clean JSON API:

* **Announcements, Data & Results hub**: auctions schedules, useful data links([TreasuryDirect][3])
* **Recent auction results** (page with “Today’s Auction Results” and the “20 Most Recent Auctions”)([TreasuryDirect][4])
* **Auction Query** UI (filterable historical auction DB)([TreasuryDirect][5])

If you ever need to **backfill history beyond what Fiscal Data gives you**, you could:

* Script a scraper for **Auction Query** + the “20 Most Recent Auctions” pages
* Or periodically download their published CSV/text files

But for a **clean API-first Finexus pipeline**, use the **Fiscal Data API** as your primary source and treat TreasuryDirect as an auxiliary source.

---

### C. Yield Curve & Macro Rates

For **yield moves** and macro impact:

1. **FRED (Federal Reserve Economic Data)**

   * Daily series like DGS2, DGS5, DGS7, DGS10, DGS20, DGS30 (constant maturity yields).
   * You already have an economic integration via `get_economic()` that includes annual Treasury rates (Treasury_2Y, Treasury_5Y, …).
   * Extend your FRED collector to pull **daily yields** instead of/alongside annual.

2. **Treasury’s H.15 equivalents**

   * TreasuryInterest Rate statistics are also linked from “Useful Data for Research,” but practically, FRED’s H.15 mirror is easiest to work with.([TreasuryDirect][6])

For **equity & risk sentiment**:

* Use your existing **prices_daily / SP500_Daily** methods in `FinancialDataCollection` for SPX and NASDAQ proxies.

---

## 2️⃣ What Exactly to Collect per Auction

Focus on **notes/bonds: 2Y, 5Y, 7Y, 10Y, 20Y, 30Y**.

From Fiscal Data auction tables (`pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills` and/or `auctions_query`), you want at minimum:

### Core auction fields

* **auction_date** – the auction date
* **issue_date** – when the security settles
* **maturity_date / term** – e.g. “2-Year Note”, “10-Year Note”
* **security_type** – Note / Bond / TIPS / FRN (you filter to standard coupon notes/bonds)
* **cusip**

### Size & demand

* **offering_amount** – amount offered
* **amount_awarded** – amount actually issued
* **total_tenders / total_bids** – total competitive bids
* **bid_to_cover_ratio** – total bids ÷ amount awarded

### Yield results

* **high_yield** – stop-out yield from the auction
* **median_yield / low_yield** – if provided
* **coupon_rate** – the coupon attached to this security

**When-issued yield (WI_yield)** is the tricky one: it is *not* in the basic Fiscal Data tables. For WI you typically:

* Pull from a **market data provider** (Bloomberg, Refinitiv, Tradeweb), or
* Approximate using the **on-the-run yield shortly before the auction** from FRED or a real-time data vendor

Then you can compute the **tail**:

```text
tail_bps = (high_yield - WI_yield) * 100  # in basis points
```

---

## 3️⃣ What to Collect Around the Auction (Yield Moves, Risk Sentiment)

For each auction, you want to line up the following around the **auction timestamp** (or auction date if you stay at daily frequency):

### Yields

* 2Y, 5Y, 7Y, 10Y, 20Y, 30Y Treasury yields
* At **t0 (auction)**, t+5 min, t+30 min, and end-of-day if you have intraday
* At minimum with daily data:

  * **yield_t0**: yield on the auction date
  * **yield_t-1**: yield on previous business day
  * **Δyield_1d = yield_t0 − yield_t-1**

### Mortgages

* 30Y mortgage rate (from Freddie Mac series, or from FRED / Fiscal Data if you integrate it)
* You can link auctions (especially 10Y, 30Y) to changes in **Mortgage_30Y** from your economic dataset.

### Tech / growth proxies

* NASDAQ 100 index (e.g. QQQ)
* Duration proxies: high-growth / long-duration sectors
* From your existing **prices_daily** pipeline using FMP or other market data provider.

### Risk sentiment

* S&P 500 returns (5-minute / daily)
* VIX level or % change
* Credit spreads (if you decide to add them later)

---

## 4️⃣ How to Store This in Your Data Model

### A. Auction master table

Create a **treasury_auctions** table in your Finexus data layer:

```text
treasury_auctions
--------------------------
auction_id          (PK, UUID)
auction_date        (DATE)
issue_date          (DATE)
maturity_date       (DATE)
term                (TEXT)   -- '2Y', '5Y', '7Y', '10Y', '20Y', '30Y'
security_type       (TEXT)   -- 'Note', 'Bond'
cusip               (TEXT)

offering_amount     (NUMERIC)
amount_awarded      (NUMERIC)
total_tenders       (NUMERIC)
bid_to_cover        (NUMERIC)

coupon_rate         (NUMERIC)
high_yield          (NUMERIC)
wi_yield            (NUMERIC)   -- from market data
tail_bps            (NUMERIC)   -- computed

source_dataset      (TEXT)      -- 'fiscaldata_pdo2', 'auctions_query', etc.
raw_json            (JSONB)
created_at          (TIMESTAMP)
updated_at          (TIMESTAMP)
```

### B. Auction reaction table

```text
treasury_auction_reaction
---------------------------
reaction_id         (PK)
auction_id          (FK → treasury_auctions)

time_bucket         (TEXT)     -- 'D-1', 'D0', 'D+1' or '0m', '5m', '30m'
yield_2y            (NUMERIC)
yield_5y            (NUMERIC)
yield_7y            (NUMERIC)
yield_10y           (NUMERIC)
yield_20y           (NUMERIC)
yield_30y           (NUMERIC)

spx_ret             (NUMERIC)
nasdaq_ret          (NUMERIC)
vix_change          (NUMERIC)

mortgage_30y        (NUMERIC)
risk_score          (NUMERIC)  -- optional composite score
```

This table is populated by **joining** auction_date with your **FRED yields** and **market prices**.

---

## 5️⃣ Classifying “Tailed” vs “Strong” Auctions

Once WI yield and auction results are in place, you can define:

```text
tail_bps = (high_yield - WI_yield) * 100
```

Example classification:

* **Strong auction**: tail_bps ≤ −2 bps and bid_to_cover > 2.5
* **Neutral**: −2 bps < tail_bps < +2 bps
* **Weak / tailed**: tail_bps ≥ +2 bps or bid_to_cover < 2.2

You can then compute and store:

```text
auction_strength_score =
    a1 * (-tail_bps)
  + a2 * (bid_to_cover)
  + a3 * (indirect_share if you later add investor categories)
  + a4 * (-Δyield_1d)
```

and use that as a **macro risk sentiment input**.

---

## 6️⃣ Putting It Together as a Data Pipeline

Concrete steps:

1. **Collector for auctions (Fiscal Data API)**

   * Base: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/` ([PyPI][1])
   * Start with `v1/accounting/tb/pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills` to get:

     * auction_date, maturity, high_yield, offering_amount, amount_awarded, etc.([PyPI][1])
   * Later, switch/extend to the more general `v1/accounting/od/auctions_query` dataset for a deeper history.([Fiscal Data][2])

2. **Collector for yields (FRED)**

   * Extend your existing FRED integration (which already delivers a full curve annually) to pull **daily 2Y–30Y yields**, using standard series codes and storing them in a daily curve table.

3. **Join auctions to yields**

   * Join by **auction_date** (and intraday timestamp if you go that far)
   * Calculate Δyields, ΔSPX, ΔVIX, etc.

4. **Persist derived metrics**

   * Compute and save `tail_bps`, `auction_strength_score`, yield moves, equity moves.

5. **Use in your higher-level analytics**

   * Macro dashboard: show auction tails vs yield reactions
   * Factor models: add “auction shock” features for tech, mortgage-sensitive stocks
   * Risk sentiment: link strong/weak auctions to changes in your risk regime indicators.

---

If you’d like, next step I can:

* sketch the **exact HTTP request** and query params to the Fiscal Data API for a 10Y note auction, and/or
* draft a **Python `TreasuryAuctionCollector` class** that matches your existing `FinancialDataCollection` style.

[1]: https://pypi.org/project/fiscaldataapi/0.0.1/ "fiscaldataapi · PyPI"
[2]: https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/?utm_source=chatgpt.com "Treasury Securities Auctions Data"
[3]: https://www.treasurydirect.gov/auctions/announcements-data-results/?utm_source=chatgpt.com "Announcements, Data & Results"
[4]: https://www.treasurydirect.gov/auctions/results/?utm_source=chatgpt.com "Recent Auction Results"
[5]: https://treasurydirect.gov/auctions/auction-query/auction-query-help/?utm_source=chatgpt.com "How To Use The Auction Query Database"
[6]: https://www.treasurydirect.gov/auctions/announcements-data-results/useful-data-for-research/?utm_source=chatgpt.com "Useful Data for Research — TreasuryDirect"

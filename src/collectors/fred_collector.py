# fred_collector.py — Finalized with FMP enrichment, QC, and Meta sheet

import os
import time
from pathlib import Path
from io import StringIO
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests


class FREDCollector:
    """
    Collect full-history macro data with FRED as primary, enriched by FMP:
      - Core FRED indicators (long history)
      - Extra economic indicators from FMP (only those not in FRED)
      - Full Treasury curve from FMP (fills & extends)
      - S&P 500 from FMP (^GSPC) to extend price history

    Exports a workbook with:
      • Raw_Long (Date, Series, Value)
      • Monthly_Panel (month-end, last)
      • Quarterly_Panel (QE-DEC, last)
      • Meta (provenance & coverage snapshot)
    """

    # ---------- Core FRED indicators (friendly_name -> FRED series id) ----------
    DEFAULT_INDICATORS: Dict[str, str] = {
        # Growth & Output
        "Real_GDP": "GDPC1",
        "GDP": "GDP",
        "Industrial_Production": "INDPRO",
        "Capacity_Utilization": "TCU",

        # Inflation & Prices
        "CPI_All_Items": "CPIAUCSL",
        "Core_CPI": "CPILFESL",
        "PCE_Price_Index": "PCEPI",
        "Producer_Price_Index": "PPIACO",
        "Materials_PPI": "WPUSI012011",

        # Labor Market
        "Unemployment_Rate": "UNRATE",
        "Nonfarm_Payrolls": "PAYEMS",
        "Labor_Force_Participation": "CIVPART",
        "Job_Openings": "JTSJOL",

        # Rates (FRED primary for 3M/10Y)
        "Fed_Funds_Rate": "FEDFUNDS",
        "Treasury_3M": "TB3MS",
        "Treasury_10Y": "GS10",
        "Yield_Curve_10Y_2Y": "T10Y2Y",  # we also compute a version from Treasuries

        # Money
        "M2_Money_Supply": "M2SL",

        # Housing
        "Housing_Starts": "HOUST",
        "Building_Permits": "PERMIT",
        "Mortgage_Rate_30Y": "MORTGAGE30US",
        "Median_House_Price": "MSPUS",
        "Housing_Months_Supply": "MSACSR",
        "Home_Prices_CS": "CSUSHPINSA",

        # Sentiment & Markets
        "Consumer_Sentiment": "UMCSENT",
        "S&P_500_Index": "SP500",
        "VIX_Index": "VIXCLS",
    }

    # FMP econ names to add (only those not already covered by FRED names).
    # (FMP docs show many; keep a conservative set that’s complementary)
    FMP_ECON_SERIES: Dict[str, str] = {
        # friendly_name : FMP 'name' param
        "Retail_Sales": "retailSales",
        "Durable_Goods_Orders": "durableGoods",
        "Initial_Jobless_Claims": "initialClaims",
        "Vehicle_Sales": "totalVehicleSales",
        "Recession_Prob_Smoothed": "smoothedUSRecessionProbabilities",
        "Credit_Card_Rate": "commercialBankInterestRateOnCreditCardPlansAllAccounts",
        "Mortgage_15Y_Avg": "15YearFixedRateMortgageAverage",
        "Retail_Money_Funds": "retailMoneyFunds",
        "CD_3M_Rate": "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
    }

    EXPORT_FILE_DIR: str = "economics"
    FILENAME_BASE: str = "fred_full_history"

    def __init__(
        self,
        indicators: Optional[Dict[str, str]] = None,
        monthly_agg: str = "last",    # 'last', 'mean', 'max', 'min'
        quarterly_agg: str = "last",  # 'last', 'mean', 'max', 'min'
        timeout: int = 30,
        retries: int = 3,
        backoff: float = 0.8,
        fmp_api_key: Optional[str] = None,
        export_dir: Path = Path("economics")
    ):
        self.indicators = indicators.copy() if indicators else self.DEFAULT_INDICATORS.copy()
        self.export_dir = export_dir
        self.filename_base = self.FILENAME_BASE
        self.monthly_agg = monthly_agg
        self.quarterly_agg = quarterly_agg
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

        self.fmp_api_key = fmp_api_key
        self.fmp_from = "1900-01-01"  # internal default for full span

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "FREDCollector/1.1"})

        # raw_frames[name] = DataFrame with columns ['Date', name]
        self.raw_frames: Dict[str, pd.DataFrame] = {}

        # will collect coverage snapshot for Meta
        self._coverage_snapshot: Optional[pd.DataFrame] = None

    # ------------------------- HTTP helpers -------------------------

    def _get_fred_csv(self, series_id: str) -> Optional[str]:
        """Download full-history CSV for a single FRED series via fredgraph.csv."""
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        for i in range(self.retries):
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 200 and r.text.strip():
                    return r.text
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff * (2 ** i))
                    continue
                break
            except Exception:
                time.sleep(self.backoff * (2 ** i))
        return None

    def _get_fmp_json(self, url: str, params: dict) -> Optional[dict]:
        """Generic FMP GET returning JSON dict/list, or None."""
        if not self.fmp_api_key:
            return None
        p = params.copy()
        p["apikey"] = self.fmp_api_key
        for i in range(self.retries):
            try:
                r = self.session.get(url, params=p, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff * (2 ** i))
                    continue
                break
            except Exception:
                time.sleep(self.backoff * (2 ** i))
        return None

    # ---------------------- FMP econ/treasury helpers ----------------------

    def _get_fmp_econ(self, fmp_name: str, friendly: str) -> Optional[pd.DataFrame]:
        """
        Pull an FMP economic indicator by 'name' param.
        Returns standardized DataFrame ['Date', friendly].
        """
        if not self.fmp_api_key:
            return None
        url = "https://financialmodelingprep.com/stable/economic-indicators"
        params = {"name": fmp_name, "from": self.fmp_from}
        data = self._get_fmp_json(url, params)
        if not data or not isinstance(data, list):
            return None

        rows = []
        for rec in data:
            d = rec.get("date")
            v = rec.get("value", rec.get("val", None))
            try:
                vv = float(v) if v is not None else None
            except Exception:
                vv = None
            if d and vv is not None:
                rows.append({"Date": d, friendly: vv})

        if not rows:
            return None

        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        return df

    def _get_fmp_treasury(self) -> Optional[pd.DataFrame]:
        """
        Pulls the full Treasury curve from FMP, from self.fmp_from to present.
        Returns wide DF with columns: Date, month1, month2, month3, ..., year30
        """
        if not self.fmp_api_key:
            return None
        url = "https://financialmodelingprep.com/stable/treasury-rates"
        params = {"from": self.fmp_from}
        data = self._get_fmp_json(url, params)
        if not data or not isinstance(data, list):
            return None
        df = pd.DataFrame(data)
        if "date" not in df.columns:
            return None
        df = df.rename(columns={"date": "Date"})
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        # ensure numeric
        for c in df.columns:
            if c == "Date": 
                continue
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    # ---------------------- FMP S&P augment helpers ----------------------

    def _get_fmp_index_prices(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Pull daily historical prices for an index (e.g., '^GSPC') from FMP.
        Uses the full EOD endpoint. Returns ['Date','Value'] where Value=adjClose (or close).
        """
        if not self.fmp_api_key:
            return None
        url = "https://financialmodelingprep.com/stable/historical-price-eod/full"
        params = {"symbol": symbol, "from": self.fmp_from}
        data = self._get_fmp_json(url, params)
        if not data:
            return None

        # Response shapes vary: dict w/ 'historical' or flat list of bars
        hist = None
        if isinstance(data, dict) and "historical" in data:
            hist = data["historical"]
        elif isinstance(data, list) and data and isinstance(data[0], dict) and "date" in data[0]:
            hist = data
        else:
            return None

        rows = []
        for rec in hist:
            d = rec.get("date")
            if not d:
                continue
            val = rec.get("adjClose", rec.get("close"))
            try:
                vv = float(val) if val is not None else None
            except Exception:
                vv = None
            if vv is None:
                continue
            rows.append({"Date": d, "Value": vv})

        if not rows:
            return None

        out = pd.DataFrame(rows)
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        out["Value"] = pd.to_numeric(out["Value"], errors="coerce")
        out = out.dropna(subset=["Value"])
        return out

    def _augment_sp500_with_fmp(self) -> None:
        """
        Extend/patch S&P_500_Index series using FMP '^GSPC' adjClose (or close).
        Keeps FRED as primary; uses FMP to fill gaps and extend earlier history.
        """
        if not self.fmp_api_key:
            return

        base_name = "S&P_500_Index"
        base = self.raw_frames.get(base_name, pd.DataFrame(columns=["Date", base_name])).copy()
        fmp = self._get_fmp_index_prices("^GSPC")
        if fmp is None or fmp.empty:
            return

        fmp = fmp.rename(columns={"Value": base_name})[["Date", base_name]]

        if base is None or base.empty:
            self.raw_frames[base_name] = fmp.sort_values("Date").reset_index(drop=True)
            return

        merged = base.merge(fmp, on="Date", how="outer", suffixes=("", "_fmp"))
        merged[base_name] = pd.to_numeric(merged[base_name], errors="coerce")
        merged[f"{base_name}_fmp"] = pd.to_numeric(merged.get(f"{base_name}_fmp"), errors="coerce")
        merged[base_name] = merged[base_name].fillna(merged[f"{base_name}_fmp"])
        merged = merged[["Date", base_name]].dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        self.raw_frames[base_name] = merged

    # ---------------------- Fetch & prepare raw ----------------------

    def fetch_all(self) -> None:
        """
        Fetch all FRED series; enrich with FMP econ & Treasuries; compute spreads; augment S&P.
        Each frame has columns: ['Date', <friendly_name>] and Date is datetime64[ns].
        """
        # 1) FRED baseline
        for friendly_name, series_id in self.indicators.items():
            csv_text = self._get_fred_csv(series_id)
            time.sleep(0.05)  # polite to FRED; FMP has no sleep
            if not csv_text:
                self.raw_frames[friendly_name] = pd.DataFrame(columns=["Date", friendly_name])
                continue
            df = pd.read_csv(StringIO(csv_text))
            df.columns = ["Date", friendly_name]
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            df[friendly_name] = pd.to_numeric(df[friendly_name], errors="coerce")
            self.raw_frames[friendly_name] = df

        # 2) Extra FMP economic indicators (add only if not already present)
        for friendly, fmp_name in self.FMP_ECON_SERIES.items():
            if friendly in self.raw_frames and not self.raw_frames[friendly].empty:
                continue
            fdf = self._get_fmp_econ(fmp_name, friendly)
            if fdf is not None and not fdf.empty:
                self.raw_frames[friendly] = fdf

        # 3) Treasuries from FMP — fill & extend; add missing tenors
        tdf = self._get_fmp_treasury()
        tenor_map = {
            "month1": "Treasury_1M",
            "month2": "Treasury_2M",
            "month3": "Treasury_3M",
            "month6": "Treasury_6M",
            "year1": "Treasury_1Y",
            "year2": "Treasury_2Y",
            "year3": "Treasury_3Y",
            "year5": "Treasury_5Y",
            "year7": "Treasury_7Y",
            "year10": "Treasury_10Y",
            "year20": "Treasury_20Y",
            "year30": "Treasury_30Y",
        }
        if tdf is not None and not tdf.empty:
            for raw_col, friendly in tenor_map.items():
                if raw_col not in tdf.columns:
                    continue
                col = friendly
                add = tdf[["Date", raw_col]].rename(columns={raw_col: col}).copy()
                add[col] = pd.to_numeric(add[col], errors="coerce")
                add = add.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

                if col in self.raw_frames and not self.raw_frames[col].empty:
                    # FRED primary: fill only missing
                    merged = self.raw_frames[col].merge(add, on="Date", how="outer", suffixes=("", "_fmp"))
                    merged[col] = pd.to_numeric(merged[col], errors="coerce")
                    merged[f"{col}_fmp"] = pd.to_numeric(merged.get(f"{col}_fmp"), errors="coerce")
                    merged[col] = merged[col].fillna(merged[f"{col}_fmp"])
                    merged = merged[["Date", col]].sort_values("Date").reset_index(drop=True)
                    self.raw_frames[col] = merged
                else:
                    self.raw_frames[col] = add

        # 4) Compute spreads (prefer existing FRED for named spreads; else compute from Treasuries)
        def _get_series(name):
            return self.raw_frames.get(name, pd.DataFrame(columns=["Date", name]))

        def _merge_two(left: pd.DataFrame, right: pd.DataFrame, lname: str, rname: str) -> pd.DataFrame:
            return left.merge(right, on="Date", how="inner", suffixes=(f"_{lname}", f"_{rname}"))

        t3m = _get_series("Treasury_3M")
        t10 = _get_series("Treasury_10Y")
        t2y = _get_series("Treasury_2Y")
        t5y = _get_series("Treasury_5Y")

        # 10Y-3M
        if "Spread_10Y_3M" not in self.raw_frames:
            if not t10.empty and not t3m.empty:
                m = _merge_two(t10, t3m, "10Y", "3M")
                m["Spread_10Y_3M"] = pd.to_numeric(m["Treasury_10Y"], errors="coerce") - pd.to_numeric(m["Treasury_3M"], errors="coerce")
                self.raw_frames["Spread_10Y_3M"] = m[["Date", "Spread_10Y_3M"]].dropna()
        # 5Y-3M
        if "Spread_5Y_3M" not in self.raw_frames:
            if not t5y.empty and not t3m.empty:
                m = _merge_two(t5y, t3m, "5Y", "3M")
                m["Spread_5Y_3M"] = pd.to_numeric(m["Treasury_5Y"], errors="coerce") - pd.to_numeric(m["Treasury_3M"], errors="coerce")
                self.raw_frames["Spread_5Y_3M"] = m[["Date", "Spread_5Y_3M"]].dropna()
        # 10Y-2Y (computed) to fill FRED 'Yield_Curve_10Y_2Y'
        if "Yield_Curve_10Y_2Y" in self.raw_frames:
            calc = None
            if not t10.empty and not t2y.empty:
                m = _merge_two(t10, t2y, "10Y", "2Y")
                m["yc"] = pd.to_numeric(m["Treasury_10Y"], errors="coerce") - pd.to_numeric(m["Treasury_2Y"], errors="coerce")
                calc = m[["Date", "yc"]].rename(columns={"yc": "Yield_Curve_10Y_2Y"})
            if calc is not None and not calc.empty:
                base = self.raw_frames["Yield_Curve_10Y_2Y"]
                merged = base.merge(calc, on="Date", how="outer", suffixes=("", "_calc"))
                merged["Yield_Curve_10Y_2Y"] = pd.to_numeric(merged["Yield_Curve_10Y_2Y"], errors="coerce")
                merged["Yield_Curve_10Y_2Y_calc"] = pd.to_numeric(merged["Yield_Curve_10Y_2Y_calc"], errors="coerce")
                merged["Yield_Curve_10Y_2Y"] = merged["Yield_Curve_10Y_2Y"].fillna(merged["Yield_Curve_10Y_2Y_calc"])
                merged = merged[["Date", "Yield_Curve_10Y_2Y"]].sort_values("Date").reset_index(drop=True)
                self.raw_frames["Yield_Curve_10Y_2Y"] = merged
        else:
            if not t10.empty and not t2y.empty:
                m = t10.merge(t2y, on="Date", how="inner")
                m["Yield_Curve_10Y_2Y"] = pd.to_numeric(m["Treasury_10Y"], errors="coerce") - pd.to_numeric(m["Treasury_2Y"], errors="coerce")
                self.raw_frames["Yield_Curve_10Y_2Y"] = m[["Date", "Yield_Curve_10Y_2Y"]].dropna()

        # 5) Augment S&P 500 coverage with FMP (^GSPC) while keeping FRED primary
        self._augment_sp500_with_fmp()

    # ---------------------- Resampling helpers ----------------------

    def _resample(self, df: pd.DataFrame, col: str, rule: str, how: str) -> pd.DataFrame:
        """
        rule: 'ME' (month-end) or 'QE-DEC' (quarter-end, Dec-anchored) recommended.
        how : 'last'|'mean'|'max'|'min'
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=["Date", col])
        rule = "ME" if rule == "M" else ("QE-DEC" if rule == "Q" else rule)
        ts = df.set_index("Date")[col]
        if how == "last":
            out = ts.resample(rule).last()
        elif how == "mean":
            out = ts.resample(rule).mean()
        elif how == "max":
            out = ts.resample(rule).max()
        elif how == "min":
            out = ts.resample(rule).min()
        else:
            raise ValueError(f"Unsupported aggregation: {how}")
        out = out.to_frame().reset_index()
        out.columns = ["Date", col]
        return out

    def build_monthly_panel(self) -> pd.DataFrame:
        frames = []
        for name, df in self.raw_frames.items():
            frames.append(self._resample(df, name, "ME", self.monthly_agg))
        panel = None
        for f in frames:
            panel = f if panel is None else panel.merge(f, on="Date", how="outer")
        if panel is None:
            return pd.DataFrame(columns=["Date"] + list(self.raw_frames.keys()))
        return panel.sort_values("Date").reset_index(drop=True)

    def build_quarterly_panel(self) -> pd.DataFrame:
        frames = []
        for name, df in self.raw_frames.items():
            frames.append(self._resample(df, name, "QE-DEC", self.quarterly_agg))
        panel = None
        for f in frames:
            panel = f if panel is None else panel.merge(f, on="Date", how="outer")
        if panel is None:
            return pd.DataFrame(columns=["Date"] + list(self.raw_frames.keys()))
        return panel.sort_values("Date").reset_index(drop=True)

    # --------------------------- Export helpers ----------------------------

    def build_raw_long(self) -> pd.DataFrame:
        frames = []
        for name, df in self.raw_frames.items():
            if df is None or df.empty:
                continue
            tmp = df.copy()
            tmp = tmp.rename(columns={name: "Value"})
            tmp["Series"] = name
            frames.append(tmp[["Date", "Series", "Value"]])
        if not frames:
            return pd.DataFrame(columns=["Date", "Series", "Value"])
        out = pd.concat(frames, ignore_index=True)
        out = out.sort_values(["Series", "Date"]).reset_index(drop=True)
        return out

    # --------------------------- QC helpers ----------------------------

    def qc_dedupe_series(self) -> None:
        """
        Drop exact duplicate dates within each series (keeps last).
        Prints how many duplicates were removed per series.
        """
        total_dupes = 0
        for name, df in list(self.raw_frames.items()):
            if df is None or df.empty or "Date" not in df.columns or name not in df.columns:
                continue
            before = len(df)
            if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).drop_duplicates(subset=["Date"], keep="last").sort_values("Date")
            removed = before - len(df)
            if removed > 0:
                print(f"🧹 De-duplicated {name}: removed {removed} duplicate row(s)")
            total_dupes += removed
            self.raw_frames[name] = df.reset_index(drop=True)
        if total_dupes == 0:
            print("🧹 No duplicate dates found across series.")

    def print_coverage_summary(self, min_years: int = 10, max_rows: int = 9999) -> pd.DataFrame:
        """
        Print min/max dates, row counts, and span in years for every series.
        Flags series with span < min_years. Returns the summary as a DataFrame.
        """
        rows = []
        for name, df in self.raw_frames.items():
            if df is None or df.empty or "Date" not in df.columns or name not in df.columns:
                rows.append({"Series": name, "Start": None, "End": None, "Rows": 0, "Years": 0.0, "Short": True})
                continue
            dmin = pd.to_datetime(df["Date"], errors="coerce").min()
            dmax = pd.to_datetime(df["Date"], errors="coerce").max()
            n = len(df)
            years = float((dmax - dmin).days) / 365.25 if pd.notna(dmin) and pd.notna(dmax) else 0.0
            rows.append({
                "Series": name,
                "Start": dmin.date() if pd.notna(dmin) else None,
                "End": dmax.date() if pd.notna(dmax) else None,
                "Rows": int(n),
                "Years": round(years, 2),
                "Short": years < float(min_years)
            })
        summ = pd.DataFrame(rows).sort_values(["Short", "Series"], ascending=[False, True]).reset_index(drop=True)

        # print
        print("\n=== Macro Series Coverage ===")
        shown = 0
        for _, r in summ.iterrows():
            if shown >= max_rows:
                print(f"... (+{len(summ)-max_rows} more)")
                break
            flag = "⚠️" if r["Short"] else "✓"
            print(f"{flag} {r['Series']:<28} Rows: {r['Rows']:>6}  Span: {r['Years']:>6.2f} yrs  Start: {r['Start']}  End: {r['End']}")
            shown += 1
        short_count = int(summ["Short"].sum())
        print(f"\nTotal series: {len(summ)}  |  Short (<{min_years}y): {short_count}")

        self._coverage_snapshot = summ.copy()
        return summ

    # --------------------------- Export ----------------------------

    def _sanitize_sheet_name(self, name: str) -> str:
        bad = {":", "\\", "/", "?", "*", "[", "]"}
        cleaned = "".join(ch for ch in str(name) if ch not in bad)[:31]
        return cleaned or "Sheet"

    def export_single_workbook(self) -> str:
        """
        Write ONE workbook:
          - Raw_Long
          - Monthly_Panel
          - Quarterly_Panel
          - Meta (provenance & coverage snapshot)
        """
        os.makedirs(self.export_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        xlsx_path = os.path.join(str(self.export_dir), f"{self.filename_base}_{ts}.xlsx")

        raw_long  = self.build_raw_long()
        monthly   = self.build_monthly_panel()
        quarterly = self.build_quarterly_panel()

        # Build Meta sheet
        meta_rows = []
        meta_rows.append({"Key": "Generated_At", "Value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        meta_rows.append({"Key": "FRED_Primary", "Value": "Yes"})
        meta_rows.append({"Key": "FMP_Enrichment", "Value": "Economic Indicators + Treasuries + S&P (^GSPC)"})
        meta_rows.append({"Key": "FMP_From", "Value": self.fmp_from})
        meta_rows.append({"Key": "Monthly_Agg", "Value": self.monthly_agg})
        meta_rows.append({"Key": "Quarterly_Agg", "Value": self.quarterly_agg})
        meta_rows.append({"Key": "Series_Count", "Value": len(self.raw_frames)})

        meta_df = pd.DataFrame(meta_rows)

        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            raw_long.to_excel(writer, sheet_name="Raw_Long", index=False)
            monthly.to_excel(writer, sheet_name="Monthly_Panel", index=False)
            quarterly.to_excel(writer, sheet_name="Quarterly_Panel", index=False)
            # Meta (top) + Coverage (below) if available
            meta_df.to_excel(writer, sheet_name="Meta", index=False)
            if isinstance(self._coverage_snapshot, pd.DataFrame) and not self._coverage_snapshot.empty:
                # leave a blank row between meta and coverage by writing to the same sheet with startrow
                start_row = len(meta_df) + 2
                self._coverage_snapshot.to_excel(writer, sheet_name="Meta", index=False, startrow=start_row)

        print(f"Saved: {os.path.relpath(xlsx_path)}")
        return xlsx_path

    # Convenience one-shots
    def run_export_single(self) -> str:
        self.fetch_all()
        # QC is optional but recommended in normal runs
        self.qc_dedupe_series()
        # self.print_coverage_summary(min_years=15)
        return self.export_single_workbook()

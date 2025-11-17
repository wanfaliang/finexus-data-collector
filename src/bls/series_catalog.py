# series_catalog.py
"""
BLS Time Series Catalog for Financial & Economic Analysis

This module defines the key BLS series IDs that are most relevant for
financial analysis, economic research, and market analysis.

References:
- CPI: https://www.bls.gov/cpi/
- CES: https://www.bls.gov/ces/
- LA: https://www.bls.gov/lau/
- PPI: https://www.bls.gov/ppi/
"""

from typing import Dict, List

# ==================== INFLATION & PRICES ====================

CPI_SERIES = {
    # CPI-U (All Urban Consumers) - Most widely used
    "CUSR0000SA0": {
        "name": "CPI-U: All items",
        "description": "Headline inflation - All urban consumers, seasonally adjusted",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "CUUR0000SA0": {
        "name": "CPI-U: All items (NSA)",
        "description": "All items, not seasonally adjusted",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CUSR0000SA0L1E": {
        "name": "CPI-U: All items less food & energy",
        "description": "Core inflation - excludes volatile food & energy",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "CUUR0000SA0L1E": {
        "name": "CPI-U: All items less food & energy (NSA)",
        "description": "Core inflation, not seasonally adjusted",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },

    # Major CPI Components
    "CUSR0000SAF1": {
        "name": "CPI-U: Food",
        "description": "Food inflation",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CUSR0000SAH1": {
        "name": "CPI-U: Housing",
        "description": "Housing costs (largest CPI component ~40%)",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CUSR0000SETA": {
        "name": "CPI-U: Energy",
        "description": "Energy prices (volatile component)",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CUSR0000SETB01": {
        "name": "CPI-U: Gasoline (all types)",
        "description": "Gasoline prices",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
    "CUSR0000SETA01": {
        "name": "CPI-U: Energy commodities",
        "description": "Energy commodities index",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
    "CUSR0000SAM": {
        "name": "CPI-U: Medical care",
        "description": "Medical care costs",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CUSR0000SAS": {
        "name": "CPI-U: Transportation",
        "description": "Transportation costs",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
}

# ==================== EMPLOYMENT & LABOR ====================

CES_SERIES = {
    # Total Employment
    "CES0000000001": {
        "name": "Total Nonfarm Payroll Employment",
        "description": "Total nonfarm employment (thousands) - key jobs report number",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "CES0500000001": {
        "name": "Total Private Employment",
        "description": "Total private sector employment (thousands)",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },

    # Wages
    "CES0500000003": {
        "name": "Average Hourly Earnings - Private",
        "description": "Average hourly earnings, all private employees",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "CES0000000003": {
        "name": "Average Hourly Earnings - Total Nonfarm",
        "description": "Average hourly earnings, total nonfarm",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },

    # Hours
    "CES0500000002": {
        "name": "Average Weekly Hours - Private",
        "description": "Average weekly hours, all private employees",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CES0000000002": {
        "name": "Average Weekly Hours - Total Nonfarm",
        "description": "Average weekly hours, total nonfarm",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },

    # Key Sectors
    "CES3000000001": {
        "name": "Manufacturing Employment",
        "description": "Manufacturing sector employment (thousands)",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "CES4200000001": {
        "name": "Retail Trade Employment",
        "description": "Retail trade employment (thousands)",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
    "CES5500000001": {
        "name": "Financial Activities Employment",
        "description": "Financial activities employment (thousands)",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
    "CES6500000001": {
        "name": "Leisure & Hospitality Employment",
        "description": "Leisure and hospitality employment (thousands)",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
}

UNEMPLOYMENT_SERIES = {
    # Unemployment Rate
    "LNS14000000": {
        "name": "Unemployment Rate",
        "description": "U-3 unemployment rate (%) - headline unemployment",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "LNS13327709": {
        "name": "U-6 Unemployment Rate",
        "description": "Total unemployed plus marginally attached plus part-time for economic reasons",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },

    # Labor Force
    "LNS11300000": {
        "name": "Labor Force Participation Rate",
        "description": "Civilian labor force participation rate (%)",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "LNS12300000": {
        "name": "Employment-Population Ratio",
        "description": "Employment-population ratio (%)",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "LNS11000000": {
        "name": "Civilian Labor Force Level",
        "description": "Civilian labor force level (thousands)",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },

    # Demographics
    "LNS14000006": {
        "name": "Unemployment Rate - Black or African American",
        "description": "Unemployment rate for Black or African American (16 years and over)",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
    "LNS14000009": {
        "name": "Unemployment Rate - Hispanic or Latino",
        "description": "Unemployment rate for Hispanic or Latino ethnicity",
        "frequency": "MONTHLY",
        "importance": "MEDIUM",
    },
}

# ==================== PRODUCER PRICES ====================

PPI_SERIES = {
    # Final Demand
    "WPSFD49207": {
        "name": "PPI: Final Demand",
        "description": "Producer Price Index for final demand",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },
    "WPSFD49116": {
        "name": "PPI: Final Demand less food & energy",
        "description": "Core PPI - final demand excluding food and energy",
        "frequency": "MONTHLY",
        "importance": "CRITICAL",
    },

    # Commodities
    "WPUFD49207": {
        "name": "PPI: Final Demand Commodities",
        "description": "PPI for final demand commodities",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
    "WPUFD49104": {
        "name": "PPI: Final Demand Energy",
        "description": "PPI for final demand energy",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },

    # Services
    "WPUFD49205": {
        "name": "PPI: Final Demand Services",
        "description": "PPI for final demand services",
        "frequency": "MONTHLY",
        "importance": "HIGH",
    },
}

# ==================== PRODUCTIVITY ====================

PRODUCTIVITY_SERIES = {
    "PRS85006092": {
        "name": "Nonfarm Business Productivity",
        "description": "Labor productivity: output per hour, nonfarm business sector",
        "frequency": "QUARTERLY",
        "importance": "HIGH",
    },
    "PRS85006112": {
        "name": "Nonfarm Business Unit Labor Costs",
        "description": "Unit labor costs, nonfarm business sector",
        "frequency": "QUARTERLY",
        "importance": "HIGH",
    },
}

# ==================== CONSOLIDATED CATALOG ====================

ALL_KEY_SERIES = {
    **CPI_SERIES,
    **CES_SERIES,
    **UNEMPLOYMENT_SERIES,
    **PPI_SERIES,
    **PRODUCTIVITY_SERIES,
}

# Priority tiers for collection
CRITICAL_SERIES = [sid for sid, meta in ALL_KEY_SERIES.items() if meta["importance"] == "CRITICAL"]
HIGH_PRIORITY_SERIES = [sid for sid, meta in ALL_KEY_SERIES.items() if meta["importance"] == "HIGH"]
MEDIUM_PRIORITY_SERIES = [sid for sid, meta in ALL_KEY_SERIES.items() if meta["importance"] == "MEDIUM"]

# ==================== HELPER FUNCTIONS ====================

def get_series_metadata(series_id: str) -> Dict:
    """Get metadata for a given series ID."""
    return ALL_KEY_SERIES.get(series_id, {})

def get_series_by_importance(importance: str) -> List[str]:
    """Get all series IDs by importance level."""
    return [sid for sid, meta in ALL_KEY_SERIES.items() if meta["importance"] == importance]

def get_series_by_frequency(frequency: str) -> List[str]:
    """Get all series IDs by frequency."""
    return [sid for sid, meta in ALL_KEY_SERIES.items() if meta["frequency"] == frequency]

def list_all_series() -> List[Dict]:
    """Return all series with their metadata."""
    return [{"series_id": sid, **meta} for sid, meta in ALL_KEY_SERIES.items()]


if __name__ == "__main__":
    print(f"Total series in catalog: {len(ALL_KEY_SERIES)}")
    print(f"Critical series: {len(CRITICAL_SERIES)}")
    print(f"High priority: {len(HIGH_PRIORITY_SERIES)}")
    print(f"Medium priority: {len(MEDIUM_PRIORITY_SERIES)}")

    print("\n=== CRITICAL SERIES ===")
    for sid in CRITICAL_SERIES:
        meta = ALL_KEY_SERIES[sid]
        print(f"{sid:20s} - {meta['name']}")

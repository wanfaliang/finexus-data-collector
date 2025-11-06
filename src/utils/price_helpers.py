"""
Price Query Helper Functions
Utilities for querying price data with fallback from bulk prices
"""
from datetime import date
from typing import Optional, List, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from src.database.models import PriceDaily, PriceDailyBulk


def get_price(
    session: Session,
    symbol: str,
    target_date: date,
    fallback_to_bulk: bool = True
) -> Optional[Dict]:
    """
    Get price for a symbol on a specific date

    Args:
        session: Database session
        symbol: Stock symbol
        target_date: Date to fetch
        fallback_to_bulk: If True, check bulk table if not found in regular table

    Returns:
        Dictionary with OHLCV data or None if not found
    """
    # Try regular prices first
    price = session.query(PriceDaily).filter(
        PriceDaily.symbol == symbol,
        PriceDaily.date == target_date
    ).first()

    if price:
        return {
            'symbol': price.symbol,
            'date': price.date,
            'open': float(price.open) if price.open else None,
            'high': float(price.high) if price.high else None,
            'low': float(price.low) if price.low else None,
            'close': float(price.close) if price.close else None,
            'volume': int(price.volume) if price.volume else None,
            'source': 'regular'
        }

    # Fallback to bulk if enabled
    if fallback_to_bulk:
        bulk_price = session.query(PriceDailyBulk).filter(
            PriceDailyBulk.symbol == symbol,
            PriceDailyBulk.date == target_date
        ).first()

        if bulk_price:
            return {
                'symbol': bulk_price.symbol,
                'date': bulk_price.date,
                'open': float(bulk_price.open) if bulk_price.open else None,
                'high': float(bulk_price.high) if bulk_price.high else None,
                'low': float(bulk_price.low) if bulk_price.low else None,
                'close': float(bulk_price.close) if bulk_price.close else None,
                'adj_close': float(bulk_price.adj_close) if bulk_price.adj_close else None,
                'volume': int(bulk_price.volume) if bulk_price.volume else None,
                'source': 'bulk'
            }

    return None


def get_close_price(
    session: Session,
    symbol: str,
    target_date: date,
    fallback_to_bulk: bool = True
) -> Optional[float]:
    """
    Get closing price for a symbol on a specific date

    Args:
        session: Database session
        symbol: Stock symbol
        target_date: Date to fetch
        fallback_to_bulk: If True, check bulk table if not found

    Returns:
        Close price as float or None
    """
    price_data = get_price(session, symbol, target_date, fallback_to_bulk)
    return price_data['close'] if price_data else None


def get_price_range(
    session: Session,
    symbol: str,
    start_date: date,
    end_date: date,
    fallback_to_bulk: bool = True
) -> List[Dict]:
    """
    Get price history for a symbol over a date range

    Args:
        session: Database session
        symbol: Stock symbol
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        fallback_to_bulk: If True, include bulk data for missing dates

    Returns:
        List of price dictionaries sorted by date
    """
    # Get regular prices
    regular_prices = session.query(PriceDaily).filter(
        PriceDaily.symbol == symbol,
        PriceDaily.date >= start_date,
        PriceDaily.date <= end_date
    ).order_by(PriceDaily.date).all()

    results = []
    regular_dates = set()

    for price in regular_prices:
        regular_dates.add(price.date)
        results.append({
            'symbol': price.symbol,
            'date': price.date,
            'open': float(price.open) if price.open else None,
            'high': float(price.high) if price.high else None,
            'low': float(price.low) if price.low else None,
            'close': float(price.close) if price.close else None,
            'volume': int(price.volume) if price.volume else None,
            'source': 'regular'
        })

    # Fallback to bulk for missing dates
    if fallback_to_bulk:
        bulk_prices = session.query(PriceDailyBulk).filter(
            PriceDailyBulk.symbol == symbol,
            PriceDailyBulk.date >= start_date,
            PriceDailyBulk.date <= end_date,
            ~PriceDailyBulk.date.in_(regular_dates)  # Only dates not in regular
        ).order_by(PriceDailyBulk.date).all()

        for price in bulk_prices:
            results.append({
                'symbol': price.symbol,
                'date': price.date,
                'open': float(price.open) if price.open else None,
                'high': float(price.high) if price.high else None,
                'low': float(price.low) if price.low else None,
                'close': float(price.close) if price.close else None,
                'adj_close': float(price.adj_close) if price.adj_close else None,
                'volume': int(price.volume) if price.volume else None,
                'source': 'bulk'
            })

    # Sort by date
    results.sort(key=lambda x: x['date'])

    return results


def check_price_availability(
    session: Session,
    symbol: str,
    target_date: date
) -> Dict[str, bool]:
    """
    Check if price exists in regular and/or bulk tables

    Args:
        session: Database session
        symbol: Stock symbol
        target_date: Date to check

    Returns:
        Dictionary with availability flags:
        {'regular': bool, 'bulk': bool, 'either': bool}
    """
    regular_exists = session.query(PriceDaily).filter(
        PriceDaily.symbol == symbol,
        PriceDaily.date == target_date
    ).count() > 0

    bulk_exists = session.query(PriceDailyBulk).filter(
        PriceDailyBulk.symbol == symbol,
        PriceDailyBulk.date == target_date
    ).count() > 0

    return {
        'regular': regular_exists,
        'bulk': bulk_exists,
        'either': regular_exists or bulk_exists
    }


def find_missing_dates(
    session: Session,
    symbol: str,
    start_date: date,
    end_date: date,
    check_bulk: bool = False
) -> List[date]:
    """
    Find dates where price data is missing

    Args:
        session: Database session
        symbol: Stock symbol
        start_date: Start date
        end_date: End date
        check_bulk: If True, only report dates missing from BOTH tables

    Returns:
        List of missing dates
    """
    # Get all dates that exist in regular table
    regular_dates = set(
        row[0] for row in session.query(PriceDaily.date).filter(
            PriceDaily.symbol == symbol,
            PriceDaily.date >= start_date,
            PriceDaily.date <= end_date
        ).all()
    )

    if check_bulk:
        # Get dates from bulk table
        bulk_dates = set(
            row[0] for row in session.query(PriceDailyBulk.date).filter(
                PriceDailyBulk.symbol == symbol,
                PriceDailyBulk.date >= start_date,
                PriceDailyBulk.date <= end_date
            ).all()
        )

        # Combined dates
        available_dates = regular_dates | bulk_dates
    else:
        available_dates = regular_dates

    # Generate all dates in range
    from datetime import timedelta
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current)
        current += timedelta(days=1)

    # Find missing dates
    missing = [d for d in all_dates if d not in available_dates]

    return missing


def compare_prices(
    session: Session,
    symbol: str,
    target_date: date
) -> Optional[Dict]:
    """
    Compare prices between regular and bulk tables for validation

    Args:
        session: Database session
        symbol: Stock symbol
        target_date: Date to compare

    Returns:
        Dictionary with comparison results or None if not in both tables
    """
    regular = session.query(PriceDaily).filter(
        PriceDaily.symbol == symbol,
        PriceDaily.date == target_date
    ).first()

    bulk = session.query(PriceDailyBulk).filter(
        PriceDailyBulk.symbol == symbol,
        PriceDailyBulk.date == target_date
    ).first()

    if not regular or not bulk:
        return None

    close_diff = abs(float(regular.close) - float(bulk.close)) if regular.close and bulk.close else None
    volume_diff = abs(int(regular.volume) - int(bulk.volume)) if regular.volume and bulk.volume else None

    return {
        'symbol': symbol,
        'date': target_date,
        'regular_close': float(regular.close) if regular.close else None,
        'bulk_close': float(bulk.close) if bulk.close else None,
        'close_difference': close_diff,
        'regular_volume': int(regular.volume) if regular.volume else None,
        'bulk_volume': int(bulk.volume) if bulk.volume else None,
        'volume_difference': volume_diff,
        'matches': close_diff < 0.01 if close_diff is not None else None
    }


def copy_from_bulk_to_regular(
    session: Session,
    symbol: str,
    start_date: date,
    end_date: date
) -> int:
    """
    Copy prices from bulk table to regular table for a symbol
    Useful when adding a new company to portfolio

    Args:
        session: Database session
        symbol: Stock symbol
        start_date: Start date
        end_date: End date

    Returns:
        Number of records copied
    """
    from sqlalchemy.dialects.postgresql import insert

    # Get bulk prices for dates not in regular table
    regular_dates = set(
        row[0] for row in session.query(PriceDaily.date).filter(
            PriceDaily.symbol == symbol,
            PriceDaily.date >= start_date,
            PriceDaily.date <= end_date
        ).all()
    )

    bulk_prices = session.query(PriceDailyBulk).filter(
        PriceDailyBulk.symbol == symbol,
        PriceDailyBulk.date >= start_date,
        PriceDailyBulk.date <= end_date,
        ~PriceDailyBulk.date.in_(regular_dates) if regular_dates else True
    ).all()

    if not bulk_prices:
        return 0

    # Convert to regular price records
    records = []
    for bp in bulk_prices:
        records.append({
            'symbol': bp.symbol,
            'date': bp.date,
            'open': bp.open,
            'high': bp.high,
            'low': bp.low,
            'close': bp.close,
            'volume': bp.volume,
            'change': None,  # Will be calculated separately if needed
            'change_percent': None,
            'vwap': None
        })

    if records:
        stmt = insert(PriceDaily).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'date'])
        session.execute(stmt)
        session.commit()

    return len(records)


if __name__ == "__main__":
    # Example usage
    from src.database.connection import get_session
    from datetime import datetime

    with get_session() as session:
        # Test get_price with fallback
        symbol = 'AAPL'
        target_date = datetime(2024, 11, 1).date()

        price = get_price(session, symbol, target_date, fallback_to_bulk=True)
        if price:
            print(f"{symbol} on {target_date}:")
            print(f"  Close: ${price['close']:.2f}")
            print(f"  Source: {price['source']}")
        else:
            print(f"No price found for {symbol} on {target_date}")

"""
Treasury Module

Data collection for U.S. Treasury securities auctions and yield data.

Components:
- TreasuryClient: API client for Fiscal Data API
- TreasuryCollector: Data collection and storage orchestration
- treasury_auction_calendar: Calendar/ICS file generation for auctions

Usage:
    from src.treasury import TreasuryClient, TreasuryCollector

    # Create client
    client = TreasuryClient()

    # Get upcoming auctions
    upcoming = client.get_upcoming_auctions()

    # Get historical results
    results = client.get_auction_results(
        security_term='10-Year',
        start_date='2024-01-01'
    )
"""
from .treasury_client import TreasuryClient, create_treasury_client
from .treasury_collector import TreasuryCollector, create_collector

__all__ = [
    'TreasuryClient',
    'TreasuryCollector',
    'create_treasury_client',
    'create_collector',
]

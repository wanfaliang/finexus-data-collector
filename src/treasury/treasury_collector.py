"""
Treasury Data Collector

Collects and stores Treasury auction data from the Fiscal Data API.
Handles:
  - Upcoming auctions calendar
  - Historical auction results
  - Daily yield curve data (from Treasury or FRED)
  - Market reaction tracking

Author: FinExus Data Collector
Created: 2025-12-03
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, UTC
from decimal import Decimal, InvalidOperation

from sqlalchemy import create_engine, select, func, and_, or_
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .treasury_client import TreasuryClient

log = logging.getLogger("TreasuryCollector")


class TreasuryCollector:
    """
    Collector for Treasury auction and yield data.

    Orchestrates data collection from Fiscal Data API and stores in database.
    """

    # Term to months mapping
    TERM_TO_MONTHS = {
        '2-Year': 24,
        '5-Year': 60,
        '7-Year': 84,
        '10-Year': 120,
        '20-Year': 240,
        '30-Year': 360,
    }

    def __init__(
        self,
        db_session: Session,
        client: Optional[TreasuryClient] = None,
    ):
        """
        Initialize Treasury collector.

        Args:
            db_session: SQLAlchemy session
            client: Treasury API client (creates new if not provided)
        """
        self.session = db_session
        self.client = client or TreasuryClient()

        # Stats tracking
        self._stats = {
            'auctions_fetched': 0,
            'auctions_inserted': 0,
            'auctions_updated': 0,
            'upcoming_fetched': 0,
            'upcoming_inserted': 0,
            'api_requests': 0,
        }

    @property
    def stats(self) -> Dict[str, int]:
        """Get collection statistics."""
        return self._stats.copy()

    def reset_stats(self):
        """Reset collection statistics."""
        for key in self._stats:
            self._stats[key] = 0

    # ===================== Data Parsing Helpers ===================== #

    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely parse a value to Decimal."""
        if value is None or value == '' or value == 'null':
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None

    def _parse_date(self, value: Any) -> Optional[date]:
        """Safely parse a date string."""
        if value is None or value == '':
            return None
        try:
            if isinstance(value, date):
                return value
            return datetime.strptime(str(value), '%Y-%m-%d').date()
        except ValueError:
            return None

    def _compute_bid_to_cover(
        self,
        total_tendered: Optional[Decimal],
        total_accepted: Optional[Decimal],
    ) -> Optional[Decimal]:
        """Compute bid-to-cover ratio."""
        if total_tendered and total_accepted and total_accepted > 0:
            return total_tendered / total_accepted
        return None

    def _compute_tail_bps(
        self,
        high_yield: Optional[Decimal],
        wi_yield: Optional[Decimal],
    ) -> Optional[Decimal]:
        """
        Compute auction tail in basis points.

        tail = (high_yield - wi_yield) * 100

        Positive tail = weak auction (stopped through WI)
        Negative tail = strong auction (stopped below WI)
        """
        if high_yield is not None and wi_yield is not None:
            return (high_yield - wi_yield) * 100
        return None

    def _classify_auction(
        self,
        tail_bps: Optional[Decimal],
        bid_to_cover: Optional[Decimal],
    ) -> Optional[str]:
        """
        Classify auction strength based on tail and bid-to-cover.

        Returns:
            'strong', 'neutral', 'weak', or 'tailed'
        """
        if tail_bps is None:
            return None

        tail_float = float(tail_bps)

        # Classification thresholds
        if tail_float <= -2.0:
            if bid_to_cover and float(bid_to_cover) > 2.5:
                return 'strong'
            return 'neutral'
        elif tail_float >= 2.0:
            return 'tailed'
        elif tail_float >= 1.0:
            return 'weak'
        else:
            return 'neutral'

    # ===================== Auction Collection ===================== #

    def collect_upcoming_auctions(self) -> int:
        """
        Collect and store upcoming Treasury auctions.

        Returns:
            Number of auctions inserted/updated
        """
        from ..database.treasury_models import TreasuryUpcomingAuction

        log.info("Collecting upcoming Treasury auctions...")

        auctions = self.client.get_upcoming_auctions(
            security_types=['Note', 'Bond'],
            target_terms_only=True,
        )
        self._stats['upcoming_fetched'] = len(auctions)
        self._stats['api_requests'] += 1

        inserted = 0
        for auc in auctions:
            auction_date = self._parse_date(auc.get('auction_date'))
            if not auction_date:
                continue

            # Check if already exists
            existing = self.session.query(TreasuryUpcomingAuction).filter(
                TreasuryUpcomingAuction.auction_date == auction_date,
                TreasuryUpcomingAuction.security_term == auc.get('security_term'),
            ).first()

            if existing:
                # Update existing
                existing.cusip = auc.get('cusip')
                # API returns 'offering_amt' not 'offering_amount'
                existing.offering_amount = self._parse_decimal(auc.get('offering_amt'))
                existing.issue_date = self._parse_date(auc.get('issue_date'))
                existing.announcement_date = self._parse_date(auc.get('announcemt_date'))
                existing.updated_at = datetime.now(UTC)
            else:
                # Insert new
                # Note: API field names differ from model names
                # offering_amt -> offering_amount, announcemt_date -> announcement_date
                # Note: API does not return maturity_date for upcoming auctions
                upcoming = TreasuryUpcomingAuction(
                    cusip=auc.get('cusip'),
                    security_type=auc.get('security_type', 'Note'),
                    security_term=auc.get('security_term'),
                    auction_date=auction_date,
                    issue_date=self._parse_date(auc.get('issue_date')),
                    offering_amount=self._parse_decimal(auc.get('offering_amt')),
                    announcement_date=self._parse_date(auc.get('announcemt_date')),
                    is_processed=False,
                )
                self.session.add(upcoming)
                inserted += 1

        self.session.commit()
        self._stats['upcoming_inserted'] = inserted

        log.info(f"Collected {len(auctions)} upcoming auctions, {inserted} new")
        return inserted

    def collect_auction_results(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        security_term: Optional[str] = None,
        backfill_years: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Collect and store historical auction results.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            security_term: Specific term to collect ('2-Year', '10-Year', etc.)
            backfill_years: If set, backfill N years of data

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        from ..database.treasury_models import TreasuryAuction

        if backfill_years:
            start_date = (date.today() - timedelta(days=backfill_years * 365)).strftime('%Y-%m-%d')

        log.info(f"Collecting auction results: start={start_date}, end={end_date}, term={security_term}")

        auctions = self.client.get_auction_results(
            security_term=security_term,
            start_date=start_date,
            end_date=end_date,
        )
        self._stats['auctions_fetched'] = len(auctions)
        self._stats['api_requests'] += 1

        inserted = 0
        updated = 0

        for auc in auctions:
            cusip = auc.get('cusip')
            auction_date = self._parse_date(auc.get('auction_date'))

            if not cusip or not auction_date:
                continue

            # Parse all fields - use API field names
            security_term = auc.get('security_term')
            high_yield = self._parse_decimal(auc.get('high_yield'))
            offering_amount = self._parse_decimal(auc.get('offering_amt'))
            total_tendered = self._parse_decimal(auc.get('total_tendered'))
            total_accepted = self._parse_decimal(auc.get('total_accepted'))

            # Use bid_to_cover_ratio from API if available, otherwise compute
            bid_to_cover = self._parse_decimal(auc.get('bid_to_cover_ratio'))
            if bid_to_cover is None:
                bid_to_cover = self._compute_bid_to_cover(total_tendered, total_accepted)

            # Check if exists
            existing = self.session.query(TreasuryAuction).filter(
                TreasuryAuction.cusip == cusip,
                TreasuryAuction.auction_date == auction_date,
            ).first()

            if existing:
                # Update existing record
                existing.high_yield = high_yield
                existing.offering_amount = offering_amount
                existing.total_tendered = total_tendered
                existing.total_accepted = total_accepted
                existing.bid_to_cover_ratio = bid_to_cover
                existing.coupon_rate = self._parse_decimal(auc.get('int_rate'))
                existing.price_per_100 = self._parse_decimal(auc.get('high_price'))
                existing.raw_json = auc
                existing.updated_at = datetime.now(UTC)
                updated += 1
            else:
                # Create new record
                # Note: API field names differ from model names
                # comp_accepted -> competitive_accepted, noncomp_accepted -> non_competitive_accepted
                # int_rate -> coupon_rate, high_discnt_rate -> high_discount_rate
                auction = TreasuryAuction(
                    cusip=cusip,
                    auction_date=auction_date,
                    security_type=auc.get('security_type', 'Note'),
                    security_term=security_term,
                    term_months=self.TERM_TO_MONTHS.get(security_term),
                    issue_date=self._parse_date(auc.get('issue_date')),
                    maturity_date=self._parse_date(auc.get('maturity_date')),
                    offering_amount=offering_amount,
                    total_tendered=total_tendered,
                    total_accepted=total_accepted,
                    bid_to_cover_ratio=bid_to_cover,
                    competitive_tendered=self._parse_decimal(auc.get('comp_tendered')),
                    competitive_accepted=self._parse_decimal(auc.get('comp_accepted')),
                    non_competitive_tendered=self._parse_decimal(auc.get('noncomp_tendered')),
                    non_competitive_accepted=self._parse_decimal(auc.get('noncomp_accepted')),
                    primary_dealer_tendered=self._parse_decimal(auc.get('primary_dealer_tendered')),
                    primary_dealer_accepted=self._parse_decimal(auc.get('primary_dealer_accepted')),
                    direct_bidder_tendered=self._parse_decimal(auc.get('direct_bidder_tendered')),
                    direct_bidder_accepted=self._parse_decimal(auc.get('direct_bidder_accepted')),
                    indirect_bidder_accepted=self._parse_decimal(auc.get('indirect_bidder_accepted')),
                    high_yield=high_yield,
                    high_discount_rate=self._parse_decimal(auc.get('high_discnt_rate')),
                    low_yield=self._parse_decimal(auc.get('low_yield')),
                    median_yield=self._parse_decimal(auc.get('avg_med_yield')),
                    coupon_rate=self._parse_decimal(auc.get('int_rate')),
                    price_per_100=self._parse_decimal(auc.get('high_price')),
                    source_endpoint='auctions_query',
                    raw_json=auc,
                )
                self.session.add(auction)
                inserted += 1

        self.session.commit()
        self._stats['auctions_inserted'] = inserted
        self._stats['auctions_updated'] = updated

        log.info(f"Collected {len(auctions)} auctions: {inserted} inserted, {updated} updated")
        return inserted, updated

    def collect_recent_auctions(self, days: int = 30) -> Tuple[int, int]:
        """
        Collect auction results from the last N days.

        Args:
            days: Number of days to look back

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
        return self.collect_auction_results(start_date=start_date)

    def backfill_auctions(
        self,
        years: int = 5,
        security_term: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        Backfill N years of auction history.

        Args:
            years: Number of years to backfill
            security_term: Optional specific term to backfill

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        return self.collect_auction_results(
            backfill_years=years,
            security_term=security_term,
        )

    # ===================== Daily Rates Collection ===================== #

    def collect_daily_rates(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """
        Collect daily Treasury yield curve rates.

        Note: This would typically pull from FRED (DGS2, DGS5, DGS10, etc.)
        or Treasury's H.15 data. Implementation depends on data source.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Number of records inserted/updated
        """
        # TODO: Implement FRED integration for daily yields
        # This is a placeholder for when FRED collector is integrated
        log.warning("Daily rates collection not yet implemented - requires FRED integration")
        return 0

    # ===================== Mark Auctions as Processed ===================== #

    def mark_upcoming_as_processed(self, auction_date: date, security_term: str):
        """
        Mark an upcoming auction as processed once results are captured.

        Args:
            auction_date: Date of the auction
            security_term: Security term
        """
        from ..database.treasury_models import TreasuryUpcomingAuction

        upcoming = self.session.query(TreasuryUpcomingAuction).filter(
            TreasuryUpcomingAuction.auction_date == auction_date,
            TreasuryUpcomingAuction.security_term == security_term,
        ).first()

        if upcoming:
            upcoming.is_processed = True
            upcoming.updated_at = datetime.now(UTC)
            self.session.commit()

    # ===================== Statistics ===================== #

    def get_auction_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored auction data.

        Returns:
            Dict with auction statistics
        """
        from ..database.treasury_models import TreasuryAuction, TreasuryUpcomingAuction

        total_auctions = self.session.query(func.count(TreasuryAuction.auction_id)).scalar() or 0

        # Auctions by term
        by_term = dict(
            self.session.query(
                TreasuryAuction.security_term,
                func.count(TreasuryAuction.auction_id)
            ).group_by(TreasuryAuction.security_term).all()
        )

        # Date range
        min_date = self.session.query(func.min(TreasuryAuction.auction_date)).scalar()
        max_date = self.session.query(func.max(TreasuryAuction.auction_date)).scalar()

        # Upcoming auctions
        upcoming_count = self.session.query(func.count(TreasuryUpcomingAuction.upcoming_id)).filter(
            TreasuryUpcomingAuction.is_processed == False
        ).scalar() or 0

        return {
            'total_auctions': total_auctions,
            'by_term': by_term,
            'earliest_auction': min_date,
            'latest_auction': max_date,
            'upcoming_auctions': upcoming_count,
        }


# ===================== Convenience Functions ===================== #

def create_collector(db_url: str) -> TreasuryCollector:
    """
    Create a TreasuryCollector with database connection.

    Args:
        db_url: Database connection URL

    Returns:
        Configured TreasuryCollector instance
    """
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return TreasuryCollector(db_session=session)

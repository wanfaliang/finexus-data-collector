#!/usr/bin/env python3
"""
Update EC data with latest observations from BLS API

This script fetches the latest data points for EC series via the BLS API
and updates the database. Use this for regular updates after initial load.

Usage:
    python scripts/bls/update_ec_latest.py
    python scripts/bls/update_ec_latest.py --start-year 2024
    python scripts/bls/update_ec_latest.py --limit 100
    python scripts/bls/update_ec_latest.py --dry-run  # Preview without fetching
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict, List, cast
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from database.bls_models import ECSeries, ECData
from database.bls_tracking_models import BLSSeriesUpdateStatus, BLSAPIUsageLog
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update EC data with latest from BLS API")
    parser.add_argument(
        '--start-year',
        type=int,
        help='Start year for update (default: last year for dry-run, current year otherwise)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=datetime.now().year,
        help='End year for update (default: current year)'
    )
    parser.add_argument(
        '--series-ids',
        help='Comma-separated list of series IDs to update (default: all active series)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of series to update (for testing)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be updated without making API calls or database changes'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update even if series are marked as current'
    )
    parser.add_argument(
        '--compensations',
        help='Compensation codes (comma-separated)'
    )
    parser.add_argument(
        '--groups',
        help='Group codes (comma-separated)'
    )
    parser.add_argument(
        '--ownerships',
        help='Ownership codes (comma-separated)'
    )
    parser.add_argument(
        '--seasonal',
        help='Seasonal adjustment: S or U'
    )

    args = parser.parse_args()

    # Set default start year based on dry-run mode
    if args.start_year is None:
        args.start_year = datetime.now().year - 1 if args.dry_run else datetime.now().year

    print("=" * 80)
    if args.dry_run:
        print("DRY RUN: PREVIEW EC DATA UPDATE (NO CHANGES WILL BE MADE)")
    else:
        print("UPDATING EC (Employment Cost Index) DATA FROM BLS API")
    print("=" * 80)
    print(f"\nYear range: {args.start_year}-{args.end_year}")

    # Get database session
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get series IDs to update
        if args.series_ids:
            series_ids = [s.strip() for s in args.series_ids.split(',')]
            print(f"Target series: {len(series_ids)} specified series")
        else:
            # Get all active series from database
            query = session.query(ECSeries.series_id).filter(ECSeries.is_active == True)
            # Apply survey-specific filters
            if args.compensations:
                filter_values = [v.strip() for v in args.compensations.split(',')]
                query = query.filter(ECSeries.comp_code.in_(filter_values))
                print(f"Filter: comp_code in {filter_values}")
            if args.groups:
                filter_values = [v.strip() for v in args.groups.split(',')]
                query = query.filter(ECSeries.group_code.in_(filter_values))
                print(f"Filter: group_code in {filter_values}")
            if args.ownerships:
                filter_values = [v.strip() for v in args.ownerships.split(',')]
                query = query.filter(ECSeries.ownership_code.in_(filter_values))
                print(f"Filter: ownership_code in {filter_values}")
            if args.seasonal:
                filter_values = [v.strip() for v in args.seasonal.split(',')]
                query = query.filter(ECSeries.seasonal.in_(filter_values))
                print(f"Filter: seasonal in {filter_values}")

            if args.limit:
                query = query.limit(args.limit)
            series_ids = [row[0] for row in query.all()]
            print(f"Target series: {len(series_ids)} active series from database")

        if not series_ids:
            print("No series to update!")
            return

        # Check status and filter out already-current series (unless --force or specific series-ids)
        if not args.series_ids and not args.force:  # Only auto-filter if not explicitly specified or forced
            from datetime import timedelta
            current_threshold = datetime.now() - timedelta(hours=24)
            current_series = session.query(
                BLSSeriesUpdateStatus.series_id
            ).filter(
                BLSSeriesUpdateStatus.survey_code == 'ec',
                BLSSeriesUpdateStatus.is_current == True,
                BLSSeriesUpdateStatus.last_checked_at >= current_threshold
            ).all()
            current_series_ids = set([row[0] for row in current_series])

            # Filter out current series
            original_count = len(series_ids)
            series_ids = [sid for sid in series_ids if sid not in current_series_ids]

            if len(current_series_ids) > 0:
                print(f"Skipping {len(current_series_ids)} already-current series (checked within 24h)")
                print(f"Series needing update: {len(series_ids)}")

        if not series_ids:
            print("\nAll series are already up-to-date!")
            print("Use --force to update anyway, or wait for new data.")
            session.close()
            return

        # Calculate number of API requests needed
        num_requests = (len(series_ids) + 49) // 50  # Ceiling division
        print(f"API requests needed: ~{num_requests} ({len(series_ids)} series ÷ 50 per request)")

        if args.dry_run:
            # In dry-run mode, check what data already exists
            print(f"\nAnalyzing existing data in database...")

            # Get latest data point for each series
            latest_data = session.query(
                ECData.series_id,
                func.max(ECData.year).label('max_year')
            ).filter(
                ECData.series_id.in_(series_ids)
            ).group_by(
                ECData.series_id
            ).all()

            series_with_data = {row[0]: row[1] for row in latest_data}
            series_without_data = set(series_ids) - set(series_with_data.keys())

            # Count series by latest data year
            year_distribution = defaultdict(int)
            for series_id, max_year in series_with_data.items():
                year_distribution[max_year] += 1

            print(f"\nExisting Data Summary:")
            print(f"  Series with data: {len(series_with_data)}")
            print(f"  Series without data: {len(series_without_data)}")

            if year_distribution:
                print(f"\n  Latest data year distribution:")
                for year in sorted(year_distribution.keys(), reverse=True):
                    count = year_distribution[year]
                    print(f"    {year}: {count} series")

            # Estimate observations to fetch
            years_to_fetch = args.end_year - args.start_year + 1
            max_periods_per_series = years_to_fetch * 4
            estimated_observations = len(series_ids) * max_periods_per_series

            print(f"\nEstimated Fetch:")
            print(f"  Years to fetch: {years_to_fetch} ({args.start_year}-{args.end_year})")
            print(f"  Max periods per series: {max_periods_per_series} (quarterly)")
            print(f"  Estimated observations: ~{estimated_observations:,} (max possible)")
            print(f"  Note: Actual count will be lower (only available data points)")

            print("\n" + "=" * 80)
            print("DRY RUN COMPLETE - No API calls made, no data updated")
            print("=" * 80)
            print("\nTo perform actual update, run without --dry-run flag")

        else:
            # Actual update mode
            # Ask for confirmation
            print("\n" + "-" * 80)
            response = input("Continue with API update? (Y/N): ")
            if response.upper() != 'Y':
                print("Update cancelled.")
                session.close()
                return
            print("-" * 80)

            # Get API key from config
            api_key = settings.api.bls_api_key

            # Create BLS client
            client = BLSClient(api_key=api_key)

            # Process in batches of 50 series (one API request each)
            print(f"\nFetching data from BLS API in batches...")
            from sqlalchemy.dialects.postgresql import insert
            from datetime import date

            batch_size = 50
            total_observations = 0
            total_series_updated = 0
            total_requests_made = 0
            failed_batches = []

            for batch_num, i in enumerate(range(0, len(series_ids), batch_size), 1):
                batch = series_ids[i:i+batch_size]
                batch_start = i + 1
                batch_end = min(i + batch_size, len(series_ids))

                try:
                    # Fetch this batch
                    print(f"Batch {batch_num}/{num_requests}: Fetching series {batch_start}-{batch_end}...")

                    rows = cast(
                        List[Dict[str, Any]],
                        client.get_many(
                            batch,
                            start_year=args.start_year,
                            end_year=args.end_year,
                            calculations=False,
                            catalog=False,
                            as_dataframe=False
                        )
                    )

                    total_requests_made += 1

                    # Convert to database format
                    data_to_upsert: List[Dict[str, Any]] = []
                    for row in rows:
                        data_to_upsert.append({
                            'series_id': row['series_id'],
                            'year': row['year'],
                            'period': row['period'],
                            'value': row['value'],
                            'footnote_codes': row.get('footnotes'),
                        })

                    # Upsert batch to database
                    if data_to_upsert:
                        stmt = insert(ECData).values(data_to_upsert)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['series_id', 'year', 'period'],
                            set_={
                                'value': stmt.excluded.value,
                                'footnote_codes': stmt.excluded.footnote_codes,
                                'updated_at': datetime.now(UTC),
                            }
                        )
                        session.execute(stmt)
                        session.commit()
                        total_observations += len(data_to_upsert)
                        print(f"  Saved {len(data_to_upsert)} observations")
                    else:
                        print(f"  No data returned for this batch")

                    # Record API usage for this batch
                    usage_log = BLSAPIUsageLog(
                        usage_date=date.today(),
                        requests_used=1,
                        series_count=len(batch),
                        survey_code='ec',
                        script_name='update_ec_latest'
                    )
                    session.add(usage_log)

                    # Update series status for this batch
                    now = datetime.now()
                    for series_id in batch:
                        # Check if series is current (has recent data)
                        latest = session.query(
                            func.max(ECData.year)
                        ).filter(
                            ECData.series_id == series_id
                        ).scalar()

                        is_current = latest is not None and latest >= args.end_year - 1

                        # Upsert status
                        status_stmt = insert(BLSSeriesUpdateStatus).values({
                            'series_id': series_id,
                            'survey_code': 'ec',
                            'last_checked_at': now,
                            'last_updated_at': now,
                            'is_current': is_current,
                        })
                        status_stmt = status_stmt.on_conflict_do_update(
                            index_elements=['series_id'],
                            set_={
                                'last_checked_at': status_stmt.excluded.last_checked_at,
                                'last_updated_at': status_stmt.excluded.last_updated_at,
                                'is_current': status_stmt.excluded.is_current,
                            }
                        )
                        session.execute(status_stmt)

                    session.commit()
                    total_series_updated += len(batch)

                except KeyboardInterrupt:
                    print(f"\n\nUpdate interrupted by user at batch {batch_num}")
                    print(f"Progress saved: {total_series_updated} series, {total_observations} observations")
                    session.commit()
                    break

                except Exception as e:
                    print(f"  ERROR in batch {batch_num}: {e}")
                    failed_batches.append((batch_num, batch_start, batch_end, str(e)))
                    session.rollback()

                    # Check if it's an API limit error
                    error_str = str(e).lower()
                    if 'quota' in error_str or 'limit' in error_str or 'exceeded' in error_str:
                        print(f"\n  API limit likely exceeded. Stopping to preserve quota.")
                        print(f"  Progress saved: {total_series_updated} series updated successfully")
                        break

                    # For other errors, continue with next batch
                    print(f"  Continuing with next batch...")
                    continue

            # Summary
            print("\n" + "=" * 80)
            if total_series_updated > 0:
                print("UPDATE COMPLETE!")
                print(f"  Series updated: {total_series_updated} / {len(series_ids)}")
                print(f"  Observations: {total_observations:,}")
                print(f"  API requests: {total_requests_made}")

                if failed_batches:
                    print(f"\n  Failed batches: {len(failed_batches)}")
                    for batch_num, start, end, error in failed_batches[:5]:  # Show first 5
                        print(f"    Batch {batch_num} (series {start}-{end}): {error[:50]}")
                    if len(failed_batches) > 5:
                        print(f"    ... and {len(failed_batches) - 5} more")

                if total_series_updated < len(series_ids):
                    remaining = len(series_ids) - total_series_updated
                    print(f"\n  Remaining series: {remaining}")
                    print(f"  Run script again to continue (already-updated series will be skipped)")
            else:
                print("NO DATA UPDATED")
                print(f"  All {len(failed_batches)} batches failed")
                if failed_batches:
                    print(f"\n  First error: {failed_batches[0][3]}")
            print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()

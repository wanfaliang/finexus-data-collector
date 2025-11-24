#!/usr/bin/env python3
"""
Upgrade all BLS update scripts with enhanced --dry-run functionality

This script automatically updates all update_*_latest.py scripts to include:
- --dry-run parameter with database analysis
- Default start year of last year in dry-run mode
- Existing data analysis (series with/without data, year distribution)
- Estimated observations count
- Empty data handling
"""
import re
from pathlib import Path
from typing import Dict, List

# Survey metadata: survey_code -> (SeriesModel, DataModel, periods_per_year, period_type)
SURVEYS = {
    'ap': ('APSeries', 'APData', 12, 'monthly'),
    'cu': ('CUSeries', 'CUData', 12, 'monthly'),
    'la': ('LASeries', 'LAData', 12, 'monthly'),
    'ce': ('CESeries', 'CEData', 12, 'monthly'),
    'pc': ('PCSeries', 'PCData', 12, 'monthly'),
    'wp': ('WPSeries', 'WPData', 12, 'monthly'),
    'sm': ('SMSeries', 'SMData', 12, 'monthly'),
    'jt': ('JTSeries', 'JTData', 12, 'monthly'),
    'ec': ('ECSeries', 'ECData', 4, 'quarterly'),
    'oe': ('OESeries', 'OEData', 1, 'annual'),
    'pr': ('PRSeries', 'PRData', 4, 'quarterly'),
    'tu': ('TUSeries', 'TUData', 1, 'annual'),
    'ip': ('IPSeries', 'IPData', 4, 'quarterly'),
    'ln': ('LNSeries', 'LNData', 12, 'monthly'),
    'cw': ('CWSeries', 'CWData', 12, 'monthly'),
    'su': ('SUSeries', 'SUData', 12, 'monthly'),
    'bd': ('BDSeries', 'BDData', 4, 'quarterly'),
    'ei': ('EISeries', 'EIData', 12, 'monthly'),
}

# Survey-specific filter configurations
# Maps survey code to list of (filter_arg_name, filter_field_name, filter_description)
FILTER_CONFIGS = {
    'ap': [
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('items', 'item_code', 'Item codes (comma-separated)'),
    ],
    'cu': [
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('items', 'item_code', 'Item codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'cw': [
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('items', 'item_code', 'Item codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'su': [
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('items', 'item_code', 'Item codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'la': [
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('measures', 'measure_code', 'Measure codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'ce': [
        ('supersectors', 'supersector_code', 'Supersector codes (comma-separated)'),
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('data-types', 'data_type_code', 'Data type codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'pc': [
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('products', 'product_code', 'Product codes (comma-separated)'),
    ],
    'wp': [
        ('groups', 'group_code', 'Group codes (comma-separated)'),
        ('items', 'item_code', 'Item codes (comma-separated)'),
    ],
    'sm': [
        ('states', 'state_code', 'State codes (comma-separated)'),
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('supersectors', 'supersector_code', 'Supersector codes (comma-separated)'),
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('data-types', 'data_type_code', 'Data type codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'jt': [
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('states', 'state_code', 'State codes (comma-separated)'),
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('size-classes', 'sizeclass_code', 'Size class codes (comma-separated)'),
        ('data-elements', 'dataelement_code', 'Data element codes (comma-separated)'),
        ('rate-levels', 'ratelevel_code', 'Rate/level codes (comma-separated)'),
        ('seasonal', 'seasonal', 'Seasonal adjustment: S or U'),
    ],
    'ec': [
        ('compensations', 'comp_code', 'Compensation codes (comma-separated)'),
        ('groups', 'group_code', 'Group codes (comma-separated)'),
        ('ownerships', 'ownership_code', 'Ownership codes (comma-separated)'),
        ('seasonal', 'seasonal', 'Seasonal adjustment: S or U'),
    ],
    'oe': [
        ('area-types', 'areatype_code', 'Area type codes (comma-separated)'),
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('occupations', 'occupation_code', 'Occupation codes (comma-separated)'),
        ('data-types', 'datatype_code', 'Data type codes (comma-separated)'),
        ('states', 'state_code', 'State codes (comma-separated)'),
        ('areas', 'area_code', 'Area codes (comma-separated)'),
        ('sectors', 'sector_code', 'Sector codes (comma-separated)'),
    ],
    'pr': [
        ('sectors', 'sector_code', 'Sector codes (comma-separated)'),
        ('classes', 'class_code', 'Class codes (comma-separated)'),
        ('measures', 'measure_code', 'Measure codes (comma-separated)'),
        ('durations', 'duration_code', 'Duration codes (comma-separated)'),
        ('seasonal', 'seasonal', 'Seasonal adjustment: S'),
    ],
    'tu': [
        ('stat-types', 'stattype_code', 'Statistic type codes (comma-separated)'),
        ('sex', 'sex_code', 'Sex codes (comma-separated)'),
        ('regions', 'region_code', 'Region codes (comma-separated)'),
        ('labor-force-status', 'lfstat_code', 'Labor force status codes (comma-separated)'),
        ('activities', 'actcode_code', 'Activity codes (comma-separated)'),
    ],
    'ip': [
        ('sectors', 'sector_code', 'Sector codes (comma-separated)'),
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('measures', 'measure_code', 'Measure codes (comma-separated)'),
        ('durations', 'duration_code', 'Duration codes (comma-separated)'),
        ('types', 'type_code', 'Type codes (comma-separated)'),
        ('areas', 'area_code', 'Area codes (comma-separated)'),
    ],
    'ln': [
        ('labor-force-status', 'lfst_code', 'Labor force status codes (comma-separated)'),
        ('ages', 'ages_code', 'Age group codes (comma-separated)'),
        ('sex', 'sexs_code', 'Sex codes (comma-separated)'),
        ('race', 'race_code', 'Race codes (comma-separated)'),
        ('education', 'education_code', 'Education codes (comma-separated)'),
        ('occupations', 'occupation_code', 'Occupation codes (comma-separated)'),
        ('industries', 'indy_code', 'Industry codes (comma-separated)'),
        ('seasonal', 'seasonal', 'Seasonal adjustment: S or U'),
    ],
    'ei': [
        ('indexes', 'index_code', 'Index codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
    'bd': [
        ('states', 'state_code', 'State codes (comma-separated)'),
        ('industries', 'industry_code', 'Industry codes (comma-separated)'),
        ('unit-analysis', 'unitanalysis_code', 'Unit of analysis codes (comma-separated)'),
        ('data-elements', 'dataelement_code', 'Data element codes (comma-separated)'),
        ('size-classes', 'sizeclass_code', 'Size class codes (comma-separated)'),
        ('data-classes', 'dataclass_code', 'Data class codes (comma-separated)'),
        ('rate-levels', 'ratelevel_code', 'Rate/level codes (comma-separated)'),
        ('seasonal', 'seasonal_code', 'Seasonal adjustment: S or U'),
    ],
}

def generate_enhanced_script(survey_code: str, series_model: str, data_model: str,
                            periods_per_year: int, period_type: str) -> str:
    """Generate enhanced update script with dry-run functionality"""

    survey_upper = survey_code.upper()
    survey_name = {
        'ap': 'Average Price Data',
        'cu': 'Consumer Price Index (All Urban Consumers)',
        'la': 'Local Area Unemployment Statistics',
        'ce': 'Current Employment Statistics',
        'pc': 'Producer Price Index by Commodity',
        'wp': 'Producer Price Index',
        'sm': 'State and Metro Area Employment',
        'jt': 'JOLTS (Job Openings and Labor Turnover Survey)',
        'ec': 'Employment Cost Index',
        'oe': 'Occupational Employment and Wage Statistics',
        'pr': 'Major Sector Productivity and Costs',
        'tu': 'American Time Use Survey',
        'ip': 'Industry Productivity',
        'ln': 'Labor Force Statistics from the Current Population Survey',
        'cw': 'Consumer Price Index for Urban Wage Earners',
        'su': 'Chained Consumer Price Index for All Urban Consumers',
        'bd': 'Business Employment Dynamics',
        'ei': 'Import/Export Price Indexes',
    }.get(survey_code, survey_upper + ' Data')

    # Get filter configuration for this survey
    filter_configs = FILTER_CONFIGS.get(survey_code, [])

    # Generate filter argument definitions for argparse
    filter_args = ""
    for arg_name, field_name, description in filter_configs:
        filter_args += f"""    parser.add_argument(
        '--{arg_name}',
        help='{description}'
    )
"""

    # Generate filter application logic
    filter_logic = ""
    if filter_configs:
        filter_logic = "\n            # Apply survey-specific filters\n"
        for arg_name, field_name, description in filter_configs:
            arg_var = arg_name.replace('-', '_')
            filter_logic += f"""            if args.{arg_var}:
                filter_values = [v.strip() for v in args.{arg_var}.split(',')]
                query = query.filter({series_model}.{field_name}.in_(filter_values))
                print(f\"Filter: {field_name} in {{filter_values}}\")
"""

    return f'''#!/usr/bin/env python3
"""
Update {survey_upper} data with latest observations from BLS API

This script fetches the latest data points for {survey_upper} series via the BLS API
and updates the database. Use this for regular updates after initial load.

Usage:
    python scripts/bls/update_{survey_code}_latest.py
    python scripts/bls/update_{survey_code}_latest.py --start-year 2024
    python scripts/bls/update_{survey_code}_latest.py --limit 100
    python scripts/bls/update_{survey_code}_latest.py --dry-run  # Preview without fetching
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
from database.bls_models import {series_model}, {data_model}
from database.bls_tracking_models import BLSSeriesUpdateStatus, BLSAPIUsageLog
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update {survey_upper} data with latest from BLS API")
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
{filter_args}
    args = parser.parse_args()

    # Set default start year based on dry-run mode
    if args.start_year is None:
        args.start_year = datetime.now().year - 1 if args.dry_run else datetime.now().year

    print("=" * 80)
    if args.dry_run:
        print("DRY RUN: PREVIEW {survey_upper} DATA UPDATE (NO CHANGES WILL BE MADE)")
    else:
        print("UPDATING {survey_upper} ({survey_name}) DATA FROM BLS API")
    print("=" * 80)
    print(f"\\nYear range: {{args.start_year}}-{{args.end_year}}")

    # Get database session
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get series IDs to update
        if args.series_ids:
            series_ids = [s.strip() for s in args.series_ids.split(',')]
            print(f"Target series: {{len(series_ids)}} specified series")
        else:
            # Get all active series from database
            query = session.query({series_model}.series_id).filter({series_model}.is_active == True){filter_logic}
            if args.limit:
                query = query.limit(args.limit)
            series_ids = [row[0] for row in query.all()]
            print(f"Target series: {{len(series_ids)}} active series from database")

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
                BLSSeriesUpdateStatus.survey_code == '{survey_code}',
                BLSSeriesUpdateStatus.is_current == True,
                BLSSeriesUpdateStatus.last_checked_at >= current_threshold
            ).all()
            current_series_ids = set([row[0] for row in current_series])

            # Filter out current series
            original_count = len(series_ids)
            series_ids = [sid for sid in series_ids if sid not in current_series_ids]

            if len(current_series_ids) > 0:
                print(f"Skipping {{len(current_series_ids)}} already-current series (checked within 24h)")
                print(f"Series needing update: {{len(series_ids)}}")

        if not series_ids:
            print("\\nAll series are already up-to-date!")
            print("Use --force to update anyway, or wait for new data.")
            session.close()
            return

        # Calculate number of API requests needed
        num_requests = (len(series_ids) + 49) // 50  # Ceiling division
        print(f"API requests needed: ~{{num_requests}} ({{len(series_ids)}} series ÷ 50 per request)")

        if args.dry_run:
            # In dry-run mode, check what data already exists
            print(f"\\nAnalyzing existing data in database...")

            # Get latest data point for each series
            latest_data = session.query(
                {data_model}.series_id,
                func.max({data_model}.year).label('max_year')
            ).filter(
                {data_model}.series_id.in_(series_ids)
            ).group_by(
                {data_model}.series_id
            ).all()

            series_with_data = {{row[0]: row[1] for row in latest_data}}
            series_without_data = set(series_ids) - set(series_with_data.keys())

            # Count series by latest data year
            year_distribution = defaultdict(int)
            for series_id, max_year in series_with_data.items():
                year_distribution[max_year] += 1

            print(f"\\nExisting Data Summary:")
            print(f"  Series with data: {{len(series_with_data)}}")
            print(f"  Series without data: {{len(series_without_data)}}")

            if year_distribution:
                print(f"\\n  Latest data year distribution:")
                for year in sorted(year_distribution.keys(), reverse=True):
                    count = year_distribution[year]
                    print(f"    {{year}}: {{count}} series")

            # Estimate observations to fetch
            years_to_fetch = args.end_year - args.start_year + 1
            max_periods_per_series = years_to_fetch * {periods_per_year}
            estimated_observations = len(series_ids) * max_periods_per_series

            print(f"\\nEstimated Fetch:")
            print(f"  Years to fetch: {{years_to_fetch}} ({{args.start_year}}-{{args.end_year}})")
            print(f"  Max periods per series: {{max_periods_per_series}} ({period_type})")
            print(f"  Estimated observations: ~{{estimated_observations:,}} (max possible)")
            print(f"  Note: Actual count will be lower (only available data points)")

            print("\\n" + "=" * 80)
            print("DRY RUN COMPLETE - No API calls made, no data updated")
            print("=" * 80)
            print("\\nTo perform actual update, run without --dry-run flag")

        else:
            # Actual update mode
            # Ask for confirmation
            print("\\n" + "-" * 80)
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
            print(f"\\nFetching data from BLS API in batches...")
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
                    print(f"Batch {{batch_num}}/{{num_requests}}: Fetching series {{batch_start}}-{{batch_end}}...")

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
                        data_to_upsert.append({{
                            'series_id': row['series_id'],
                            'year': row['year'],
                            'period': row['period'],
                            'value': row['value'],
                            'footnote_codes': row.get('footnotes'),
                        }})

                    # Upsert batch to database
                    if data_to_upsert:
                        stmt = insert({data_model}).values(data_to_upsert)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['series_id', 'year', 'period'],
                            set_={{
                                'value': stmt.excluded.value,
                                'footnote_codes': stmt.excluded.footnote_codes,
                                'updated_at': datetime.now(UTC),
                            }}
                        )
                        session.execute(stmt)
                        session.commit()
                        total_observations += len(data_to_upsert)
                        print(f"  Saved {{len(data_to_upsert)}} observations")
                    else:
                        print(f"  No data returned for this batch")

                    # Record API usage for this batch
                    usage_log = BLSAPIUsageLog(
                        usage_date=date.today(),
                        requests_used=1,
                        series_count=len(batch),
                        survey_code='{survey_code}',
                        script_name='update_{survey_code}_latest'
                    )
                    session.add(usage_log)

                    # Update series status for this batch
                    now = datetime.now()
                    for series_id in batch:
                        # Check if series is current (has recent data)
                        latest = session.query(
                            func.max({data_model}.year)
                        ).filter(
                            {data_model}.series_id == series_id
                        ).scalar()

                        is_current = latest is not None and latest >= args.end_year - 1

                        # Upsert status
                        status_stmt = insert(BLSSeriesUpdateStatus).values({{
                            'series_id': series_id,
                            'survey_code': '{survey_code}',
                            'last_checked_at': now,
                            'last_updated_at': now,
                            'is_current': is_current,
                        }})
                        status_stmt = status_stmt.on_conflict_do_update(
                            index_elements=['series_id'],
                            set_={{
                                'last_checked_at': status_stmt.excluded.last_checked_at,
                                'last_updated_at': status_stmt.excluded.last_updated_at,
                                'is_current': status_stmt.excluded.is_current,
                            }}
                        )
                        session.execute(status_stmt)

                    session.commit()
                    total_series_updated += len(batch)

                except KeyboardInterrupt:
                    print(f"\\n\\nUpdate interrupted by user at batch {{batch_num}}")
                    print(f"Progress saved: {{total_series_updated}} series, {{total_observations}} observations")
                    session.commit()
                    break

                except Exception as e:
                    print(f"  ERROR in batch {{batch_num}}: {{e}}")
                    failed_batches.append((batch_num, batch_start, batch_end, str(e)))
                    session.rollback()

                    # Check if it's an API limit error
                    error_str = str(e).lower()
                    if 'quota' in error_str or 'limit' in error_str or 'exceeded' in error_str:
                        print(f"\\n  API limit likely exceeded. Stopping to preserve quota.")
                        print(f"  Progress saved: {{total_series_updated}} series updated successfully")
                        break

                    # For other errors, continue with next batch
                    print(f"  Continuing with next batch...")
                    continue

            # Summary
            print("\\n" + "=" * 80)
            if total_series_updated > 0:
                print("UPDATE COMPLETE!")
                print(f"  Series updated: {{total_series_updated}} / {{len(series_ids)}}")
                print(f"  Observations: {{total_observations:,}}")
                print(f"  API requests: {{total_requests_made}}")

                if failed_batches:
                    print(f"\\n  Failed batches: {{len(failed_batches)}}")
                    for batch_num, start, end, error in failed_batches[:5]:  # Show first 5
                        print(f"    Batch {{batch_num}} (series {{start}}-{{end}}): {{error[:50]}}")
                    if len(failed_batches) > 5:
                        print(f"    ... and {{len(failed_batches) - 5}} more")

                if total_series_updated < len(series_ids):
                    remaining = len(series_ids) - total_series_updated
                    print(f"\\n  Remaining series: {{remaining}}")
                    print(f"  Run script again to continue (already-updated series will be skipped)")
            else:
                print("NO DATA UPDATED")
                print(f"  All {{len(failed_batches)}} batches failed")
                if failed_batches:
                    print(f"\\n  First error: {{failed_batches[0][3]}}")
            print("=" * 80)

    except Exception as e:
        print(f"\\nERROR: {{e}}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
'''

def main():
    scripts_dir = Path("scripts/bls")

    print("=" * 80)
    print("UPGRADING BLS UPDATE SCRIPTS WITH ENHANCED --DRY-RUN FUNCTIONALITY")
    print("=" * 80)

    updated_count = 0
    for survey_code, (series_model, data_model, periods, period_type) in SURVEYS.items():
        script_path = scripts_dir / f"update_{survey_code}_latest.py"

        if not script_path.exists():
            print(f"\\nSkipping {survey_code.upper()}: Script not found")
            continue

        print(f"\\nUpdating {survey_code.upper()}...")

        # Generate new script content
        new_content = generate_enhanced_script(
            survey_code, series_model, data_model, periods, period_type
        )

        # Write to file
        script_path.write_text(new_content, encoding='utf-8')
        updated_count += 1
        print(f"  [OK] Updated {script_path.name}")

    print("\\n" + "=" * 80)
    print(f"SUCCESS! Updated {updated_count}/{len(SURVEYS)} scripts")
    print("=" * 80)
    print("\\nAll update scripts now have:")
    print("  - --dry-run parameter with database analysis")
    print("  - Default start year of last year in dry-run mode")
    print("  - Existing data analysis and year distribution")
    print("  - Estimated observations count")
    print("  - Empty data handling")

if __name__ == "__main__":
    main()
